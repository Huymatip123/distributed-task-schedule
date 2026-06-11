"""
worker.py
---------
Worker Node (Section 2 / Section 3.1).

Responsibilities implemented here:
  - Register with master       (REGISTER message, Section 3.1 / 4)
  - Execute tasks               (run_task from tasks.py, Section 5)
  - Send results                (RESULT message, Section 4)
  - Send heartbeat messages     (HEARTBEAT every 2s, Section 6)

Thread design (Section 9):
  Thread "heartbeat_loop" : Heartbeat Sender
  Thread "worker_thread" (x cpu_cores) : Receive Task / Execute
  Main thread : reads TASK messages from the master and feeds an internal
                queue.Queue, so heartbeats keep flowing even while tasks
                are executing (CPU-bound work happens in the worker
                threads, not the network thread).

A threading.Lock (send_lock) protects the shared socket so that the
heartbeat thread and the task-execution threads never interleave writes.
"""

import socket
import threading
import time
import argparse
import queue

from net import MessageStream
from tasks import run_task


def main():
    p = argparse.ArgumentParser(description="Distributed Task Scheduler - Worker")
    p.add_argument("--master-host", default="127.0.0.1")
    p.add_argument("--master-port", type=int, default=6000)
    p.add_argument("--worker-id", type=int, required=True)
    p.add_argument("--cpu-cores", type=int, default=2,
                    help="Number of tasks this worker can execute in parallel")
    args = p.parse_args()

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((args.master_host, args.master_port))
    ms = MessageStream(sock)

    send_lock = threading.Lock()

    def safe_send(obj):
        with send_lock:
            ms.send(obj)

    # --- Section 3.1: Worker Registration -------------------------------
    safe_send({
        "type": "REGISTER",
        "worker_id": args.worker_id,
        "cpu_cores": args.cpu_cores,
    })
    print("[WORKER %d] Registered with master %s:%d (cpu_cores=%d)"
          % (args.worker_id, args.master_host, args.master_port, args.cpu_cores))

    task_queue = queue.Queue()

    # --- Heartbeat Sender thread (Section 6) ----------------------------
    def heartbeat_loop():
        while True:
            time.sleep(2.0)
            try:
                safe_send({"type": "HEARTBEAT", "worker_id": args.worker_id})
            except OSError:
                return

    # --- Task execution threads (Section 3.1: Execute tasks) ------------
    def worker_thread(idx):
        while True:
            tid, op, inp = task_queue.get()
            print("[WORKER %d] thread-%d running task %d (%s, input=%s)"
                  % (args.worker_id, idx, tid, op, inp))
            try:
                output = run_task(op, inp)
            except Exception as exc:
                output = "ERROR: %s" % exc

            try:
                safe_send({
                    "type": "RESULT",
                    "task_id": tid,
                    "worker_id": args.worker_id,
                    "output": output,
                })
            except OSError:
                return
            print("[WORKER %d] thread-%d finished task %d" % (args.worker_id, idx, tid))

    threading.Thread(target=heartbeat_loop, daemon=True).start()
    for i in range(args.cpu_cores):
        threading.Thread(target=worker_thread, args=(i,), daemon=True).start()

    # --- Main thread: Receive Task ---------------------------------------
    while True:
        msg = ms.recv()
        if msg is None:
            print("[WORKER %d] Lost connection to master, exiting" % args.worker_id)
            break
        if msg.get("type") == "TASK":
            task_queue.put((msg["task_id"], msg["operation"], msg["input"]))


if __name__ == "__main__":
    main()

"""
master.py
---------
The Master Node (Section 2 of the assignment).

Responsibilities implemented here:
  - Maintain task queue                (3.2, Section 8: Task struct)
  - Schedule tasks                     (3.3: FIFO / Round Robin / Least Loaded)
  - Track worker status                (Section 8: WorkerInfo / WorkerTable)
  - Detect failures                    (Section 6: heartbeat timeout > 6s)
  - Reassign unfinished tasks          (Section 7: failure recovery)

Thread design (Section 9):
  Thread "accept_workers" : Accept Connections (from workers)
  Thread "accept_clients" : Accept Connections (task submission / stats)
  Thread "scheduler_loop" : Scheduler
  Thread "heartbeat_monitor" : Heartbeat Monitor

A single threading.Lock protects all shared structures (self.workers,
self.tasks, self.queue), as required by Section 9 ("Students must protect
shared structures with mutexes/semaphores/monitors").
"""

import socket
import threading
import time
import argparse
from collections import deque

from net import MessageStream


HEARTBEAT_TIMEOUT = 6.0  # seconds (Section 6)


class Master:
    def __init__(self, worker_port, client_port, policy):
        self.worker_port = worker_port
        self.client_port = client_port
        self.policy = policy

        self.lock = threading.Lock()

        # WorkerTable (Section 3.1 / Section 8: struct WorkerInfo)
        # worker_id -> {stream, alive, current_load, last_heartbeat, cpu_cores}
        self.workers = {}

        # task_id -> {task_id, type, input, status, assigned_worker,
        #             submit_time, start_time, end_time}
        self.tasks = {}

        self.queue = deque()       # READY task ids, in arrival order
        self.rr_index = 0          # round-robin pointer
        self.next_task_id = 1

    # ------------------------------------------------------------------
    # Bootstrapping
    # ------------------------------------------------------------------
    def start(self):
        threading.Thread(target=self.accept_workers, daemon=True).start()
        threading.Thread(target=self.accept_clients, daemon=True).start()
        threading.Thread(target=self.scheduler_loop, daemon=True).start()
        threading.Thread(target=self.heartbeat_monitor, daemon=True).start()

        print("[MASTER] policy=%s worker_port=%d client_port=%d"
              % (self.policy, self.worker_port, self.client_port))
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("\n[MASTER] shutting down")

    # ------------------------------------------------------------------
    # Thread 1 (worker side): Accept Connections
    # ------------------------------------------------------------------
    def accept_workers(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(("0.0.0.0", self.worker_port))
        s.listen(64)
        while True:
            conn, addr = s.accept()
            threading.Thread(target=self.handle_worker, args=(conn,), daemon=True).start()

    def handle_worker(self, conn):
        ms = MessageStream(conn)
        msg = ms.recv()
        if not msg or msg.get("type") != "REGISTER":
            ms.close()
            return

        wid = msg["worker_id"]
        cores = msg.get("cpu_cores", 1)
        with self.lock:
            self.workers[wid] = {
                "stream": ms,
                "alive": True,
                "current_load": 0,
                "last_heartbeat": time.time(),
                "cpu_cores": cores,
            }
        print("[MASTER] Worker %d registered (cpu_cores=%d)" % (wid, cores))

        while True:
            msg = ms.recv()
            if msg is None:
                with self.lock:
                    if wid in self.workers:
                        self.mark_failed(wid)
                print("[MASTER] Worker %d connection closed" % wid)
                return

            mtype = msg.get("type")
            if mtype == "HEARTBEAT":
                with self.lock:
                    w = self.workers.get(wid)
                    if w is not None:
                        w["last_heartbeat"] = time.time()
                        if not w["alive"]:
                            w["alive"] = True
                            print("[MASTER] Worker %d is ALIVE again" % wid)

            elif mtype == "RESULT":
                tid = msg["task_id"]
                with self.lock:
                    task = self.tasks.get(tid)
                    w = self.workers.get(wid)
                    completed_now = False
                    if task and task["status"] == "RUNNING" and task["assigned_worker"] == wid:
                        task["status"] = "COMPLETED"
                        task["end_time"] = time.time()
                        completed_now = True
                        if w is not None:
                            w["current_load"] = max(0, w["current_load"] - 1)
                if completed_now:
                    rt = task["end_time"] - task["submit_time"]
                    print("[MASTER] Task %d COMPLETED by worker %d (response_time=%.3fs) -> %s"
                          % (tid, wid, rt, msg.get("output")))
                else:
                    print("[MASTER] Ignored stale RESULT for task %d from worker %d" % (tid, wid))

    # ------------------------------------------------------------------
    # Thread 1 (client side): Accept Connections (task submission / stats)
    # ------------------------------------------------------------------
    def accept_clients(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(("0.0.0.0", self.client_port))
        s.listen(64)
        while True:
            conn, addr = s.accept()
            threading.Thread(target=self.handle_client, args=(conn,), daemon=True).start()

    def handle_client(self, conn):
        ms = MessageStream(conn)
        while True:
            msg = ms.recv()
            if msg is None:
                ms.close()
                return

            mtype = msg.get("type")

            if mtype == "TASK":
                with self.lock:
                    tid = self.next_task_id
                    self.next_task_id += 1
                    self.tasks[tid] = {
                        "task_id": tid,
                        "type": msg["operation"],
                        "input": msg["input"],
                        "status": "READY",          # Section 8: status field
                        "assigned_worker": None,
                        "submit_time": time.time(),
                        "start_time": None,
                        "end_time": None,
                    }
                    self.queue.append(tid)          # FIFO arrival order (3.2/3.3)
                ms.send({"type": "ACK", "task_id": tid})

            elif mtype == "STATS":
                with self.lock:
                    total = len(self.tasks)
                    completed = sum(1 for t in self.tasks.values() if t["status"] == "COMPLETED")
                    running = sum(1 for t in self.tasks.values() if t["status"] == "RUNNING")
                    ready = sum(1 for t in self.tasks.values() if t["status"] == "READY")
                    rts = [t["end_time"] - t["submit_time"]
                           for t in self.tasks.values() if t["status"] == "COMPLETED"]
                    avg_rt = sum(rts) / len(rts) if rts else 0.0
                    workers_info = {
                        wid: {"alive": w["alive"], "current_load": w["current_load"]}
                        for wid, w in self.workers.items()
                    }
                ms.send({
                    "type": "STATS_REPLY",
                    "total": total,
                    "completed": completed,
                    "running": running,
                    "ready": ready,
                    "avg_response_time": avg_rt,
                    "workers": workers_info,
                })

            elif mtype == "PING":
                ms.send({"type": "PONG"})

    # ------------------------------------------------------------------
    # Thread 2: Scheduler  (Section 3.3)
    # ------------------------------------------------------------------
    def scheduler_loop(self):
        while True:
            time.sleep(0.2)
            with self.lock:
                if not self.queue:
                    continue

                alive_ids = sorted(wid for wid, w in self.workers.items() if w["alive"])
                if not alive_ids:
                    continue

                if self.policy == "fifo":
                    # Assign the head of the queue to the first idle worker.
                    idle = [wid for wid in alive_ids if self.workers[wid]["current_load"] == 0]
                    if not idle:
                        continue
                    tid = self.queue.popleft()
                    self._assign(tid, idle[0])

                elif self.policy == "round_robin":
                    # Workers chosen cyclically (Section 3.3).
                    tid = self.queue.popleft()
                    target = alive_ids[self.rr_index % len(alive_ids)]
                    self.rr_index += 1
                    self._assign(tid, target)

                elif self.policy == "least_loaded":
                    # Assign to the worker with the smallest current_load.
                    target = min(alive_ids, key=lambda w: self.workers[w]["current_load"])
                    tid = self.queue.popleft()
                    self._assign(tid, target)

                else:
                    raise ValueError("Unknown policy: %s" % self.policy)

    def _assign(self, tid, wid):
        """Caller must hold self.lock."""
        task = self.tasks[tid]
        task["status"] = "RUNNING"
        task["assigned_worker"] = wid
        task["start_time"] = time.time()
        self.workers[wid]["current_load"] += 1
        try:
            self.workers[wid]["stream"].send({
                "type": "TASK",
                "task_id": tid,
                "operation": task["type"],
                "input": task["input"],
            })
            print("[MASTER] Task %d (%s) -> Worker %d [policy=%s]"
                  % (tid, task["type"], wid, self.policy))
        except OSError:
            # Worker socket already dead; let heartbeat monitor clean up.
            task["status"] = "READY"
            task["assigned_worker"] = None
            self.workers[wid]["current_load"] = max(0, self.workers[wid]["current_load"] - 1)
            self.queue.appendleft(tid)

    def mark_failed(self, wid):
        """Caller must hold self.lock. Marks worker `wid` as dead and
        requeues any RUNNING task currently assigned to it
        (Section 7: Task RUNNING -> READY -> reassigned)."""
        w = self.workers.get(wid)
        if w is None or not w["alive"]:
            return
        w["alive"] = False
        w["current_load"] = 0
        for tid, task in self.tasks.items():
            if task["assigned_worker"] == wid and task["status"] == "RUNNING":
                task["status"] = "READY"
                task["assigned_worker"] = None
                self.queue.appendleft(tid)
                print("[MASTER] Task %d requeued (worker %d failed)" % (tid, wid))

    # ------------------------------------------------------------------
    # Thread 3: Heartbeat Monitor (Section 6 & 7)
    # ------------------------------------------------------------------
    def heartbeat_monitor(self):
        while True:
            time.sleep(1.0)
            now = time.time()
            with self.lock:
                for wid, w in self.workers.items():
                    if w["alive"] and (now - w["last_heartbeat"] > HEARTBEAT_TIMEOUT):
                        print("[MASTER] Worker %d FAILED (heartbeat timeout > %ds)"
                              % (wid, HEARTBEAT_TIMEOUT))
                        self.mark_failed(wid)


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Distributed Task Scheduler - Master")
    p.add_argument("--worker-port", type=int, default=6000,
                    help="Port workers connect to (default 6000)")
    p.add_argument("--client-port", type=int, default=6001,
                    help="Port clients connect to for task submission/stats (default 6001)")
    p.add_argument("--policy", choices=["fifo", "round_robin", "least_loaded"],
                    default="fifo", help="Scheduling policy (Section 3.3)")
    args = p.parse_args()

    Master(args.worker_port, args.client_port, args.policy).start()

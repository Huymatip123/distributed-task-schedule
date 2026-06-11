"""
client.py
---------
Task submission client (Section 3.2). Connects to the master's client port
and submits Task(task_id, task_type, input_data) descriptions. task_id is
assigned by the master.

Can also be imported as a library by the experiment scripts
(submit_tasks / get_stats).
"""

import socket
import argparse

from net import MessageStream


def submit_tasks(host, port, task_list):
    """task_list: iterable of (operation, input_data) tuples."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((host, port))
    ms = MessageStream(sock)
    acked = 0
    for op, inp in task_list:
        ms.send({"type": "TASK", "operation": op, "input": inp})
        ack = ms.recv()
        if ack and ack.get("type") == "ACK":
            acked += 1
    ms.close()
    return acked


def get_stats(host, port):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((host, port))
    ms = MessageStream(sock)
    ms.send({"type": "STATS"})
    reply = ms.recv()
    ms.close()
    return reply


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Distributed Task Scheduler - Client")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=6001)
    p.add_argument("--num-tasks", type=int, default=20)
    p.add_argument("--type", default="prime_count",
                    choices=["prime_count", "matrix_mult", "monte_carlo_pi",
                             "word_count", "factorial"])
    p.add_argument("--input", default="200000",
                    help="Input value/size for the chosen task type")
    p.add_argument("--stats", action="store_true",
                    help="Just print master stats and exit")
    args = p.parse_args()

    if args.stats:
        print(get_stats(args.host, args.port))
    else:
        n = submit_tasks(args.host, args.port, [(args.type, args.input)] * args.num_tasks)
        print("Submitted %d/%d tasks of type '%s' (input=%s)"
              % (n, args.num_tasks, args.type, args.input))

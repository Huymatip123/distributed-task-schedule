"""
net.py
------
Tiny helper that turns a raw TCP socket into a line-delimited JSON message
stream. Every message is a JSON object terminated by '\\n'. This implements
the "simple JSON protocol" recommended in the assignment (Section 4).
"""

import json


class MessageStream:
    """Wraps a socket and lets you send/receive whole JSON messages."""

    def __init__(self, sock):
        self.sock = sock
        self.buf = b""

    def send(self, obj):
        data = (json.dumps(obj) + "\n").encode("utf-8")
        self.sock.sendall(data)

    def recv(self):
        """Return the next JSON message as a dict, or None if the peer closed
        the connection."""
        while b"\n" not in self.buf:
            try:
                chunk = self.sock.recv(4096)
            except OSError:
                return None
            if not chunk:
                return None
            self.buf += chunk
        line, _, self.buf = self.buf.partition(b"\n")
        if not line.strip():
            return self.recv()
        return json.loads(line.decode("utf-8"))

    def close(self):
        try:
            self.sock.close()
        except OSError:
            pass

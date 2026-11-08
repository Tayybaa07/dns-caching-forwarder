"""Send one control command to a running test server.
    python testserver/ctl.py 6301 delay 100
    python testserver/ctl.py 6301 stop
    python testserver/ctl.py 6301 set www.lab.test A 10.0.0.5 10
"""
import socket
import sys

port, cmd = int(sys.argv[1]), " ".join(sys.argv[2:])
with socket.create_connection(("127.0.0.1", port), timeout=3) as s:
    s.sendall((cmd + "\n").encode())
    print(s.makefile().readline().strip())

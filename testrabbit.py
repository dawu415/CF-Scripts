#!/usr/bin/env python3
import os, socket, struct

# Fill these from your service key / VCAP_SERVICES
HOST = "10.31.220.52"
PORT = 5672
USER = "<user>"
PASS = "<pass>"
VHOST = "<vhost>"          # e.g. "/" or "app-123"
QUEUE = "<queue-name>"

# Helper to send a frame
def send_frame(sock, frame_type, channel, payload):
    sock.sendall(struct.pack('!BHI', frame_type, channel, len(payload)) + payload + b'\xCE')

# Connect TCP
s = socket.create_connection((HOST, PORT))

# 1. Send protocol header
s.sendall(b"AMQP\x00\x00\x09\x01")

# 2. Read server start (frame type 1)
s.recv(4096)

# 3. Send Start-OK with SASL PLAIN
authzid = USER.encode()
authcid = USER.encode()
passwd  = PASS.encode()
resp = b"\0".join([authzid, authcid, passwd])
args = (
    b'\x00\x0cPLAIN' +        # mechanism = "PLAIN"
    struct.pack('!I', len(resp)) + resp +
    b'\x00'                   # locale/en-US
)
method = struct.pack('!HH', 10, 11) + args  # connection.start-ok
send_frame(s, 1, 0, method)

# 4. Read connection.tune / connection.open
s.recv(4096)

# 5. Open connection with vhost
args = VHOST.encode()
method = struct.pack('!HHB', 10, 40, len(args)) + args + b'\x00\x00'
send_frame(s, 1, 0, method)
s.recv(4096)

# 6. Open a channel
send_frame(s, 1, 1, struct.pack('!HH', 20, 10))  # channel.open
s.recv(4096)

# 7. Queue.declare (passive)
qname = QUEUE.encode()
payload = (
    struct.pack('!HHH', 50, 10, 1) +  # queue.declare, channel 1
    b'\x00' +                         # reserved-1
    struct.pack('!B', len(qname)) + qname +
    struct.pack('!B', 0b00001000)     # passive flag
)
send_frame(s, 1, 1, payload)

resp = s.recv(4096)
print("Response bytes:", resp)

s.close()


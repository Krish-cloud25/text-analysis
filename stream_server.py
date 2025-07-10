import socket
import time

HOST = 'localhost'
PORT = 9999

with open("dataset_unzipped/SMSSpamCollection_large", "r") as file:

    lines = file.readlines()

server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
server_socket.bind((HOST, PORT))
server_socket.listen(1)

print(f"Listening on {HOST}:{PORT}...")
conn, addr = server_socket.accept()
print(f"Connected by {addr}")

# Send lines quickly
for line in lines:
    if line.strip():
        conn.sendall(line.encode("utf-8"))
        print("Sent:", line.strip())
        time.sleep(0.1)  # Try 0.1 for Spark to catch up


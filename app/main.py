import socket
import threading
from collections import namedtuple
import logging


logger = logging.getLogger("SERVER")

PORT = 6379

Ping = namedtuple("Ping", [])
Echo = namedtuple("Echo", ["msg"])


def main():
    server_socket = socket.create_server(("", PORT), reuse_port=True)
    logger.info(f"Started on port {PORT}")

    while True:
        client_sock, addr = server_socket.accept()
        logger.info(f"client with addr {addr[1]} connected.")
        threading.Thread(target=handle_connection, args=(client_sock,)).start()

    server_socket.close()


def handle_connection(sock: socket.socket) -> None:
    while True:
        data = sock.recv(1024)
        if not data:
            break
        logger.info(f"Received: {repr(data.decode())}")
        command = parse_data(data)
        logger.info(f"Parsed as: {command}")
        if isinstance(command, Ping):
            sock.sendall(b"+PONG\r\n")
        elif isinstance(command, Echo):
            sock.sendall(as_bulk_str(command.msg).encode())
        else:
            assert False, f"unreachable: unknown command: {command}"

    logger.info(f"client {sock.getsockname()[1]} disconnected.")
    sock.close()


# [PING] = *1\r\n$4\r\nPING\r\n
# [ECHO, hey] = *2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n


def parse_data(raw_data: bytes) -> Ping | Echo:
    data = raw_data.decode()
    parts = data.split("\r\n")
    logger.debug(parts)
    logger.debug(f"{"PING" in parts=}")
    logger.debug(f"{"ECHO" in parts=}")
    if "PING" in parts:
        return Ping()
    elif "ECHO" in parts:
        return Echo(msg=parts[-2])
    else:
        assert False, "unreachable"


def as_bulk_str(s: str) -> str:
    return f"${len(s)}\r\n{s}\r\n"


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    main()

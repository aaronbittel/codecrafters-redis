import argparse
import logging
import shlex
import socket

from app.config import PORT
from app.resp import Array, dump_resp

logger = logging.getLogger("CLIENT")


class Client:
    def __init__(self, host: str = "localhost", port: int = PORT) -> None:
        self.host = host
        self.port = port
        self.server_sock: socket.socket | None = None

    def connect(self) -> None:
        self.server_sock = socket.create_connection(address=("localhost", self.port))
        logger.info(f"Connected to server on port {self.port}")

    def close(self) -> None:
        if self.server_sock:
            logger.info("Disconnected from server")
            self.server_sock.close()
            self.server_sock = None

    def ping(self) -> str:
        self.sock.sendall(Array(["PING"]).encode())
        # TODO: write parser for receiving commands
        return self.sock.recv(1024).decode()

    def echo(self, msg: str) -> str:
        self.sock.sendall(Array(["ECHO", msg]).encode())
        return self.sock.recv(1024).decode()

    @property
    def sock(self) -> socket.socket:
        if self.server_sock is None:
            raise RuntimeError("Client not connected")
        return self.server_sock


def roundtrip(sock: socket.socket, req: Array) -> bytes:
    sock.sendall(req.encode())
    logger.debug(f"Raw Send: {repr(req.encode())}")
    resp = sock.recv(1024)
    logger.debug(f"Raw Got: {repr(resp.decode())}")
    return resp


def cli_main(port: int) -> None:
    with socket.create_connection(address=("localhost", port)) as sock:
        logger.info(f"Connected to server on port {port}")
        while True:
            cmd = input("> ")
            if cmd.strip() in ("quit", "q", "exit"):
                break
            req = Array(shlex.split(cmd))
            raw_resp = roundtrip(sock, req)
            print("resp", repr(raw_resp))
            resp = raw_resp.decode()
            dump_resp(resp)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser("Redis-Client")
    parser.add_argument(
        "-p", "--port", default=PORT, type=int, help="port the client connects to"
    )
    args = parser.parse_args()

    cli_main(args.port)

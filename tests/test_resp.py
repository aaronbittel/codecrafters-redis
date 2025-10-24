import shlex
import threading
import time
from io import BufferedReader, BytesIO
from typing import Generator

import pytest

from app.client import Client
from app.resp import Command, as_bulk_bytes, as_error_bytes, create_command
from app.server import Server

TEST_PORT = 6666


@pytest.fixture(name="server")
def redis_server() -> Generator[None, None, None]:
    server = Server(port=TEST_PORT)
    server_t = threading.Thread(target=server.start)
    server_t.start()
    time.sleep(0.01)

    yield

    server.stop_event.set()
    server_t.join()


@pytest.fixture(name="client")
def redis_client(server: Server) -> Generator[Client, None, None]:
    client = Client(port=TEST_PORT)
    client.connect()
    yield client
    client.close()


def test_roundtrip_ping(client: Client):
    assert client.ping() == "+PONG\r\n"


@pytest.mark.parametrize("msg", ["hi", '"Hello, World!"'])
def test_roundtrip_echo_success(client: Client, msg: str):
    assert client.echo(msg) == as_bulk_bytes(msg).decode()


def test_roundtrip_echo_empty(client: Client):
    assert (
        client.echo("")
        == as_error_bytes("ECHO cmd: wrong number of arguments").decode()
    )


@pytest.mark.parametrize(
    "cmd_parts,expected",
    [
        ("PING", Command(name="PING", args=[])),
        ("echo msg", Command(name="ECHO", args=["msg"])),
        (
            "ECHO reallyreallylongmessage",
            Command(name="ECHO", args=["reallyreallylongmessage"]),
        ),
        (
            'ECHO "Hello, World!"',
            Command(name="ECHO", args=["Hello, World!"]),
        ),
        ("GET key", Command(name="GET", args=["key"])),
        ("SeT key value", Command(name="SET", args=["key", "value"])),
        ("LPUSH keylist 1 2 3", Command(name="LPUSH", args=["keylist", "1", "2", "3"])),
        ("echo", Command(name="ECHO", args=[])),
    ],
)
def test_parse_command(cmd_parts: str, expected: Command):
    raw_cmd = BytesIO(bytes(create_command(*shlex.split(cmd_parts)), encoding="utf-8"))
    reader = BufferedReader(raw_cmd)
    cmd = Command.parse(reader)
    assert cmd.name == expected.name
    assert cmd.args == expected.args

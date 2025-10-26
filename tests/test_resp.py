from re import A
import shlex
import threading
import time
from io import BytesIO
from typing import Generator

import pytest

from app.client import Client
from app.resp import (
    Array,
    BulkString,
    Command,
    Stream,
    StreamID,
    StreamValue,
    to_redis_value,
)
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
    assert client.echo(msg) == msg


def test_roundtrip_echo_empty(client: Client):
    assert client.echo("") == "ECHO cmd: wrong number of arguments"


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
    raw_cmd = Array(shlex.split(cmd_parts)).encode()
    reader = BytesIO(raw_cmd)
    cmd = Command.parse(reader)
    assert cmd.name == expected.name
    assert cmd.args == expected.args


@pytest.mark.parametrize(
    "start,expected_len", [(StreamID(0, 2), 2), (StreamID(0, 0), 3)]
)
def test_stream_slicing(start: str, expected_len: int):
    stream = Stream(
        values=[
            StreamValue(StreamID(0, 1), values={"foo": "bar"}),
            StreamValue(StreamID(0, 2), values={"bar": "baz"}),
            StreamValue(StreamID(0, 3), values={"baz": "foo"}),
        ]
    )
    stream_range = stream[start : StreamID(0, 3)]
    assert len(stream_range) == expected_len


@pytest.mark.parametrize(
    "start,end,expected",
    [
        ("0-2", "0-3", [["0-2", ["bar", "baz"]], ["0-3", ["baz", "foo"]]]),
        ("-", "0-2", [["0-1", ["foo", "bar"]], ["0-2", ["bar", "baz"]]]),
        ("0-2", "+", [["0-2", ["bar", "baz"]], ["0-3", ["baz", "foo"]]]),
    ],
)
def test_xadd_and_xrange(start: str, end: str, expected: list):
    server = Server(port=TEST_PORT)
    res1 = server.handle_command(
        Command(name="XADD", args=["stream_key", "0-1", "foo", "bar"])
    )
    res2 = server.handle_command(
        Command(name="XADD", args=["stream_key", "0-2", "bar", "baz"])
    )
    res3 = server.handle_command(
        Command(name="XADD", args=["stream_key", "0-3", "baz", "foo"])
    )

    assert res1.encode() == BulkString("0-1").encode()
    assert res2.encode() == BulkString("0-2").encode()
    assert res3.encode() == BulkString("0-3").encode()

    res = server.handle_command(Command(name="XRANGE", args=["stream_key", start, end]))
    assert isinstance(res, Array)
    assert len(res.values) == len(expected)

    expected_value = to_redis_value(expected)
    assert res.encode() == expected_value.encode()


def test_xadd_and_xread():
    server = Server(port=TEST_PORT)

    resp = server.handle_command(
        Command(name="XADD", args=["stream_key", "0-1", "temperature", "96"])
    )

    assert resp.encode() == BulkString("0-1").encode()

    res = server.handle_command(
        Command(name="XREAD", args=["STREAMS", "stream_key", "0-0"])
    )
    expected = [["stream_key", [["0-1", ["temperature", "96"]]]]]
    expected_value = to_redis_value(expected)
    assert res.encode() == expected_value.encode()


def test_to_redis_value():
    got = to_redis_value(
        [["0-1", ["foo", "bar"]], ["0-2", ["bar", "baz"]], ["0-3", ["baz", "foo"]]]
    )
    expected = Array(
        [
            Array([BulkString("0-1"), Array([BulkString("foo"), BulkString("bar")])]),
            Array([BulkString("0-2"), Array([BulkString("bar"), BulkString("baz")])]),
            Array([BulkString("0-3"), Array([BulkString("baz"), BulkString("foo")])]),
        ]
    )
    assert got == expected


def test_simple_array():
    array = Array(["Hello"])
    expected = b"*1\r\n$5\r\nHello\r\n"
    assert array.encode() == expected


def test_simple_array2():
    array = Array([Array(["Hello"])])
    expected = b"*1\r\n*1\r\n$5\r\nHello\r\n"
    assert array.encode() == expected


def test_simple_array3():
    array = Array(
        [
            "Hi",
            Array(
                ["Hello"],
            ),
        ],
    )
    expected = b"*2\r\n$2\r\nHi\r\n*1\r\n$5\r\nHello\r\n"
    assert array.encode() == expected


def test_resp_big_array():
    array = Array(
        [
            Array(
                [
                    "1526985054069-0",
                    Array(
                        [
                            "temperature",
                            "36",
                            "humidity",
                            "95",
                        ]
                    ),
                ]
            ),
            Array(
                [
                    "1526985054079-0",
                    Array(
                        [
                            "temperature",
                            "37",
                            "humidity",
                            "94",
                        ]
                    ),
                ]
            ),
        ],
    )
    expected = (
        "*2\r\n"
        "*2\r\n$15\r\n1526985054069-0\r\n"
        "*4\r\n$11\r\ntemperature\r\n$2\r\n36\r\n$8\r\nhumidity\r\n$2\r\n95\r\n"
        "*2\r\n$15\r\n1526985054079-0\r\n"
        "*4\r\n$11\r\ntemperature\r\n$2\r\n37\r\n$8\r\nhumidity\r\n$2\r\n94\r\n"
    ).encode()
    assert array.encode() == expected

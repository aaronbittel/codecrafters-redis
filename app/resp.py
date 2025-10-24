from __future__ import annotations

from dataclasses import dataclass
from io import BufferedReader
from typing import IO, Self

NULL_BULK_BYTES = b"$-1\r\n"
OK_BYTES = b"+OK\r\n"
EMPTY_ARRAY_BYTES = b"*0\r\n"
NULL_ARRAY_BYTES = b"*-1\r\n"


@dataclass
class Command:
    name: str
    args: list[str]

    @classmethod
    def parse(cls, reader: IO[bytes]) -> Self:
        arr_prefix = _safe_read(reader, 1)
        if arr_prefix != b"*":
            raise ValueError(f"Expected array for command, bot got {arr_prefix!r}")
        try:
            count = int(_safe_readline(reader).strip())
        except ValueError as v:
            raise ValueError("Protocol error: expected integer") from v
        raw_args: list[bytes] = []
        for _ in range(count):
            prefix = _safe_read(reader, 1)
            if prefix != b"$":
                raise ValueError(
                    f"Expected bulk string for element, but got {prefix!r}"
                )
            try:
                length = int(_safe_readline(reader).strip())
            except ValueError as v:
                raise ValueError("Protocol error: expected integer") from v
            arg = _safe_readline(reader).strip()
            assert len(arg) == length, (
                f"promised length was {length}, but got {arg} with length {len(arg)}"
            )
            raw_args.append(arg)
        args = list(map(lambda arg: arg.decode(), raw_args))
        return cls(name=args[0].upper(), args=args[1:])


def create_command(*args: str) -> str:
    def fmt(arg: str) -> str:
        return f"${len(arg)}\r\n{arg}\r\n"

    return f"*{len(args)}\r\n{''.join(fmt(arg) for arg in args)}"


def dump_resp(resp: str) -> None:
    dump = " ".join(
        filter(
            lambda p: not (p.startswith("*") or p.startswith("$")),
            resp.split("\r\n"),
        )
    )
    print(dump if dump.strip() != "" else "(empty)")


def as_bulk_bytes(s: str) -> bytes:
    return f"${len(s)}\r\n{s}\r\n".encode()


def as_integer_bytes(n: int) -> bytes:
    return f":{n}\r\n".encode()


def as_array_bytes(xs: list[str]) -> bytes:
    def fmt(arg: str) -> str:
        return f"${len(arg)}\r\n{arg}\r\n"

    return f"*{len(xs)}\r\n{''.join(fmt(x) for x in xs)}".encode()


def as_error_bytes(msg: str) -> bytes:
    return f"-ERR {msg}\r\n".encode()


def _safe_read(reader: BufferedReader, size: int) -> bytes:
    data = reader.read(size)
    if not data:
        raise ConnectionResetError("client closed connection")
    return data


def _safe_readline(reader: BufferedReader, size: int | None = -1) -> bytes:
    line = reader.readline(size)
    if not line:
        raise ConnectionResetError("client closed connection")
    return line

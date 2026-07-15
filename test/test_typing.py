from unittest import TestCase
from amsgpack import Ext, Unpacker, Packer
from subprocess import run
import sys
from json import loads
import re


def _run_pyright_types():
    r = run(
        [sys.executable, "-m", "pyright", "--outputjson", __file__],
        capture_output=True,
    )
    ret: dict[str, str] = {}
    for item in loads(r.stdout)["generalDiagnostics"]:
        message = item["message"]
        if match := re.match(r'Type of "(\w+)" is "(.*)"$', message):
            name, value = match.groups()
            assert name not in ret
            ret[name] = value
    assert r.returncode == 0, r.stderr
    return ret


def _run_pyright_on_whole_project():
    r = run(
        [sys.executable, "-m", "pyright", "--outputjson", "."],
        capture_output=True,
    )
    assert r.returncode == 0, r.stderr
    return [
        item
        for item in loads(r.stdout)["generalDiagnostics"]
        if not item["message"].startswith(
            ('Type of "', 'Import "setuptools" could not be resolved')
        )
    ]


class UnpackTyping(TestCase):
    def pyright_packer_without_default(self):
        packer_without_default = Packer().packb
        reveal_type(packer_without_default)

    def pyright_packer_with_default(self):
        def default(value: NameError):
            pass

        packer_with_default = Packer(default=default).packb
        reveal_type(packer_with_default)

    def pyright_unpacker_with_ext(self):
        def ext_hook(ext: Ext):
            if ext.code == 0x255:
                return NameError()

        unpacker = Unpacker(ext_hook=ext_hook)
        unpacker_with_ext = unpacker.unpackb(b"")
        reveal_type(unpacker_with_ext)

    def pyright_unpacker_without_ext(self):
        unpacker = Unpacker()
        unpacker_without_ext = unpacker.unpackb(b"")
        reveal_type(unpacker_without_ext)

    def test_pyright_whole_project(self):
        diagnostics = _run_pyright_on_whole_project()
        self.assertEqual(len(diagnostics), 0, msg=f"{diagnostics!r}")

    def test_pyright_types(self) -> None:
        types = _run_pyright_types()
        self.assertEqual(
            types.pop("unpacker_with_ext"),
            (
                "Mapping[str | int | float | bool | bytes | Ext | Raw | datetime | Timestamp "
                "| None, ... | Mapping[str, Value] | Sequence[Value] | str | int | float | "
                "bool | bytes | Ext | Raw | datetime | Timestamp | bytearray | None] | "
                "Mapping[str, Mapping[str | int | float | bool | bytes | Ext | Raw | datetime "
                "| Timestamp | None, Value] | ... | Sequence[Value] | str | int | float | "
                "bool | bytes | Ext | Raw | datetime | Timestamp | bytearray | None] | "
                "Sequence[Mapping[str | int | float | bool | bytes | Ext | Raw | datetime | "
                "Timestamp | None, Value] | Mapping[str, Value] | ... | str | int | float | "
                "bool | bytes | Ext | Raw | datetime | Timestamp | bytearray | None] | str | "
                "int | float | bool | bytes | Ext | Raw | datetime | Timestamp | bytearray | "
                "NameError | None"
            ),
        )
        self.assertEqual(
            types.pop("unpacker_without_ext"),
            (
                "Mapping[str | int | float | bool | bytes | Ext | Raw | datetime | Timestamp "
                "| None, ... | Mapping[str, Value] | Sequence[Value] | str | int | float | "
                "bool | bytes | Ext | Raw | datetime | Timestamp | bytearray | None] | "
                "Mapping[str, Mapping[str | int | float | bool | bytes | Ext | Raw | datetime "
                "| Timestamp | None, Value] | ... | Sequence[Value] | str | int | float | "
                "bool | bytes | Ext | Raw | datetime | Timestamp | bytearray | None] | "
                "Sequence[Mapping[str | int | float | bool | bytes | Ext | Raw | datetime | "
                "Timestamp | None, Value] | Mapping[str, Value] | ... | str | int | float | "
                "bool | bytes | Ext | Raw | datetime | Timestamp | bytearray | None] | str | "
                "int | float | bool | bytes | Ext | Raw | datetime | Timestamp | bytearray | "
                "None"
            ),
        )
        self.assertEqual(
            types.pop("packer_without_default"),
            (
                "(obj: Mapping[str | int | float | bool | bytes | Ext | Raw | datetime | "
                "Timestamp | None, ... | Mapping[str, Value] | Sequence[Value] | str | int | "
                "float | bool | bytes | Ext | Raw | datetime | Timestamp | bytearray | None] "
                "| Mapping[str, Mapping[str | int | float | bool | bytes | Ext | Raw | "
                "datetime | Timestamp | None, Value] | ... | Sequence[Value] | str | int | "
                "float | bool | bytes | Ext | Raw | datetime | Timestamp | bytearray | None] "
                "| Sequence[Mapping[str | int | float | bool | bytes | Ext | Raw | datetime | "
                "Timestamp | None, Value] | Mapping[str, Value] | ... | str | int | float | "
                "bool | bytes | Ext | Raw | datetime | Timestamp | bytearray | None] | str | "
                "int | float | bool | bytes | Ext | Raw | datetime | Timestamp | bytearray | "
                "None) -> bytes"
            ),
        )
        self.assertEqual(
            types.pop("packer_with_default"),
            (
                "(obj: Mapping[str | int | float | bool | bytes | Ext | Raw | datetime | "
                "Timestamp | None, ... | Mapping[str, Value] | Sequence[Value] | str | int | "
                "float | bool | bytes | Ext | Raw | datetime | Timestamp | bytearray | None] "
                "| Mapping[str, Mapping[str | int | float | bool | bytes | Ext | Raw | "
                "datetime | Timestamp | None, Value] | ... | Sequence[Value] | str | int | "
                "float | bool | bytes | Ext | Raw | datetime | Timestamp | bytearray | None] "
                "| Sequence[Mapping[str | int | float | bool | bytes | Ext | Raw | datetime | "
                "Timestamp | None, Value] | Mapping[str, Value] | ... | str | int | float | "
                "bool | bytes | Ext | Raw | datetime | Timestamp | bytearray | None] | str | "
                "int | float | bool | bytes | Ext | Raw | datetime | Timestamp | bytearray | "
                "NameError | None) -> bytes"
            ),
        )
        self.assertEqual(len(types), 0)

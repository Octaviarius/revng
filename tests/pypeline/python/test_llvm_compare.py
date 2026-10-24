"""Optional installed-LLVM regression checks for native benchmark comparison."""

import os
import subprocess
from pathlib import Path

import pytest
from llvm_compare import LLVMText


@pytest.fixture
def llvm():
    directory = os.environ.get("REVNG_LLVM_LIBRARY_DIR")
    if not directory:
        pytest.skip("Set REVNG_LLVM_LIBRARY_DIR for installed LLVM comparison checks")
    directory = Path(directory)
    text = LLVMText(directory)

    def assemble(source):
        return subprocess.check_output(
            [str(directory.parent / "bin/llvm-as"), "-o", "-"], input=source.encode()
        )

    return directory, text, assemble


def test_local_type_names_only(llvm):
    _, text, assemble = llvm
    source = "%Named = type { i32 }\n@storage = global %Named zeroinitializer\ndefine i32 @f(i32 %a) { %b = add i32 %a, 7 ret i32 %b }\n"
    changed = source.replace("Named", "Named.123").replace("%a", "%local.8").replace("%b", "%other")
    assert text(assemble(source)) == text(assemble(changed))


@pytest.mark.parametrize("changed", ["sub i32 %a, 7", "add i32 %a, 8"])
def test_instruction_and_constant_changes_are_detected(llvm, changed):
    _, text, assemble = llvm
    source = "define i32 @f(i32 %a) { %b = add i32 %a, 7 ret i32 %b }\n"
    assert text(assemble(source)) != text(assemble(source.replace("add i32 %a, 7", changed)))


def test_comparator_matches_llvm_tools(llvm):
    directory, text, assemble = llvm
    bitcode = assemble("define i32 @f(i32 %x) { ret i32 %x }\n")
    canonical = subprocess.check_output(
        [str(directory.parent / "bin/opt"), "-passes=strip-nondebug"], input=bitcode
    )
    expected = subprocess.check_output(
        [str(directory.parent / "bin/llvm-dis"), "-o", "-"], input=canonical
    )
    assert text(bitcode) == expected


@pytest.mark.parametrize(
    "passes", [b"strip-nondebug", b"strip-nondebug,function(instcombine,reassociate,gvn)"]
)
def test_side_effects_and_constants_are_not_ignored(llvm, passes):
    directory, _, assemble = llvm
    text = LLVMText(directory, passes)
    source = "declare void @a()\ndeclare void @b()\ndefine i32 @f() { call void @a() call void @b() ret i32 7 }\n"
    reordered = source.replace("call void @a() call void @b()", "call void @b() call void @a()")
    assert text(assemble(source)) != text(assemble(reordered))
    assert text(assemble(source)) != text(assemble(source.replace("ret i32 7", "ret i32 8")))


def test_function_declaration_order_is_not_semantic(llvm):
    _, text, assemble = llvm
    first = "declare void @z()\ndefine i32 @f() { call void @z() ret i32 7 }\n"
    second = "define i32 @f() { call void @z() ret i32 7 }\ndeclare void @z()\n"
    assert text(assemble(first)) == text(assemble(second))

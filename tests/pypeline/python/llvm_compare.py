"""Exact LLVM text comparison using the installed LLVM C API, not Python normalization."""

import ctypes as c


class LLVMText:
    def __init__(self, directory, passes=b"strip-nondebug"):
        self.canonical_passes = passes
        core = c.CDLL(str(directory / "libLLVMCore.so.16"))
        reader = c.CDLL(str(directory / "libLLVMBitReader.so.16"))
        passes = c.CDLL(str(directory / "libLLVMPasses.so.16"))
        self.passes = passes
        self.core = core
        self.reader = reader
        specifications = [
            (passes, "LLVMCreatePassBuilderOptions", [], c.c_void_p),
            (passes, "LLVMDisposePassBuilderOptions", [c.c_void_p], None),
            (passes, "LLVMRunPasses", [c.c_void_p, c.c_char_p, c.c_void_p, c.c_void_p], c.c_void_p),
            (core, "LLVMContextCreate", [], c.c_void_p),
            (core, "LLVMContextDispose", [c.c_void_p], None),
            (
                core,
                "LLVMCreateMemoryBufferWithMemoryRangeCopy",
                [c.c_char_p, c.c_size_t, c.c_char_p],
                c.c_void_p,
            ),
            (core, "LLVMDisposeMemoryBuffer", [c.c_void_p], None),
            (
                reader,
                "LLVMParseBitcodeInContext2",
                [c.c_void_p, c.c_void_p, c.POINTER(c.c_void_p)],
                c.c_int,
            ),
            (core, "LLVMPrintModuleToString", [c.c_void_p], c.c_void_p),
            (core, "LLVMDisposeMessage", [c.c_void_p], None),
            (core, "LLVMDisposeModule", [c.c_void_p], None),
        ]
        import hashlib
        import os
        import subprocess
        import tempfile
        from pathlib import Path

        source = Path(__file__).with_name("llvm_sort.cpp")
        key = hashlib.sha256(source.read_bytes() + str(directory).encode()).hexdigest()[:20]
        cache = Path(tempfile.gettempdir()) / ("revng-llvm-compare-" + key)
        cache.mkdir(exist_ok=True)
        library = cache / "sort.so"
        if not library.exists():
            temporary = cache / f"sort-{os.getpid()}.so"
            subprocess.run(
                [
                    str(directory.parent / "bin/clang++"),
                    "-shared",
                    "-fPIC",
                    "-O2",
                    "-std=c++17",
                    "-fno-exceptions",
                    "-fno-rtti",
                    "-I" + str(directory.parent / "include"),
                    str(source),
                    str(directory / "libLLVMCore.so.16"),
                    "-Wl,-rpath," + str(directory),
                    "-o",
                    str(temporary),
                ],
                check=True,
            )
            temporary.replace(library)
        self.sort_library = c.CDLL(str(library))
        self.sort_module = self.sort_library.revngSortLLVMModule
        self.sort_module.argtypes = [c.c_void_p]
        self.sort_module.restype = None
        for library, name, arguments, result in specifications:
            function = getattr(library, name)
            function.argtypes = arguments
            function.restype = result

    def __call__(self, data):
        context = self.core.LLVMContextCreate()
        buffer = self.core.LLVMCreateMemoryBufferWithMemoryRangeCopy(
            bytes(data), len(data), b"<stdin>"
        )
        module = c.c_void_p()
        message = None
        try:
            if self.reader.LLVMParseBitcodeInContext2(context, buffer, c.byref(module)):
                raise ValueError("LLVM could not parse benchmark output")
            self.sort_module(module)
            options = self.passes.LLVMCreatePassBuilderOptions()
            try:
                error = self.passes.LLVMRunPasses(module, self.canonical_passes, None, options)
                if error:
                    raise ValueError("LLVM canonicalization failed")
            finally:
                self.passes.LLVMDisposePassBuilderOptions(options)
            message = self.core.LLVMPrintModuleToString(module)
            return c.string_at(message)
        finally:
            if message:
                self.core.LLVMDisposeMessage(message)
            if module:
                self.core.LLVMDisposeModule(module)
            self.core.LLVMDisposeMemoryBuffer(buffer)
            self.core.LLVMContextDispose(context)

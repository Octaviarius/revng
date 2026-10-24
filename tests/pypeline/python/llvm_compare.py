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

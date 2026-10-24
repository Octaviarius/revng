// This file is distributed under the MIT License. See LICENSE.md for details.
// Comparison-only adapter: reorder LLVM module symbol lists without changing IR.
#include "llvm-c/Core.h"
#include "llvm/IR/Module.h"
#include "llvm/IR/Function.h"
#include "llvm/IR/GlobalVariable.h"
#include "llvm/IR/GlobalAlias.h"

extern "C" void revngSortLLVMModule(LLVMModuleRef Reference) {
  auto *Module = llvm::unwrap(Reference);
  auto ByName = [](const auto &A, const auto &B) {
    return A.getName() < B.getName();
  };
  Module->getFunctionList().sort(ByName);
  Module->getGlobalList().sort(ByName);
  Module->getAliasList().sort(ByName);
}

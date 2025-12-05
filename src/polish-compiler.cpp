#include <overload.hpp>
#include <polish-compiler.hpp>

#include <cstring>
#include <sys/mman.h>
#include <vector>

struct PolishCompiler::Impl {
    uint8_t* code_buffer = nullptr;
    size_t buffer_capacity = 0;
    size_t buffer_used = 0;

    Impl() {
        buffer_capacity = 64 * 1024 * 1024;

        code_buffer = static_cast<uint8_t*>(
            mmap(nullptr, buffer_capacity, PROT_READ | PROT_WRITE | PROT_EXEC,
                 MAP_PRIVATE | MAP_ANONYMOUS, -1, 0));
    }

    ~Impl() {
        if (code_buffer && code_buffer != MAP_FAILED) {
            munmap(code_buffer, buffer_capacity);
        }
    }

    void* Compile(std::span<const PolishOp> program, size_t nargs) {
        int64_t stack_size = static_cast<int64_t>(nargs);

        for (const auto& op : program) {
            bool valid = std::visit(
                Overload{
                    [&stack_size](BinaryOp) {
                        if (stack_size < 2) {
                            return false;
                        }
                        stack_size--;
                        return true;
                    },
                    [&stack_size](Duplicate dup) {
                        if (dup.from >= static_cast<size_t>(stack_size)) {
                            return false;
                        }
                        stack_size++;
                        return true;
                    },
                    [&stack_size](Push) {
                        stack_size++;
                        return true;
                    },
                    [&stack_size](Pop) {
                        if (stack_size < 1) {
                            return false;
                        }
                        stack_size--;
                        return true;
                    },
                    [&stack_size](Swap) {
                        if (stack_size < 2) {
                            return false;
                        }
                        return true;
                    },
                    [&stack_size](TraceElement te) {
                        if (te.index >= static_cast<size_t>(stack_size)) {
                            return false;
                        }
                        return true;
                    },
                },
                op);

            if (!valid) {
                return nullptr;
            }
        }

        if (stack_size != 1) {
            return nullptr;
        }

        std::vector<uint8_t> code;
        // Reserve space: ~10 bytes per operation + args
        code.reserve(nargs * 2 + program.size() * 10);

        auto emit = [&code](std::initializer_list<uint8_t> bytes) {
            for (uint8_t b : bytes) {
                code.push_back(b);
            }
        };

        auto emit_u32 = [&code](uint32_t val) {
            code.push_back(val & 0xFF);
            code.push_back((val >> 8) & 0xFF);
            code.push_back((val >> 16) & 0xFF);
            code.push_back((val >> 24) & 0xFF);
        };

        auto emit_u64 = [&code](uint64_t val) {
            for (int i = 0; i < 8; i++) {
                code.push_back((val >> (i * 8)) & 0xFF);
            }
        };

        // System V ABI: rdi, rsi, rdx, rcx, r8, r9
        // push rdi=0x57, push rsi=0x56, push rdx=0x52, push rcx=0x51
        // push r8=0x41 0x50, push r9=0x41 0x51
        const uint8_t push_regs[] = {0x57, 0x56, 0x52, 0x51};

        for (size_t i = 0; i < nargs && i < 4; i++) {
            code.push_back(push_regs[i]);
        }
        if (nargs > 4) {
            emit({0x41, 0x50});  // push r8
        }
        if (nargs > 5) {
            emit({0x41, 0x51});  // push r9
        }

        stack_size = static_cast<int64_t>(nargs);
        // code generation
        for (const auto& op : program) {
            std::visit(
                Overload{
                    [&](BinaryOp bin_op) {
                        switch (bin_op.kind) {
                        case BinaryOp::Add:
                            // All register operations - faster than
                            // read-modify-write
                            emit({0x58});              // pop rax (rhs)
                            emit({0x5A});              // pop rdx (lhs)
                            emit({0x48, 0x01, 0xD0});  // add rax, rdx
                            emit({0x50});              // push rax
                            break;
                        case BinaryOp::Sub:
                            // pop rax (rhs), pop rdx (lhs), sub rdx, rax, push
                            // rdx
                            emit({0x58});  // pop rax (rhs)
                            emit({0x5A});  // pop rdx (lhs)
                            emit({0x48, 0x29,
                                  0xC2});  // sub rdx, rax (rdx = lhs - rhs)
                            emit({0x52});  // push rdx
                            break;
                        case BinaryOp::Mult:
                            // pop rdx; pop rax; imul rax, rdx; push rax
                            // imul is faster than mul (doesn't compute high
                            // bits)
                            emit({0x5A});                    // pop rdx
                            emit({0x58});                    // pop rax
                            emit({0x48, 0x0F, 0xAF, 0xC2});  // imul rax, rdx
                            emit({0x50});                    // push rax
                            break;
                        }
                        stack_size--;
                    },
                    [&](Duplicate dup) {
                        // push qword [rsp + offset] - single instruction!
                        size_t offset = dup.from * 8;
                        if (offset == 0) {
                            emit({0xFF, 0x34, 0x24});  // push qword [rsp]
                        } else if (offset < 128) {
                            emit({0xFF, 0x74, 0x24,
                                  static_cast<uint8_t>(
                                      offset)});  // push qword [rsp+disp8]
                        } else {
                            emit(
                                {0xFF, 0xB4, 0x24});  // push qword [rsp+disp32]
                            emit_u32(static_cast<uint32_t>(offset));
                        }
                        stack_size++;
                    },
                    [&](Push p) {
                        if (p.value < 0x80000000u) {
                            // value < 2^31, we push directly
                            // push imm32    (sign extended to 64-bit)
                            emit({0x68});
                            emit_u32(p.value);
                        } else {  // Value >= 2^31, so we move value to rax and
                                  // only than push it

                            // mov rax, imm64; push rax
                            emit({0x48, 0xB8});
                            emit_u64(static_cast<uint64_t>(p.value));
                            emit({0x50});
                        }
                        stack_size++;
                    },
                    [&](Pop) {
                        // add rsp, 8
                        emit({0x48, 0x83, 0xC4, 0x08});
                        stack_size--;
                    },
                    [&](Swap) {
                        // xchg with memory is very slow (~20 cycles)
                        // Use two pops and two pushes instead (4 cycles total)
                        emit({0x58});  // pop rax  (top)
                        emit({0x5A});  // pop rdx  (second)
                        emit({0x50});  // push rax (former top -> second
                                       // position)
                        emit({0x52});  // push rdx (former second -> top)
                    },
                    [&](TraceElement te) {
                        // Load element value into rsi (2nd argument)
                        size_t offset = te.index * 8;
                        if (offset == 0) {
                            emit({0x48, 0x8B, 0x34, 0x24});  // mov rsi, [rsp]
                        } else if (offset < 128) {
                            // mov rsi, [rsp+disp8]
                            emit({0x48, 0x8B, 0x74, 0x24,
                                  static_cast<uint8_t>(offset)});
                        } else {
                            // mov rsi, [rsp+disp32]
                            emit({0x48, 0x8B, 0xB4, 0x24});
                            emit_u32(static_cast<uint32_t>(offset));
                        }

                        // Load data pointer into rdi (1st argument)
                        emit({0x48, 0xBF});  // movabs rdi, imm64
                        emit_u64(reinterpret_cast<uint64_t>(te.data));

                        // Load function pointer into rax
                        emit({0x48, 0xB8});  // movabs rax, imm64
                        emit_u64(reinterpret_cast<uint64_t>(te.trace_fn));

                        bool need_align =
                            (stack_size % 2 ==
                             0);  // "call" function pushes 4 bytes to the
                                  // stack, so we need to compensate if
                                  // stack_size is even
                        if (need_align) {
                            emit({0x48, 0x83, 0xEC, 0x08});  // sub rsp, 8
                        }
                        emit({0xFF, 0xD0});  // call rax
                        if (need_align) {
                            emit({0x48, 0x83, 0xC4, 0x08});  // add rsp, 8
                        }
                    },
                },
                op);
        }

        emit({0x58});  // pop rax
        emit({0xC3});  // ret

        if (buffer_used + code.size() > buffer_capacity) {
            return nullptr;  // Out of space
        }

        void* result = code_buffer + buffer_used;
        std::memcpy(result, code.data(), code.size());
        buffer_used += code.size();
        // Align to 16 bytes for next function
        buffer_used = (buffer_used + 15) & ~15;

        return result;
    }
};

PolishCompiler::PolishCompiler() : impl_(std::make_unique<Impl>()) {
}

PolishCompiler::~PolishCompiler() = default;

void* PolishCompiler::Compile(std::span<const PolishOp> program, size_t nargs) {
    return impl_->Compile(program, nargs);
}

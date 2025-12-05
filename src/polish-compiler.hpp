#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <variant>

struct BinaryOp {
    enum Kind {
        Add,
        Sub,
        Mult,
    };

    Kind kind;
};

struct Duplicate {
    size_t from;
};

struct Push {
    uint32_t value;
};

struct Pop {};

struct Swap {};

struct TraceElement {
    size_t index;
    void* data;
    void (*trace_fn)(void*, uint64_t);
};

using PolishOp =
    std::variant<BinaryOp, Duplicate, Push, Pop, Swap, TraceElement>;

class PolishCompiler {
  public:
    PolishCompiler();

    ~PolishCompiler();

    void* Compile(std::span<const PolishOp> program, size_t nargs);

  private:
    // https://en.cppreference.com/w/cpp/language/pimpl.html
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

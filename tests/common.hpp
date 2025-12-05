#include <polish-compiler.hpp>

#include <build.hpp>
#include <overload.hpp>

#include <catch2/catch_test_macros.hpp>

#include <optional>
#include <span>
#include <utility>
#include <vector>

constexpr inline auto add = BinaryOp{BinaryOp::Add};
constexpr inline auto sub = BinaryOp{BinaryOp::Sub};
constexpr inline auto mult = BinaryOp{BinaryOp::Mult};

#define CHECK_ON(fn_jit, fn_correct, ...)                                      \
    do {                                                                       \
        if constexpr (kBuildType != BuildType::ASan) {                         \
            CHECK(fn_jit(__VA_ARGS__) == fn_correct(__VA_ARGS__));             \
        }                                                                      \
    } while (false)

template <class T>
struct CompilationOutput;

template <size_t... Idx>
struct CompilationOutput<std::index_sequence<Idx...>> {
    template <size_t>
    using Input = uint64_t;

    using Type = uint64_t (*)(Input<Idx>...);
};

struct WrappedPolishCompiler : PolishCompiler {
    template <size_t NArgs>
    auto Compile(std::span<const PolishOp> program) {
        return reinterpret_cast<
            CompilationOutput<std::make_index_sequence<NArgs>>::Type>(
            PolishCompiler::Compile(program, NArgs));
    }
};

template <class... Args>
std::optional<uint64_t> RunPolishExpr(std::span<const PolishOp> program,
                                      Args... args) {
    std::vector<uint64_t> st;
    (st.push_back(args), ...);

    for (auto op : program) {
        Overload apply{
            [&st](BinaryOp op) {
                if (st.size() < 2) {
                    return false;
                }
                auto rhs = st.back();
                st.pop_back();
                auto lhs = st.back();
                uint64_t result = 0;
                switch (op.kind) {
                case BinaryOp::Add:
                    result = lhs + rhs;
                    break;
                case BinaryOp::Sub:
                    result = lhs - rhs;
                    break;
                case BinaryOp::Mult:
                    result = lhs * rhs;
                    break;
                }
                st.back() = result;
                return true;
            },
            [&st](Duplicate op) {
                if (op.from >= st.size()) {
                    return false;
                }
                auto value = st[st.size() - op.from - 1];
                st.push_back(value);
                return true;
            },
            [&st](Push op) {
                st.push_back(op.value);
                return true;
            },
            [&st](Pop /*op*/) {
                if (st.size() == 0) {
                    return false;
                }
                st.pop_back();
                return true;
            },
            [&st](Swap /*op*/) {
                if (st.size() < 2) {
                    return false;
                }
                std::swap(st.back(), st[st.size() - 2]);
                return true;
            },
            [&st](TraceElement op) {
                if (op.index >= st.size()) {
                    return false;
                }
                op.trace_fn(op.data, st[st.size() - op.index - 1]);
                return true;
            },
        };
        if (!std::visit(apply, op)) {
            return std::nullopt;
        }
    }
    if (st.size() != 1) {
        return std::nullopt;
    }
    return st.back();
}

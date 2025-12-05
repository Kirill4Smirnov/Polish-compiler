#include "common.hpp"

#include <pcg-random.hpp>

#include <catch2/catch_get_random_seed.hpp>
#include <catch2/catch_test_macros.hpp>

TEST_CASE("Push32") {
    PCGRandom rng{Catch::getSeed()};
    WrappedPolishCompiler compiler;
    {
        constexpr auto kShift = uint32_t{1} << 31;
        auto fn = compiler.Compile<2>({{
            Push{kShift},
            add,
            Swap{},
            Push{kShift},
            add,
            mult,
        }});
        REQUIRE(fn != 0);

        auto fn_correct = [](uint64_t a, uint64_t b) {
            return (a + kShift) * (b + kShift);
        };

        CHECK_ON(fn, fn_correct, 0, 0);
        CHECK_ON(fn, fn_correct, 123, 456);
        for (size_t i = 0; i < 10; ++i) {
            auto a = rng.Generate64();
            auto b = rng.Generate64();
            CHECK_ON(fn, fn_correct, a, b);
        }
    }
}

TEST_CASE("TraceError") {
    PolishCompiler compiler;

    {
        std::vector<PolishOp> seq = {
            TraceElement{
                .index = 0,
                .data = nullptr,
                .trace_fn = nullptr,
            },
        };
        {
            auto fn = compiler.Compile(seq, 0);
            CHECK(fn == nullptr);
        }
        {
            auto fn = compiler.Compile(seq, 1);
            CHECK(fn != nullptr);
        }
        {
            auto fn = compiler.Compile(seq, 2);
            CHECK(fn == nullptr);
        }
    }

    {
        constexpr size_t kElements = 20;
        std::vector<PolishOp> seq(kElements, Push{1});

        for (size_t args = 0; args <= 6; ++args) {
            for (size_t traced = 17; traced <= 27; ++traced) {
                seq.emplace_back(TraceElement{
                    .index = traced,
                    .data = nullptr,
                    .trace_fn = nullptr,
                });
                for (size_t i = 0; i < kElements + args - 1; ++i) {
                    seq.emplace_back(Pop{});
                }

                auto fn = compiler.Compile(seq, args);

                INFO("Args = " << args << ", traced = " << traced);
                if (traced < kElements + args) {
                    CHECK(fn != nullptr);
                } else {
                    CHECK(fn == nullptr);
                }

                seq.resize(kElements);
            }
        }
    }
}

extern "C" void RecordRsp(void* data, uint64_t element);
asm(R"(
RecordRsp:
    mov [rdi], rsp
    ret
)");

TEST_CASE("StackAlignment") {
    WrappedPolishCompiler compiler;
    std::vector<uint64_t> rsps(4, 0);

    auto fn = compiler.Compile<2>({{
        TraceElement{
            .index = 1,
            .data = &rsps[0],
            .trace_fn = RecordRsp,
        },
        Push{1},
        TraceElement{
            .index = 0,
            .data = &rsps[1],
            .trace_fn = RecordRsp,
        },
        Push{2},
        TraceElement{
            .index = 3,
            .data = &rsps[2],
            .trace_fn = RecordRsp,
        },
        Push{3},
        TraceElement{
            .index = 3,
            .data = &rsps[3],
            .trace_fn = RecordRsp,
        },
        Pop{},
        Pop{},
        Pop{},
        Pop{},
    }});

    REQUIRE(fn != nullptr);

    if constexpr (kBuildType == BuildType::ASan) {
        return;
    }

    fn(3, 4);

    for (size_t i = 0; i + 1 < rsps.size(); ++i) {
        INFO("i = " << i);
        CHECK(rsps[i] >= rsps[i + 1]);
    }
    CHECK(rsps.front() > rsps.back());

    for (size_t i = 0; i < rsps.size(); ++i) {
        INFO("i = " << i);
        CHECK((rsps[i] & 15) == 8);
    }
}

TEST_CASE("Tracing") {
    WrappedPolishCompiler compiler;
    std::vector<uint64_t> trace;
    std::vector<PolishOp> program;
    PCGRandom rng{Catch::getSeed()};

    auto record = [](void* where, uint64_t value) {
        reinterpret_cast<std::vector<uint64_t>*>(where)->push_back(value);
    };
    auto trace_elem = [&](size_t idx) {
        return TraceElement{
            .index = idx,
            .data = &trace,
            .trace_fn = record,
        };
    };

    constexpr size_t kNArgs = 6;
    constexpr size_t kElements = 10;

    for (size_t i = 0; i < kElements; ++i) {
        program.emplace_back(Push{rng.Generate32() | 1});
    }
    for (size_t i = 0; i < kElements + kNArgs; ++i) {
        program.emplace_back(trace_elem(i));
    }
    for (size_t i = 0; i < kElements + kNArgs - 1; ++i) {
        program.emplace_back(mult);
    }

    program.emplace_back(trace_elem(0));

    auto fn = compiler.Compile<6>(program);
    REQUIRE(fn != nullptr);

    if constexpr (kBuildType == BuildType::ASan) {
        return;
    }

    for (size_t i = 0; i < 5; ++i) {
        auto a = rng.Generate64();
        auto b = rng.Generate64();
        auto c = rng.Generate64();
        auto d = rng.Generate64();
        auto e = rng.Generate64();
        auto f = rng.Generate64();

        auto res1 = fn(a, b, c, d, e, f);
        auto trace1 = std::move(trace);
        trace.clear();

        auto res2 = RunPolishExpr(program, a, b, c, d, e, f);
        auto trace2 = std::move(trace);
        trace.clear();

        INFO("a = " << a << ", b = " << b << ", c = " << c << ", d = " << d
                    << ", e = " << e << ", f = " << f);
        REQUIRE(res2.has_value());
        CHECK(res1 == *res2);
        CHECK(trace1 == trace2);
    }
}

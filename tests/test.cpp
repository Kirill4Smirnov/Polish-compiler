#include "common.hpp"

#include <polish-compiler.hpp>

#include <benchmark/run.hpp>
#include <build.hpp>
#include <pcg-random.hpp>

#include <catch2/catch_get_random_seed.hpp>
#include <catch2/catch_test_macros.hpp>

#include <iomanip>
#include <random>

using namespace std::chrono_literals;

TEST_CASE("Simple") {
    WrappedPolishCompiler compiler;
    {
        // fn(a, b) = a * (b + 3)
        auto fn = compiler.Compile<2>({{
            Push{3},
            add,
            mult,
        }});
        REQUIRE(fn != nullptr);

        auto fn_correct = [](uint64_t a, uint64_t b) { return a * (b + 3); };
        CHECK_ON(fn, fn_correct, 1, 2);
        CHECK_ON(fn, fn_correct, 1, 2);
        CHECK_ON(fn, fn_correct, 7, 10);
        CHECK_ON(fn, fn_correct, 123456789, 987654321);
        CHECK_ON(fn, fn_correct, 12345678910111213, 131211109876543210);
        CHECK_ON(fn, fn_correct, -5, -10);
        CHECK_ON(fn, fn_correct, -10, 5);
    }

    {
        // fn(a, b, c, d, e, f) = a + b + c + d + e + f
        auto fn = compiler.Compile<6>({{
            add,
            add,
            add,
            add,
            add,
        }});
        REQUIRE(fn != nullptr);
        auto fn_correct = [](uint64_t a, uint64_t b, uint64_t c, uint64_t d,
                             uint64_t e,
                             uint64_t f) { return a + b + c + d + e + f; };
        CHECK_ON(fn, fn_correct, 0, 0, 100, 200, 4000, 137869691762391);
        CHECK_ON(fn, fn_correct, 1ull << 60, 1ull << 61, 1ull << 62, 1ull << 62,
                 1ull << 62, 1ull << 62);
    }

    {
        // fn() = 1
        auto fn = compiler.Compile<0>({{Push{1}}});
        auto fn_correct = []() -> uint64_t { return 1; };
        REQUIRE(fn != nullptr);
        CHECK_ON(fn, fn_correct);
    }

    {
        // fn(a, b) = a - b
        auto fn = compiler.Compile<2>({{sub}});
        auto fn_correct = [](uint64_t a, uint64_t b) { return a - b; };
        CHECK_ON(fn, fn_correct, 1, 2);
        CHECK_ON(fn, fn_correct, 123456, 654321);
        CHECK_ON(fn, fn_correct, 78634, 4321);
    }
}

TEST_CASE("Advanced") {
    WrappedPolishCompiler compiler;

    {
        // fn(a, b, c, d, e, f) = ((c + d) - (e * f)) * (a + b + 1)
        auto fn = compiler.Compile<6>({{
            mult,
            Swap{},
            Duplicate{2},
            add,
            Swap{},
            sub,
            Swap{},
            Pop{},
            Swap{},
            Duplicate{2},
            Push{1},
            add,
            add,
            mult,
            Swap{},
            Pop{},
        }});
        auto fn_correct = [](uint64_t a, uint64_t b, uint64_t c, uint64_t d,
                             uint64_t e, uint64_t f) {
            return ((c + d) - (e * f)) * (a + b + 1);
        };
        REQUIRE(fn != nullptr);
        CHECK_ON(fn, fn_correct, 1, 2, 3, 4, 5, 6);
        CHECK_ON(fn, fn_correct, 1234, 5678, 9101, 1121, 3141, 5161);
    }

    {
        // fn(a, b) = 55 * a + 34 * b
        auto fn = compiler.Compile<2>({{
            Duplicate{1}, add, Swap{},  // a+b, a
            Duplicate{1}, add, Swap{},  // 2a+b, a+b
            Duplicate{1}, add, Swap{},  // 3a+2b, 2a+b
            Duplicate{1}, add, Swap{},  // 5a+3b, 3a+2b
            Duplicate{1}, add, Swap{},  // 8a+5b, 5a+3b
            Duplicate{1}, add, Swap{},  // 13a+8b, 8a+5b
            Duplicate{1}, add, Swap{},  // 21a+13b, 13a+8b
            Duplicate{1}, add, Swap{},  // 34a+21b, 21a+13b
            Duplicate{1}, add, Swap{},  // 55a+34b, 34a+21b
            Pop{},
        }});
        auto fn_correct = [](uint64_t a, uint64_t b) {
            return 55 * a + 34 * b;
        };

        REQUIRE(fn != nullptr);
        CHECK_ON(fn, fn_correct, 0, 0);
        CHECK_ON(fn, fn_correct, 1, 1);
        CHECK_ON(fn, fn_correct, 67861354871342, 17392786946264756);
    }

    {
        // fn(a) = a+1993253540
        auto fn = compiler.Compile<1>({{
            Push{1993253540},
            add,
        }});
        auto fn_correct = [](uint64_t a) { return a + 1993253540; };

        REQUIRE(fn != nullptr);
        CHECK_ON(fn, fn_correct, 0);
        CHECK_ON(fn, fn_correct, 1614399308);
        CHECK_ON(fn, fn_correct, 4140737188);
        CHECK_ON(fn, fn_correct, 14248468316710639744ull);
    }

    {
        // fn(a) = a+5818650909575623225
        auto fn = compiler.Compile<1>({{
            Push{1354760236},
            Push{1 << 16},
            Push{1 << 16},
            mult,
            mult,
            Push{2034381369},
            add,
            add,
        }});
        auto fn_correct = [](uint64_t a) { return a + 5818650909575623225ull; };

        REQUIRE(fn != nullptr);
        CHECK_ON(fn, fn_correct, 13474150119330595010ull);
        CHECK_ON(fn, fn_correct, 9712743489882720630ull);
        CHECK_ON(fn, fn_correct, 583110846669149394ull);
    }
}

TEST_CASE("Errors") {
    PolishCompiler compiler;
    {
        std::vector<PolishOp> seq = {};

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
        std::vector<PolishOp> seq = {
            Push{1},
            Pop{},
            Pop{},
        };

        {
            auto fn = compiler.Compile(seq, 0);
            CHECK(fn == nullptr);
        }
        {
            auto fn = compiler.Compile(seq, 1);
            CHECK(fn == nullptr);
        }
        {
            auto fn = compiler.Compile(seq, 2);
            CHECK(fn != nullptr);
        }
        {
            auto fn = compiler.Compile(seq, 3);
            CHECK(fn == nullptr);
        }
    }

    {
        std::vector<PolishOp> seq = {
            Swap{},
        };

        for (size_t nargs = 0; nargs <= 6; ++nargs) {
            auto fn = compiler.Compile(seq, nargs);
            CHECK(fn == nullptr);
        }
    }

    {
        std::vector<PolishOp> seq = {
            Duplicate{1},
        };

        for (size_t nargs = 0; nargs <= 6; ++nargs) {
            auto fn = compiler.Compile(seq, nargs);
            CHECK(fn == nullptr);
        }
    }
}

TEST_CASE("Performance") {
    if constexpr (kBuildType != BuildType::Release) {
        return;
    }

    PCGRandom rng{Catch::getSeed()};
    auto gen_31_bit = [&rng] { return rng.Generate32() >> 1; };

    auto program = [&] {
        std::discrete_distribution<size_t> d{20, 30, 25, 35};
        std::vector<PolishOp> program;

        while (program.size() < 1'000'000) {
            auto choice = d(rng);
            if (choice == 0) {
                program.push_back(Swap{});
            } else if (choice == 1) {
                program.push_back(Duplicate{1});
                program.push_back(mult);
            } else if (choice == 2) {
                program.push_back(Duplicate{1});
                program.push_back(Push{1});
                program.push_back(add);
                program.push_back(add);
            } else if (choice == 3) {
                program.push_back(
                    Push{static_cast<uint32_t>(gen_31_bit() & ~1)});
                program.push_back(add);
            } else {
                program.push_back(
                    Push{static_cast<uint32_t>(gen_31_bit() | 1)});
                program.push_back(mult);
            }
        }
        program.push_back(Swap{});
        program.push_back(Pop{});

        return program;
    }();

    WrappedPolishCompiler compiler;
    auto fn = compiler.Compile<2>(program);
    REQUIRE(fn != nullptr);

    auto measure_implementation = [&rng]<class F>(F&& impl) -> CPUTimer::Times {
        return RunWithWarmup(
            [&impl, rng]() mutable {
                return impl(rng.Generate64(), rng.Generate64());
            },
            8, 256);
    };

    auto compiled = measure_implementation(fn);
    auto interpreted =
        measure_implementation([&program](uint64_t a, uint64_t b) {
            return RunPolishExpr(program, a, b);
        });

    auto ratio = double(interpreted.TotalCpuTime().count()) /
                 double(compiled.TotalCpuTime().count());

    WARN("Compiled version is " << std::fixed << std::setprecision(3) << ratio
                                << " faster than the interpreted one");
    {
        INFO("Too slow");
        CHECK(ratio > 7.5);
    }
}

TEST_CASE("CompilationPerformance") {
    if constexpr (kBuildType != BuildType::Release) {
        return;
    }
    WrappedPolishCompiler compiler;

    CPUTimer t;

    for (uint32_t i = 0; i < 2048; ++i) {
        auto f = compiler.Compile<2>({{Push{i}, add, add}});
        REQUIRE(f(5, 6) == 5 + 6 + i);
    }

    auto times = t.GetTimes();
    REQUIRE(times.wall_time < 150ms);
}

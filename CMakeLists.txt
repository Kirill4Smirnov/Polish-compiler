file(GLOB_RECURSE SOURCES
    CONFIGURE_DEPENDS
    src/*.cpp)

add_caos_library(polish_compiler STATIC ${SOURCES})
target_include_directories(polish_compiler PUBLIC src)

add_catch_executable(test_polish_compiler tests/test.cpp)
target_link_libraries(test_polish_compiler PRIVATE caos_utils benchmark polish_compiler)

add_catch_executable(test_polish_compiler_tracing tests/test-tracing.cpp)
target_link_libraries(test_polish_compiler_tracing PRIVATE caos_utils polish_compiler)

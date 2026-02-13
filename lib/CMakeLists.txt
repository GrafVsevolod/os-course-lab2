add_library(
    vtpc
    SHARED
    vtpc.c
)

target_include_directories(
    vtpc
    PUBLIC
    .
)

target_compile_definitions(vtpc PRIVATE _GNU_SOURCE)

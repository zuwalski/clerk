
add_executable (test_clerk test_instance.c test_stream.c test_main test_util.c)

target_link_libraries (test_clerk LINK_PUBLIC clerklib)

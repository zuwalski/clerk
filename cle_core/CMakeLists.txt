# the main static lib 
add_library (clerklib backends/cle_pages_backend.c backends/cle_logs_backend.c cle_stream.c cle_task.c cle_commit.c cle_iterate.c cle_struct.c cle_object.c)

target_include_directories (clerklib PUBLIC ${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_CURRENT_SOURCE_DIR}/backends)

# a simple (demo) command-line tool
add_executable (cle_cli cle_cli.c)

target_link_libraries (cle_cli LINK_PUBLIC clerklib)

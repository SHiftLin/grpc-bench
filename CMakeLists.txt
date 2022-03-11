cmake_minimum_required(VERSION 3.5.1)

project(grpc-bench C CXX)

include(common.cmake)

# Proto file
get_filename_component(bm_proto "benchmark.proto" ABSOLUTE)
get_filename_component(bm_proto_path "${bm_proto}" PATH)

# Generated sources
set(bm_proto_srcs "${CMAKE_CURRENT_BINARY_DIR}/benchmark.pb.cc")
set(bm_proto_hdrs "${CMAKE_CURRENT_BINARY_DIR}/benchmark.pb.h")
set(bm_grpc_srcs "${CMAKE_CURRENT_BINARY_DIR}/benchmark.grpc.pb.cc")
set(bm_grpc_hdrs "${CMAKE_CURRENT_BINARY_DIR}/benchmark.grpc.pb.h")
add_custom_command(
      OUTPUT "${bm_proto_srcs}" "${bm_proto_hdrs}" "${bm_grpc_srcs}" "${bm_grpc_hdrs}"
      COMMAND ${_PROTOBUF_PROTOC}
      ARGS --grpc_out "${CMAKE_CURRENT_BINARY_DIR}"
        --cpp_out "${CMAKE_CURRENT_BINARY_DIR}"
        -I "${bm_proto_path}"
        --plugin=protoc-gen-grpc="${_GRPC_CPP_PLUGIN_EXECUTABLE}"
        "${bm_proto}"
      DEPENDS "${bm_proto}")      

# Include generated *.pb.h files
include_directories("${CMAKE_CURRENT_BINARY_DIR}")

add_library(bm_grpc_proto
  ${bm_grpc_srcs}
  ${bm_grpc_hdrs}
  ${bm_proto_srcs}
  ${bm_proto_hdrs})
target_link_libraries(bm_grpc_proto
  ${_REFLECTION}
  ${_GRPC_GRPCPP}
  ${_PROTOBUF_LIBPROTOBUF})

foreach(_target
  greeter_client
  greeter_server
  grpc_bench_server
  grpc_latency_client 
  grpc_rpc_tput_client)
  add_executable(${_target} "${_target}.cc")
  target_link_libraries(${_target}
    bm_grpc_proto
    ${_REFLECTION}
    ${_GRPC_GRPCPP}
    ${_PROTOBUF_LIBPROTOBUF})
endforeach()

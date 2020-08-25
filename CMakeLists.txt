cmake_minimum_required(VERSION 3.0.0)
project(CCGraph VERSION 0.1.0)

set(CMAKE_CXX_COMPILER "/usr/bin/g++")
set(CMAKE_C_COMPILER "/usr/bin/gcc")

SET(CMAKE_EXE_LINKER_FLAGS  "${CMAKE_EXE_LINKER_FLAGS} -lgcov")
# just use the original gcc 5.x abi, to match dependent libraries
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} --std=c++17 -fPIC -Wall -DCARES_STATICLIB -Wno-strict-aliasing -Wuninitialized -Wmaybe-uninitialized")

set(CMAKE_BUILD_TYPE "Release")
# set(CMAKE_BUILD_TYPE "Debug")

if (CMAKE_BUILD_TYPE STREQUAL "Debug")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O2 -g")
endif()

if (CMAKE_BUILD_TYPE STREQUAL "Release")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O0 -g" )
endif()

set(LIBRARY_OUTPUT_PATH ${PROJECT_SOURCE_DIR}/build/lib) 

# include(CTest)
# enable_testing()

# aux_source_directory(${PROJECT_SOURCE_DIR}/src ${PROJECT_SOURCE_DIR}/src/graphstore   DIR_SRCS)
# aux_source_directory(${PROJECT_SOURCE_DIR}/server SERVER_SRCS)
# aux_source_directory(${PROJECT_SOURCE_DIR}/proto  PROTO_SRCS)
# include_directories(${PROJECT_SOURCE_DIR}/include)

aux_source_directory(${PROJECT_SOURCE_DIR}/src/proto  PROTO_SRCS)

include_directories(${PROJECT_SOURCE_DIR}/src)
include_directories(${PROJECT_SOURCE_DIR}/src/exception)
include_directories(${PROJECT_SOURCE_DIR}/src/latch)
include_directories(${PROJECT_SOURCE_DIR}/src/store)
include_directories(${PROJECT_SOURCE_DIR}/src/utils)
include_directories(${PROJECT_SOURCE_DIR}/src/graphstore)
include_directories(${PROJECT_SOURCE_DIR}/src/transaction)
include_directories(${PROJECT_SOURCE_DIR}/src/lock)
include_directories(${PROJECT_SOURCE_DIR}/src/executor)
include_directories(${PROJECT_SOURCE_DIR}/src/bench_query)
include_directories(${PROJECT_SOURCE_DIR}/src/client)
include_directories(${PROJECT_SOURCE_DIR}/src/proto)

# add_library(ccgraphcore ${DIR_SRCS})
# add_library( SimpleGraphClient ${PROJECT_SOURCE_DIR}/client/sgclient.cc 
#                                ${PROTO_SRCS} 
#                                ${PROJECT_SOURCE_DIR}/src/node.cc)

# add_executable( SimpleGraphServer ${PROJECT_SOURCE_DIR}/server/sgserver.cc ${PROJECT_SOURCE_DIR}/server/simplegraph.grpc.pb.cc)
add_executable( AbitaryTesterMain ${PROJECT_SOURCE_DIR}/tester_main.cc)
add_executable( CCGraphClient ${PROJECT_SOURCE_DIR}/src/ccgraph_client_main.cpp
                              ${PROTO_SRCS})
add_executable( CCGraphServer ${PROJECT_SOURCE_DIR}/src/ccgraph_server_main.cpp
                              ${PROTO_SRCS})

# add_executable( SimpleGraphServer ${PROJECT_SOURCE_DIR}/server/sgserver.cc
#                                   ${PROTO_SRCS} 
#                                   ${PROJECT_SOURCE_DIR}/src/node.cc)
                                  
# add_executable( SimpleGraphClientCmdLine ${PROJECT_SOURCE_DIR}/client/sgclient_main.cc)

# add_executable( GraphDBTest 
#                 ${PROJECT_SOURCE_DIR}/test/simplegraph_test.cc)
# add_executable( ConTable2BufTest 
#                 ${PROJECT_SOURCE_DIR}/test/con_table2_buf_test.cc)
# add_executable( ConTable2BufGet2Test 
#                 ${PROJECT_SOURCE_DIR}/test/con_table2_buf_get2_test.cc)
# add_executable( ConTable2BufSizeTest 
#                 ${PROJECT_SOURCE_DIR}/test/con_table2_buf_size_test.cc)
# add_executable( TransactionTest 
#                 ${PROJECT_SOURCE_DIR}/test/transaction_test.cc)
# add_executable( SimpleGraphDBTest 
#                 ${PROJECT_SOURCE_DIR}/test/simplegraphdb_test.cc)
# add_executable( SimpleGraphServerTest 
#                 ${PROJECT_SOURCE_DIR}/test/sgserver_test.cc)

target_link_libraries(AbitaryTesterMain       folly)
target_link_libraries(AbitaryTesterMain       event)
target_link_libraries(AbitaryTesterMain       double-conversion)
target_link_libraries(AbitaryTesterMain       glog)
target_link_libraries(AbitaryTesterMain       gflags)
target_link_libraries(AbitaryTesterMain       pthread)
target_link_libraries(AbitaryTesterMain       dl)
target_link_libraries(AbitaryTesterMain       boost_context)

target_link_libraries(CCGraphServer folly event double-conversion glog gflags pthread dl boost_context protobuf grpc++ grpc++_reflection grpc++_unsecure)
# target_link_libraries(CCGraphServer pthread protobuf grpc++ grpc++_reflection  grpc++_unsecure)
target_link_libraries(CCGraphClient folly event double-conversion glog gflags pthread dl boost_context protobuf grpc++ grpc++_reflection grpc++_unsecure)


# target_link_libraries(GraphDBTest           GraphDB)
# target_link_libraries(ConTable2BufTest      GraphDB)
# target_link_libraries(ConTable2BufGet2Test  GraphDB)
# target_link_libraries(ConTable2BufSizeTest  GraphDB)
# target_link_libraries(TransactionTest       GraphDB)
# target_link_libraries(SimpleGraphDBTest     GraphDB)

# target_link_libraries(SimpleGraphServerTest SimpleGraphClient pthread protobuf gpr grpc grpc++ grpc++_reflection grpc_unsecure grpc++_unsecure)

# target_link_libraries(SimpleGraphServer     GraphDB pthread protobuf gpr grpc grpc++ grpc++_reflection grpc_unsecure grpc++_unsecure)
# target_link_libraries(SimpleGraphClient     pthread protobuf gpr grpc grpc++ grpc++_reflection grpc_unsecure grpc++_unsecure)
# target_link_libraries(SimpleGraphClientCmdLine  SimpleGraphClient   pthread protobuf gpr grpc grpc++ grpc++_reflection grpc_unsecure grpc++_unsecure)


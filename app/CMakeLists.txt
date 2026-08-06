cmake_minimum_required(VERSION 3.8)
project(doradd-bench CXX)

set(VERONA_PATH "../external/verona-rt")
set(SNMALLOC_PATH "../external/snmalloc/")

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
add_compile_options(-Wno-pointer-arith)

# Release Build
if(CMAKE_BUILD_TYPE MATCHES Release)
  set(BRANCH "release")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3")
  # show symbols in perf top (assembly)
  #set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -fno-omit-frame-pointer")
endif()

# Debug Build
if(CMAKE_BUILD_TYPE MATCHES Debug)
  set(BRANCH "debug")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -fno-omit-frame-pointer")
  #set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fsanitize=address -static-libasan")
  #set(CMAKE_LINKER_FLAGS_DEBUG "${CMAKE_LINKER_FLAGS_DEBUG} -fsanitize=address")
endif()

find_package(Threads REQUIRED)

# YCSB
add_executable(ycsb ycsb.cc)
add_compile_definitions(PREFETCH)
add_compile_definitions(CORE_PIPE)
add_compile_definitions(INDEXER)
# add_compile_definitions(RPC_LATENCY)
# add_compile_definitions(LOG_LATENCY)
#add_compile_definitions(LOG_SCHED_OHEAD)
#add_compile_definitions(ZERO_SERV_TIME)
#add_compile_definitions(TEST_TWO)

target_include_directories(ycsb PRIVATE ../src/misc)
target_include_directories(ycsb PRIVATE ../src/doradd)
target_include_directories(ycsb PRIVATE ${VERONA_PATH}/src/rt)
target_include_directories(ycsb PRIVATE ${SNMALLOC_PATH}/src)
target_compile_options(ycsb PRIVATE -mcx16 -march=native)
target_compile_definitions(ycsb PRIVATE -DSNMALLOC_CHEAP_CHECKS)
target_compile_definitions(ycsb PRIVATE -DACQUIRE_ALL)
target_link_libraries(ycsb PRIVATE ${CMAKE_THREAD_LIBS_INIT})
target_link_libraries(ycsb PRIVATE atomic)
target_include_directories(ycsb PRIVATE ../external/SPSCQueue/include/rigtorp)

add_custom_command(
  OUTPUT ${CMAKE_SOURCE_DIR}/tpcc_gen.cc
  COMMAND m4 ${CMAKE_SOURCE_DIR}/tpcc_macros.m4 ${CMAKE_SOURCE_DIR}/tpcc.cc > ${CMAKE_SOURCE_DIR}/tpcc_gen.cc
  DEPENDS ${CMAKE_SOURCE_DIR}/tpcc_macros.m4 ${CMAKE_SOURCE_DIR}/tpcc.cc
  COMMENT "Generating tpcc_gen.cc"
  VERBATIM
)

# TPCC
add_executable(tpcc ${CMAKE_SOURCE_DIR}/tpcc_gen.cc)


# add_compile_definitions(PREFETCH)
# add_compile_definitions(CORE_PIPE)
# add_compile_definitions(INDEXER)
add_compile_definitions(SINGLE_TABLE)
#add_compile_definitions(WAREHOUSE_SPLIT)

target_include_directories(tpcc PRIVATE ../src/misc)
target_include_directories(tpcc PRIVATE ../src/doradd)
target_include_directories(tpcc PRIVATE ${VERONA_PATH}/src/rt)
target_include_directories(tpcc PRIVATE ${SNMALLOC_PATH}/src)
target_compile_options(tpcc PRIVATE -mcx16 -march=native)
target_compile_definitions(tpcc PRIVATE -DSNMALLOC_CHEAP_CHECKS)
target_compile_definitions(tpcc PRIVATE -DACQUIRE_ALL)
target_link_libraries(tpcc PRIVATE ${CMAKE_THREAD_LIBS_INIT})
target_link_libraries(tpcc PRIVATE atomic)
target_include_directories(tpcc PRIVATE ../external/SPSCQueue/include/rigtorp)

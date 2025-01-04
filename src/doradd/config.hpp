#pragma once

#include <chrono>
#include <stddef.h>
#include <stdint.h>
#include <vector>

static constexpr size_t BATCH_PREFETCHER = 8;
static constexpr size_t BATCH_SPAWNER = 8;
static constexpr size_t MAX_BATCH = 4;
static constexpr uint64_t RPC_LOG_SIZE = 4'000'000;
static constexpr uint64_t TX_COUNTER_LOG_SIZE = 1'000'000;
static constexpr uint64_t ANNOUNCE_THROUGHPUT_BATCH_SIZE = 1'000'000;
static constexpr size_t CHANNEL_SIZE = 2;
static constexpr size_t CHANNEL_SIZE_IDX_PREF = 2;

using ts_type = std::chrono::time_point<std::chrono::system_clock>;

#define RW 1
#define LLC_LOCALITY 1
#define L1D_LOCALITY 3

#ifdef LOG_SCHED_OHEAD
using log_arr_type = std::vector<std::tuple<uint32_t, uint32_t>>;
#else
using log_arr_type = std::vector<uint32_t>;
#endif

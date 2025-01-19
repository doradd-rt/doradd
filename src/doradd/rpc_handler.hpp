#pragma once

#include "../misc/inter_arrival.hpp"

#include <atomic>
#include <cassert>
#include <immintrin.h>
#include <stdio.h>
#include <stdlib.h>

struct RPCHandler
{
  std::atomic<uint64_t>* avail_cnt;
  struct rand_gen* dist; // inter-arrival distribution
  uint64_t log_arr;

  RPCHandler(
    std::atomic<uint64_t>* avail_cnt_, char* gen_type, uint64_t log_arr_)
  : avail_cnt(avail_cnt_), log_arr(log_arr_)
  {
    dist = lancet_init_rand(gen_type);
  }

  void run()
  {
    long next_ts = time_ns();
    int i = 0;

    // spinning and populating cnts
    while (1)
    {
      while (time_ns() < next_ts)
        _mm_pause();

      if (i >= RPC_LOG_SIZE)
        break;
      
      // record arrival timestamp for latency measuring
      auto* addr =
        reinterpret_cast<void*>(log_arr + (uint64_t)(i++ * sizeof(ts_type)));
      *reinterpret_cast<ts_type*>(addr) = std::chrono::system_clock::now();

      avail_cnt->fetch_add(1, std::memory_order_relaxed);
      next_ts += gen_inter_arrival(dist);
    }
  }
};

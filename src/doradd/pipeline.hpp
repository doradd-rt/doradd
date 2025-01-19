#pragma once

#include "config.hpp"
#include "dispatcher.hpp"
#include "pin-thread.hpp"
#include "rpc_handler.hpp"
#include "SPSCQueue.h"
#include "txcounter.hpp"

#include <thread>
#include <unordered_map>

std::unordered_map<std::thread::id, uint64_t*>* counter_map;
std::unordered_map<std::thread::id, log_arr_type*>* log_map;
std::mutex* counter_map_mutex;

template<typename T>
void build_pipelines(int worker_cnt, char* log_name, char* gen_type)
{
  // init verona-rt scheduler
  auto& sched = Scheduler::get();
  sched.init(worker_cnt + 1);
  when() << []() { std::cout << "Hello deterministic world!\n"; };

  // init stats collectors for workers
  counter_map = new std::unordered_map<std::thread::id, uint64_t*>();
  counter_map->reserve(worker_cnt);
  log_map = new std::unordered_map<std::thread::id, log_arr_type*>();
  log_map->reserve(worker_cnt);
  counter_map_mutex = new std::mutex();

  // init and run dispatcher pipelines
  when() << [&]() {
    printf("Init and Run - Dispatcher Pipelines\n");

    std::atomic<uint64_t> req_cnt(0);

    // Init RPC handler
    uint8_t* log_arr = static_cast<uint8_t*>(
      aligned_alloc_hpage(RPC_LOG_SIZE * sizeof(ts_type)));

    uint64_t log_arr_addr = (uint64_t)log_arr;

    RPCHandler rpc_handler(&req_cnt, gen_type, log_arr_addr);

    // Map txn logs into memory
    int fd = open(log_name, O_RDONLY);
    if (fd == -1)
    {
      printf("File not existed\n");
      exit(1);
    }
    struct stat sb;
    fstat(fd, &sb);
    void* ret = reinterpret_cast<char*>(mmap(
      nullptr,
      sb.st_size,
      PROT_READ | PROT_WRITE,
      MAP_PRIVATE | MAP_POPULATE,
      fd,
      0));

    // Init dispatcher, prefetcher, and spawner
#ifndef CORE_PIPE
    FileDispatcher<T> dispatcher(
      ret,
      worker_cnt,
      counter_map,
      counter_map_mutex,
      &req_cnt,
      log_arr_addr
    );

    std::thread extern_thrd([&]() mutable {
      pin_thread(2);
      std::this_thread::sleep_for(std::chrono::seconds(1));
      dispatcher.run();
    });
#else

#  ifdef INDEXER
    rigtorp::SPSCQueue<int> ring_idx_pref(CHANNEL_SIZE_IDX_PREF);
    Indexer<T> indexer(ret, &ring_idx_pref, &req_cnt);
#  endif

    rigtorp::SPSCQueue<int> ring_pref_disp(CHANNEL_SIZE);

#  if defined(INDEXER)
    Prefetcher<T> prefetcher(ret, &ring_pref_disp, &ring_idx_pref);
    // give init_time_log_arr to spawner. Needed for capturing in when.
    Spawner<T> spawner(
      ret,
      worker_cnt,
      counter_map,
      counter_map_mutex,
      &ring_pref_disp,
      log_arr_addr);
#  else
    Prefetcher<T> prefetcher(ret, &ring_pref_disp);
    Spawner<T> spawner(
      ret, worker_cnt, counter_map, counter_map_mutex, &ring_pref_disp);
#  endif // INDEXER

    std::thread spawner_thread([&]() mutable {
      pin_thread(1);
      std::this_thread::sleep_for(std::chrono::seconds(1));
      spawner.run();
    });
    std::thread prefetcher_thread([&]() mutable {
      pin_thread(2);
      std::this_thread::sleep_for(std::chrono::seconds(2));
      prefetcher.run();
    });
#endif

#ifdef INDEXER
    std::thread indexer_thread([&]() mutable {
      pin_thread(3);
      std::this_thread::sleep_for(std::chrono::seconds(4));
      indexer.run();
    });
#endif

    std::thread rpc_handler_thread([&]() mutable {
      pin_thread(0);
      std::this_thread::sleep_for(std::chrono::seconds(6));
      rpc_handler.run();
    });

    rpc_handler_thread.join();
    spawner_thread.join();
    prefetcher_thread.join();
    indexer_thread.join();

    std::cout << "Calculate latency stats" << std::endl;
    log_arr_type all_samples(worker_cnt * TX_COUNTER_LOG_SIZE);
    uint64_t idx = 0;
    for (const auto& entry : *log_map)
    {
      if (entry.second)
      {
        for (auto value : *(entry.second))
          all_samples[idx++] = value;
      }
    }

    double p99 = TxCounter::percentile(all_samples, 99);
    std::cout << "P99 latency: " << p99 << " µs" << std::endl;
  };

  sched.run();
}

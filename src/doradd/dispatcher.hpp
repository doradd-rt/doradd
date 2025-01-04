#pragma once

#include "config.hpp"
#include "hugepage.hpp"
#include "warmup.hpp"
#include "SPSCQueue.h"

#include <cassert>
#include <fcntl.h>
#include <mutex>
#include <numeric>
#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <thread>
#include <unordered_map>
#include <vector>

typedef rigtorp::SPSCQueue<int> InterCore;

template<typename T>
struct DoraddBuf
{
  /* uint64_t pkt_addr; */
  typename T::Marshalled workload;
};

template<typename T>
class alignas(64) BaseDispatcher
{
protected:
  uint32_t BUFFER_SIZE;
  DoraddBuf<T>* req_buf;
  char* curr_req;
  uint64_t processed_cnt; // Local transaction counter

  static constexpr uint64_t TERMINATION_COUNT = 40'000'000;

  BaseDispatcher(void* mmap_ret)
  {
    BUFFER_SIZE = *(reinterpret_cast<uint32_t*>(mmap_ret));
    req_buf = reinterpret_cast<DoraddBuf<T>*>(
      reinterpret_cast<char*>(mmap_ret) + sizeof(uint32_t));
    curr_req = reinterpret_cast<char*>(req_buf);
  }

  char* get_curr_req(int idx)
  {
    return reinterpret_cast<char*>(
      &req_buf[idx % BUFFER_SIZE]);
  }

  bool terminate() const {
    return processed_cnt >= TERMINATION_COUNT;
  }

  virtual void run() = 0;
};

class FirstCoreFunc
{
protected:
  std::atomic<uint64_t>* recvd_req_cnt;
  uint64_t handled_req_cnt;

public:
  FirstCoreFunc(std::atomic<uint64_t>* recvd_req_cnt_):
    recvd_req_cnt(recvd_req_cnt_), handled_req_cnt{0} {}

  size_t get_avail_req_cnts()
  {
    uint64_t avail_cnt;
    size_t dyn_batch;

    do
    {
      uint64_t load_val =
        recvd_req_cnt->load(std::memory_order_relaxed);
      avail_cnt = load_val - handled_req_cnt;
      if (avail_cnt >= MAX_BATCH)
        dyn_batch = MAX_BATCH;
      else if (avail_cnt > 0)
        dyn_batch = static_cast<size_t>(avail_cnt);
      else
      {
        _mm_pause();
        continue;
      }
    } while (avail_cnt == 0);

    handled_req_cnt += dyn_batch;

    return dyn_batch;
  }
};

class LastCoreFunc
{
protected:
  bool counter_registered;
  uint64_t tx_count;
  uint8_t worker_cnt;
  std::unordered_map<std::thread::id, uint64_t*>* counter_map;
  ts_type last_print_ts;
  uint64_t tx_exec_sum;
  uint64_t last_tx_exec_sum;
  uint64_t tx_spawn_sum;
#ifdef RPC_LATENCY
  FILE* res_log_fd;
#endif

  void track_worker_counter()
  {
    if (counter_map->size() == worker_cnt)
      counter_registered = true;
  }

  uint64_t calc_tx_exec_sum()
  {
    uint64_t sum = 0;
    for (const auto& counter_pair : *counter_map)
      sum += *(counter_pair.second);

    return sum;
  }

public:
  LastCoreFunc(
    std::unordered_map<std::thread::id, uint64_t*>* counter_map_, uint8_t worker_cnt_
#ifdef RPC_LATENCY
    , FILE* res_log_fd_
#endif
  ):
    worker_cnt(worker_cnt_),
#ifdef RPC_LATENCY
    res_log_fd(res_log_fd_),
#endif
    counter_map(counter_map_),
    tx_count{0},
    counter_registered{false},
    tx_exec_sum{0},
    last_tx_exec_sum{0},
    last_print_ts{std::chrono::system_clock::now()} {}

  void measure_throughput(size_t batch)
  {
    tx_count += batch;

    if (!counter_registered) {
      track_worker_counter();
      return;
    }

    // Announce throughput
    if (tx_count >= ANNOUNCE_THROUGHPUT_BATCH_SIZE)
    {
      auto time_now = std::chrono::system_clock::now();
      std::chrono::duration<double> duration = time_now - last_print_ts;
      auto dur_cnt = duration.count();

      tx_exec_sum = calc_tx_exec_sum();

      printf("spawn - %lf tx/s\n", tx_count / dur_cnt);
      printf(
        "exec  - %lf tx/s\n", (tx_exec_sum - last_tx_exec_sum) / dur_cnt);
#ifdef RPC_LATENCY
      fprintf(res_log_fd, "%lf\n", tx_count / dur_cnt);
#endif
      tx_count = 0;
      last_tx_exec_sum = tx_exec_sum;
      last_print_ts = time_now;
    }
  }
};

template<typename T>
class alignas(64) Indexer : BaseDispatcher<T>, FirstCoreFunc
{
public:
  InterCore* outQueue;

  Indexer(void* global_buf_, InterCore* queue_, std::atomic<uint64_t>* req_cnt_):
    BaseDispatcher<T>(global_buf_), FirstCoreFunc(req_cnt_), outQueue(queue_) {}

  void run()
  {
    int idx{0}, batch{0};

    while(!this->terminate())
    {
      batch = this->get_avail_req_cnts();

      for (int i = 0; i < batch; i++)
      {
        this->curr_req = this->get_curr_req(idx++);
        T::prepare_cowns(this->curr_req);
        this->processed_cnt++;
      }

      outQueue->push(batch);
    }
  }
};

template<typename T>
class alignas(64) Prefetcher: BaseDispatcher<T>
{
public:
  InterCore* inQueue;
  InterCore* outQueue;

  Prefetcher(void* global_buf_, InterCore* outQueue_,  InterCore* inQueue_):
    BaseDispatcher<T>(global_buf_), inQueue(inQueue_), outQueue(outQueue_) {}

  void run()
  {
    int idx{0}, batch{0};

    while(!this->terminate())
    {
      if (!inQueue->front())
        continue;

      batch = static_cast<int>(*inQueue->front());

      // Prefetch into Spawner's LLC
      for (int i = 0; i < batch; i++)
      {
        this->curr_req = this->get_curr_req(idx++);
        T::prefetch_cowns(this->curr_req);
        this->processed_cnt++;
      }

      outQueue->push(batch);
      inQueue->pop();
    }
  }
};

template<typename T>
class alignas(64) Spawner : BaseDispatcher<T>, LastCoreFunc
{
public:
  InterCore* inQueue;
  // TODO: fix log arr round
#ifdef RPC_LATENCY
  uint64_t txn_log_id= 0;
  uint64_t init_time_log_arr;
  ts_type init_time;
#endif

  Spawner(void* global_buf_, uint8_t worker_cnt_,
    std::unordered_map<std::thread::id, uint64_t*>* counter_map_,
    std::mutex* counter_map_mutex_, InterCore* queue_
#ifdef RPC_LATENCY
    , uint64_t init_time_log_arr_, FILE* res_log_fd_
#endif
    ):
    BaseDispatcher<T>(global_buf_),
    LastCoreFunc(counter_map_, worker_cnt_
#ifdef RPC_LATENCY
    ,res_log_fd_
#endif
    ),
    inQueue(queue_)
#ifdef RPC_LATENCY
    , init_time_log_arr(init_time_log_arr_)
#endif
  {}

  int dispatch_one()
  {
#ifdef RPC_LATENCY
    init_time = *reinterpret_cast<ts_type*>(
      init_time_log_arr + (uint64_t)sizeof(ts_type) * txn_log_id);

    txn_log_id++;
    return T::parse_and_process(this->curr_req, init_time);
#else
    return T::parse_and_process(this->curr_req);
#endif
  }

  void run()
  {
    int idx{0}, batch{0}, pref_idx{0};
    char* pref_curr_req = this->curr_req;

    while(!this->terminate())
    {
      if (!inQueue->front())
        continue;

      batch = static_cast<int>(*inQueue->front());

      // Prefetch into Spawner's L1 from LLC
      for (int i = 0; i < batch; i++)
      {
        pref_curr_req = this->get_curr_req(pref_idx++);
        T::prefetch_cowns(pref_curr_req);
      }

      for (int i = 0; i < batch; i++)
      {
        this->curr_req = this->get_curr_req(idx++);
        dispatch_one();
        this->processed_cnt++;
      }

      inQueue->pop();

      this->measure_throughput(batch);
    }
  }
};

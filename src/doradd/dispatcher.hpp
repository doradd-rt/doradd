#pragma once

#include "SPSCQueue.h"
#include "config.hpp"
#include "hugepage.hpp"
#include "warmup.hpp"

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
  DoraddBuf<T>* req_buf;
  char* curr_req;
  uint64_t processed_cnt = 0; // Local transaction counter
  uint32_t BUFFER_SIZE;

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
    return reinterpret_cast<char*>(&req_buf[idx % BUFFER_SIZE]);
  }

  bool terminate() const
  {
    return processed_cnt >= TERMINATION_COUNT;
  }

  virtual void run() = 0;
};

class FirstCoreFunc
{
protected:
  std::atomic<uint64_t>* recvd_req_cnt;
  uint64_t handled_req_cnt = 0;

public:
  FirstCoreFunc(std::atomic<uint64_t>* recvd_req_cnt_)
  : recvd_req_cnt(recvd_req_cnt_)
  {}

  size_t get_avail_req_cnts()
  {
    size_t dyn_batch = 0;

    while (dyn_batch == 0)
    {
      uint64_t avail_cnt =
        recvd_req_cnt->load(std::memory_order_relaxed) - handled_req_cnt;

      if (avail_cnt > 0)
      {
        dyn_batch =
          (avail_cnt >= MAX_BATCH) ? MAX_BATCH : static_cast<size_t>(avail_cnt);
      }
      else
      {
        _mm_pause();
      }
    }

    handled_req_cnt += dyn_batch;
    return dyn_batch;
  }
};

class LastCoreFunc
{
protected:
  bool counter_registered = false;
  uint64_t tx_count = 0;
  uint8_t worker_cnt;
  std::unordered_map<std::thread::id, uint64_t*>* counter_map;
  ts_type last_print_ts = {};
  uint64_t last_tx_exec_sum = 0;

  void track_worker_counter()
  {
    if (counter_map->size() == worker_cnt)
      counter_registered = true;
  }

  uint64_t calc_tx_exec_sum() const
  {
    uint64_t sum = 0;
    for (const auto& counter_pair : *counter_map)
      sum += *(counter_pair.second);
    return sum;
  }

public:
  LastCoreFunc(
    std::unordered_map<std::thread::id, uint64_t*>* counter_map_,
    uint8_t worker_cnt_)
  : worker_cnt(worker_cnt_),
    counter_map(counter_map_)
  {}

  void measure_throughput(size_t batch)
  {
    tx_count += batch;

    if (!counter_registered)
    {
      track_worker_counter();
      return;
    }

    if (tx_count < ANNOUNCE_THROUGHPUT_BATCH_SIZE)
      return;

    auto now = std::chrono::system_clock::now();

    // Initialize last_print_ts on the first measurement
    if (last_print_ts.time_since_epoch().count() == 0) {
      last_print_ts = now;
      tx_count = 0;
      last_tx_exec_sum = calc_tx_exec_sum();
      return;
    }

    double duration =
      std::chrono::duration<double>(now - last_print_ts).count();

    uint64_t current_exec_sum = calc_tx_exec_sum();
    double spawn_rate = tx_count / duration;
    double exec_rate = (current_exec_sum - last_tx_exec_sum) / duration;

    std::cout << "Spawn throughput: " << spawn_rate << " rps" << std::endl;
    std::cout << "Exec throughput: " << exec_rate << " rps" << std::endl;

    tx_count = 0;
    last_tx_exec_sum = current_exec_sum;
    last_print_ts = now;
  }
};

template<typename T>
class alignas(64) Indexer : public BaseDispatcher<T>, public FirstCoreFunc
{
public:
  InterCore* outQueue;

  Indexer(void* global_buf_, InterCore* queue_, std::atomic<uint64_t>* req_cnt_)
  : BaseDispatcher<T>(global_buf_), FirstCoreFunc(req_cnt_), outQueue(queue_)
  {}

  void run() override
  {
    int idx = 0;

    while (!this->terminate())
    {
      size_t batch = this->get_avail_req_cnts();

      for (size_t i = 0; i < batch; ++i)
      {
        this->curr_req = this->get_curr_req(idx++);
        T::prepare_cowns(this->curr_req);
        this->processed_cnt++;
      }

      outQueue->push(static_cast<int>(batch));
    }
  }
};

template<typename T>
class alignas(64) Prefetcher : public BaseDispatcher<T>
{
public:
  InterCore* inQueue;
  InterCore* outQueue;

  Prefetcher(void* global_buf_, InterCore* outQueue_, InterCore* inQueue_)
  : BaseDispatcher<T>(global_buf_), inQueue(inQueue_), outQueue(outQueue_)
  {}

  void run() override
  {
    int idx = 0;

    while (!this->terminate())
    {
      if (!inQueue->front())
        continue;

      int batch = static_cast<int>(*inQueue->front());

      for (int i = 0; i < batch; ++i)
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
class alignas(64) Spawner : public BaseDispatcher<T>, public LastCoreFunc
{
public:
  InterCore* inQueue;
  uint64_t txn_log_id = 0;
  uint64_t init_time_log_arr;

  Spawner(
    void* global_buf_,
    uint8_t worker_cnt_,
    std::unordered_map<std::thread::id, uint64_t*>* counter_map_,
    std::mutex* counter_map_mutex_,
    InterCore* queue_,
    uint64_t init_time_log_arr_)
  : BaseDispatcher<T>(global_buf_),
    LastCoreFunc(
      counter_map_,
      worker_cnt_),
    inQueue(queue_),
    init_time_log_arr(init_time_log_arr_)
  {}

  int dispatch_one()
  {
    ts_type init_time = *reinterpret_cast<ts_type*>(
      init_time_log_arr + 
      static_cast<uint64_t>(sizeof(ts_type)) * txn_log_id++);

    return T::parse_and_process(this->curr_req, init_time);
  }

  void run() override
  {
    int idx = 0;
    int pref_idx = 0;
    char* pref_curr_req = this->curr_req;

    while (!this->terminate())
    {
      if (!inQueue->front())
        continue;

      int batch = static_cast<int>(*inQueue->front());

      for (int i = 0; i < batch; ++i)
      {
        pref_curr_req = this->get_curr_req(pref_idx++);
        T::prefetch_cowns(pref_curr_req);
      }

      for (int i = 0; i < batch; ++i)
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

#pragma once

#include <thread>
#include <unordered_map>
#include <mutex>
#include <tuple>

#include "config.hpp"

extern std::unordered_map<std::thread::id, uint64_t*>* counter_map;
extern std::unordered_map<std::thread::id, log_arr_type*>* log_map;
extern std::mutex* counter_map_mutex;

/* Thread-local singleton TxCounter */
struct TxCounter {
  static TxCounter& instance() {
    static thread_local TxCounter instance;
    return instance;
  }

  void incr() { tx_cnt++; }

#ifdef LOG_LATENCY
  #ifdef LOG_SCHED_OHEAD
  void log_latency(uint32_t exec_time, uint32_t txn_time) {
    log_arr->push_back({exec_time, txn_time});
  }
  #else
  void log_latency(uint32_t exec_time) {
    log_arr->push_back(exec_time);  
  }
  #endif
#endif

private:
  uint64_t tx_cnt; 
#ifdef LOG_LATENCY
  log_arr_type* log_arr; 
#endif
  
  TxCounter()  {
    tx_cnt = 0;
    std::lock_guard<std::mutex> lock(*counter_map_mutex);
    (*counter_map)[std::this_thread::get_id()] = &tx_cnt;
#ifdef LOG_LATENCY
    log_arr = new log_arr_type();
    log_arr->reserve(TX_COUNTER_LOG_SIZE);
    (*log_map)[std::this_thread::get_id()] = log_arr;
#endif
  }

  ~TxCounter() noexcept { 
    std::lock_guard<std::mutex> lock(*counter_map_mutex);
    counter_map->erase(std::this_thread::get_id());
#ifdef LOG_LATENCY
    log_map->erase(std::this_thread::get_id());
#endif
  }

  // Deleted to ensure singleton pattern
  TxCounter(const TxCounter&) = delete;
  TxCounter& operator=(const TxCounter&) = delete;
  TxCounter(TxCounter&&) = delete;
  TxCounter& operator=(TxCounter&&) = delete;
};

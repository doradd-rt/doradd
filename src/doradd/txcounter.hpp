#pragma once

#include "config.hpp"

#include <mutex>
#include <thread>
#include <unordered_map>

extern std::unordered_map<std::thread::id, uint64_t*>* counter_map;
extern std::unordered_map<std::thread::id, log_arr_type*>* log_map;
extern std::mutex* counter_map_mutex;
const int SAMPLE_RATE = 10;

/* Thread-local singleton TxCounter */
struct TxCounter
{
  static TxCounter& instance()
  {
    static thread_local TxCounter instance;
    return instance;
  }

  void incr()
  {
    tx_cnt++;
  }

  void log_latency(ts_type init_time)
  {
    if (tx_cnt % SAMPLE_RATE == 0)
    {
      auto time_now = std::chrono::system_clock::now();
      std::chrono::duration<double> duration = time_now - init_time;
      uint32_t log_duration =
        static_cast<uint32_t>(duration.count() * 1'000'000);

      log_arr->push_back(log_duration);
    }
  }

  // Function to compute the percentile of a vector
  static double percentile(const log_arr_type &vec,
                           double percentile) {
    // Make sure the vector is not empty
    if (vec.empty()) {
      return 0.0;
    }

    // Sort the vector
    log_arr_type sortedVec = vec;
    std::sort(sortedVec.begin(), sortedVec.end());

    // Calculate the index corresponding to the percentile
    double index = (percentile / 100.0) * (sortedVec.size() - 1);

    // If index is an integer
    if (index == std::floor(index)) {
      return sortedVec[static_cast<int>(index)];
    }

    // If index is a fractional value, interpolate
    int lowerIndex = std::floor(index);
    int upperIndex = std::ceil(index);
    double lowerValue = sortedVec[lowerIndex];
    double upperValue = sortedVec[upperIndex];
    return lowerValue + (index - lowerIndex) * (upperValue - lowerValue);
  }

private:
  uint64_t tx_cnt;
  log_arr_type* log_arr;

  TxCounter()
  {
    tx_cnt = 0;
    std::lock_guard<std::mutex> lock(*counter_map_mutex);
    (*counter_map)[std::this_thread::get_id()] = &tx_cnt;
    log_arr = new log_arr_type();
    log_arr->reserve(TX_COUNTER_LOG_SIZE);
    (*log_map)[std::this_thread::get_id()] = log_arr;
  }

  ~TxCounter() noexcept
  {
    std::lock_guard<std::mutex> lock(*counter_map_mutex);
    counter_map->erase(std::this_thread::get_id());
    log_map->erase(std::this_thread::get_id());
  }

  // Deleted to ensure singleton pattern
  TxCounter(const TxCounter&) = delete;
  TxCounter& operator=(const TxCounter&) = delete;
  TxCounter(TxCounter&&) = delete;
  TxCounter& operator=(TxCounter&&) = delete;
};

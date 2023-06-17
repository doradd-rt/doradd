#pragma once

#include <chrono>
#include <assert.h>

struct TxExecCounter
{
  uint64_t tx_cnt; 
  std::chrono::time_point<std::chrono::system_clock> last_print;

  void count_tx()
  {
    // Report every second
    std::chrono::milliseconds interval(1000);
    tx_cnt++;
    auto time_now = std::chrono::system_clock::now();
    if ((time_now - last_print) > interval) 
    {
      std::chrono::duration<double> duration = time_now - last_print;
      printf("%lf tx/s\n", tx_cnt / duration.count());
      tx_cnt = 0;
      last_print = time_now;
    }
  }
};

struct TxPendingCounter
{
  uint64_t tx_cnt;
  uint64_t threshold;

  TxPendingCounter(uint64_t threshold_) : threshold(threshold_) {}

  bool incr_pending(uint64_t cnt)
  {
    tx_cnt += cnt;
    //printf("incr to %lu\n", pending_tx_cnt);
    if (tx_cnt < threshold) 
      return true;  

    return false; 
  }

  void decr_pending()
  {
    //printf("decr to %lu\n", pending_tx_cnt);
    assert(1 <= tx_cnt);
    tx_cnt--;
  }
};

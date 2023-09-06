#pragma once

#include <stdio.h>
#include <thread>

void pin_thread(int cpu) {
  if (cpu < 0) 
    return;      
  
  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  CPU_SET(cpu, &cpuset);
  if (pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset) == -1) {
    perror("pthread_setaffinity_no");
    exit(1);
  }        
}

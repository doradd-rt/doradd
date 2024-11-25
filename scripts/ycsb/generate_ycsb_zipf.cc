#include "zipfian_random.h"

#include <algorithm>
#include <array>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <iterator>
#include <random>
#include <vector>

#define ROW_COUNT 10'000'000
#define TX_COUNT 1'000'000 // 4M for Caracal
#define ROW_PER_TX 10
#define NrMSBContentionKey 6

using Rand = foedus::assorted::ZipfianRandom;

// Contention: 7 keys of 10 are chosen from 77 (2^7) set of 10M (2^24)
// nr_lsb is 17 (24-7) to mask 7 keys into a fixed space.
std::array<uint32_t, ROW_PER_TX> gen_keys(Rand* r, int contention)
{
  int nr_lsb = 63 - __builtin_clzll(ROW_COUNT) - NrMSBContentionKey;
  size_t mask = 0;
  if (nr_lsb > 0)
    mask = (1 << nr_lsb) - 1;

  int NrContKey = 0;
  if (contention)
    NrContKey = 7;

  std::array<uint32_t, ROW_PER_TX> keys;
  for (int i = 0; i < ROW_PER_TX; i++)
  {
  again:
    keys[i] = r->next() % ROW_COUNT;
    // if branch below skip the idx-0, i.e., the most contended key
    // since "& mask" will opt it out, thus keys are [1, 10M-1]
    // To stay consistent w/ Caracal, we keep these here.
    if (i < NrContKey)
    {
      keys[i] &= ~mask;
    }
    else
    {
      if ((keys[i] & mask) == 0)
        goto again;
    }
    for (int j = 0; j < i; j++)
      if (keys[i] == keys[j])
        goto again;
  }

  return keys;
}

uint16_t gen_write_set(int contention)
{
  // Set 0:10 read-write ratio for contended workloads
  if (contention)
    return static_cast<uint16_t>((1 << 10) - 1);

  std::vector<uint8_t> binDigits = {0, 0, 0, 0, 0, 0, 0, 0, 1, 1};
  uint16_t result = 0;
  std::random_device rd;
  std::mt19937 g(rd());
  std::shuffle(binDigits.begin(), binDigits.end(), g);

  for (uint8_t digit : binDigits)
    result = (result << 1) | digit;

  return result;
}

void gen_bin_txn(Rand* rand, std::ofstream* f, int contention)
{
  auto keys = gen_keys(rand, contention);
  auto ws = gen_write_set(contention);
  int padding = 8 * ROW_PER_TX + 6;

  // pack
  for (const uint32_t& key : keys)
  {
    for (size_t i = 0; i < sizeof(uint32_t); i++)
    {
      uint8_t byte = (key >> (i * 8)) & 0xFF;
      f->write(reinterpret_cast<const char*>(&byte), sizeof(uint8_t));
    }
  }

  f->write(reinterpret_cast<const char*>(&ws), sizeof(uint16_t));

  for (int i = 0; i < padding; i++)
  {
    uint8_t zeroByte = 0x00;
    f->write(reinterpret_cast<const char*>(&zeroByte), sizeof(uint8_t));
  }
}

int main(int argc, char** argv)
{
  if (argc != 5 || strcmp(argv[1], "-d") != 0 || strcmp(argv[3], "-c") != 0)
  {
    fprintf(stderr, "Usage: ./program -d distribution -c contention\n");
    return -1;
  }

  double zipf_s = 0;
  if (!strcmp(argv[2], "zipfian"))
  {
    zipf_s = 0.9;
    printf("generating w/ zipfian distribution\n");
  }
  else if (strcmp(argv[2], "uniform"))
    fprintf(stderr, "distribution should be uniform or zipfian\n");

  int contention = 0;
  if (!strcmp(argv[4], "cont"))
  {
    contention = 1;
    printf("generating w/ contended accesses\n");
  }
  else
  {
    printf("generating w/ No contended accesses\n");
  }

  Rand rand;
  rand.init(ROW_COUNT, zipf_s, 1238);

  char log_name[50];
  snprintf(log_name, sizeof(log_name), "ycsb_%s_%s.txt", argv[2], argv[4]);
  
  std::ofstream outLog(log_name, std::ios::binary);
  uint32_t count = TX_COUNT;
  outLog.write(reinterpret_cast<const char*>(&count), sizeof(uint32_t));

  for (int i = 0; i < TX_COUNT; i++)
  {
    gen_bin_txn(&rand, &outLog, contention);
  }

  outLog.close();
  return 0;
}

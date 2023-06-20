#include <deterdb.hpp>
#include <dispatcher.hpp>
#include <txcounter.hpp>
#include <perf.hpp>
#include <thread>
#include <unordered_map>

constexpr uint32_t ROWS_PER_TX = 10;
constexpr uint32_t ROW_SIZE = 1000;
constexpr uint32_t WRITE_SIZE = 100;
const uint64_t ROW_COUNT = 1000000;
const uint64_t PENDING_THRESHOLD = 200000;

struct YCSBRow
{
  char payload[ROW_SIZE];
};

struct __attribute__((packed)) YCSBTransactionMarshalled
{
  uint64_t indices[ROWS_PER_TX];
  uint16_t write_set;
  uint8_t  pad[46];
};
static_assert(sizeof(YCSBTransactionMarshalled) == 128);

struct YCSBTransaction
{
public:
  cown_ptr<Row<YCSBRow>> rows[ROWS_PER_TX];
  // Bit field. Assume less than 16 concurrent rows per transaction
  uint16_t write_set;
  uint16_t dispatcher_set;
  uint32_t row_count;
  static std::shared_ptr<Index<YCSBRow>> index;
  static cown_ptr<TxExecCounter> tx_exec_counter;
  //thread_local static uint64_t tx_cnt;
  //static TxCounter tx_counter;

  static int parse(const char* input, YCSBTransaction& tx)
  {
    const YCSBTransactionMarshalled* txm =
      reinterpret_cast<const YCSBTransactionMarshalled*>(input);

    tx.write_set = txm->write_set;
    tx.row_count = ROWS_PER_TX;
    tx.dispatcher_set = 0;

    for (int i = 0; i < ROWS_PER_TX; i++) 
      tx.rows[i] = index->get_row(txm->indices[i]);

    return sizeof(YCSBTransactionMarshalled);
  }

  void process() const
  {
    
    using type1 = acquired_cown<Row<YCSBRow>>;
#if 0
    when(tx.rows[0],tx.rows[1],tx.rows[2],tx.rows[3],tx.rows[4],tx.rows[5],tx.rows[6],tx.rows[7],tx.rows[8],tx.rows[9]) << [=]
      (type1 acq_row0, type1 acq_row1, type1 acq_row2, type1 acq_row3,type1 acq_row4,type1 acq_row5,type1 acq_row6,type1 acq_row7,type1 acq_row8,type1 acq_row9)
#endif
    when (rows[0],rows[1]) << 
    [=](type1 acq_row0, type1 acq_row1)  
    {
      uint8_t sum = 0;
      uint16_t write_set_l = write_set;
      int j;

      if (write_set_l & 0x1)
      {
        memset(&acq_row0->val, sum, WRITE_SIZE);
      }
      else
      {
        for (j = 0; j < ROW_SIZE; j++)
          sum += acq_row0->val.payload[j];
      }
      write_set_l >>= 1;

      if (write_set_l & 0x1)
      {
        memset(&acq_row1->val, sum, WRITE_SIZE);
      }
      else
      {
        for (j = 0; j < ROW_SIZE; j++)
          sum += acq_row1->val.payload[j];
      }
      write_set_l >>= 1;
     
      //tx_cnt++;
      TxCounter::instance().incr();
     
      // FIXME: Contention? thread-local perf
      // perf accounting: throughput
#if 0
      when(tx_exec_counter) <<[](acquired_cown<TxExecCounter> acq_tx_exec_counter)
      {
        acq_tx_exec_counter->count_tx();
      };
#endif
    };
  }
};

std::shared_ptr<Index<YCSBRow>> YCSBTransaction::index;
cown_ptr<TxExecCounter> YCSBTransaction::tx_exec_counter;
std::unordered_map<std::thread::id, uint64_t*>* counter_map;

int main(int argc, char** argv)
{
  auto& sched = Scheduler::get();
  //Scheduler::set_detect_leaks(true);
  //sched.set_fair(true);
  sched.init(8);

  if (argc != 2)
  {
    fprintf(stderr, "Usage ./ycsb <dispatcher_input_file>\n");
    return -1;
  }

  when() << []() { std::cout << "Hello deterministic world!\n"; };

  // Create rows and populate index
  YCSBTransaction::index = std::make_shared<Index<YCSBRow>>();

  for (int i = 0; i < ROW_COUNT; i++)
  {
    cown_ptr<Row<YCSBRow>> cown_r = make_cown<Row<YCSBRow>>();
    YCSBTransaction::index->insert_row(cown_r);
  }
 
  counter_map = new std::unordered_map<std::thread::id, uint64_t*>();
  
  YCSBTransaction::tx_exec_counter = make_cown<TxExecCounter>();

  auto dispatcher_cown = make_cown<FileDispatcher<YCSBTransaction>>(argv[1], 1000, counter_map);
  when(dispatcher_cown) << [=]
    (acquired_cown<FileDispatcher<YCSBTransaction>> acq_dispatcher) 
    { acq_dispatcher->run(); };

  sched.run();
}

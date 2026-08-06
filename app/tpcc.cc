#include "pipeline.hpp"
#include "tpcc/db.hpp"
#include "tpcc/generator.hpp"
#include "txcounter.hpp"

#include <cpp/when.h>
#include <thread>
#include <unordered_map>

using namespace verona::rt;
using namespace verona::cpp;

#define UPDATE_STOCK(_INDEX) \
  { \
    _s##_INDEX->s_quantity -= txm->params[35 + (_INDEX - 1)]; \
    if (_s##_INDEX->s_quantity < 10) \
      _s##_INDEX->s_quantity += 91; \
    _s##_INDEX->s_ytd += txm->params[35 + (_INDEX - 1)]; \
    _s##_INDEX->s_order_cnt += 1; \
    if (txm->params[51] == 1) \
      _s##_INDEX->s_remote_cnt += 1; \
  }

#define INSERT_ORDER_LINE(_INDEX) \
  { \
    OrderLine _ol##_INDEX = OrderLine(txm->params[0], txm->params[1], order_hash_key, _INDEX); \
    _ol##_INDEX.ol_i_id = txm->params[5 + (_INDEX - 1)]; \
    _ol##_INDEX.ol_supply_w_id = txm->params[20 + (_INDEX - 1)]; \
    _ol##_INDEX.ol_quantity = txm->params[35 + (_INDEX - 1)]; \
    _ol##_INDEX.ol_amount = _i##_INDEX->i_price * txm->params[35 + (_INDEX - 1)]; \
    amount += _ol##_INDEX.ol_amount; \
    uint64_t _ol_hash_key##_INDEX = _ol##_INDEX.hash_key(); \
    cown_ptr<OrderLine> _ol_cown##_INDEX = make_cown<OrderLine>(_ol##_INDEX); \
    index->order_line_table.insert_row(_ol_hash_key##_INDEX, _ol_cown##_INDEX); \
  }

#define UPDATE_STOCK_AND_INSERT_ORDER_LINE(_INDEX) \
  { \
    UPDATE_STOCK(_INDEX); \
    INSERT_ORDER_LINE(_INDEX); \
  }


// Macros
__GENERATE_STOCK_MACROS(15)
__GENERATE_ITEM_MACROS(15)
__GENERATE_UPDATE_STOCK_OL_MACROS(15)


#ifdef WAREHOUSE_SPLIT
  #define WHEN(...) when(w) <<[=] (auto _w) { _w->w_ytd += txm->params[52]; }; when(__VA_ARGS__)
  #define WAREHOUSE_OP() {}
  #define PARAMS(...) (__VA_ARGS__)
#else
  #define WHEN(...) when(w, __VA_ARGS__)
  #define WAREHOUSE_OP() \
  { \
    _w->w_ytd += txm->params[52]; \
  }
  #define PARAMS(...) (auto _w, __VA_ARGS__)
#endif

// Params
// 0 w_id
// 1 d_id
// 2 c_id
// 3 ol_cnt
// 4 o_entry_d
// 5 i_ids[15]
// 20 i_w_ids[15]
// 35 i_qtys[15]
// 50 number_of_items
// 51 number_of_rows
// 52 h_amount
// 53 c_w_id
// 54 c_d_id
// 55 c_last
// 56 h_date
// rest: unused
struct TPCCTransactionMarshalled
{
  uint8_t txn_type; // 0 for NewOrder, 1 for Payment
  uint32_t params[65];
  uint64_t cown_ptrs[33];
  uint8_t padding[51];
} __attribute__((packed));
// new - 64*9 = 576

struct TPCCTransaction
{
public:
  using Marshalled = TPCCTransactionMarshalled;

  // requests per run; bounded by order table capacity (MAX_ORDER_TRANSACTIONS)
  static constexpr uint64_t RPC_LOG_SIZE = 1'000'000;

  // Bit field. Assume less than 16 concurrent rows per transaction
  // uint16_t write_set;
  // static Index<YCSBRow>* index;
  static Database* index;

#if defined(INDEXER) || defined(TEST_TWO)
  // Indexer: read db and in-place update cown_ptr
  static int prepare_cowns(char* input)
  {
    auto txm = reinterpret_cast<TPCCTransactionMarshalled*>(input);

    // printf("params are %u, %u, %u\n", txm->params[0], txm->params[1],
    // txm->params[2]);
    //  New Order
    if (txm->txn_type == 0)
    {
      // Warehouse
      // txm->cown_ptrs[0] =
      //   index->warehouse_table.get_row_addr(txm->params[0])->get_base_addr();
      txm->cown_ptrs[0] = index->warehouse_table.get_row_addr(Warehouse::hash_key(txm->params[0]))->get_base_addr();

      // // District
      txm->cown_ptrs[1] =
        index->district_table.get_row_addr(District::hash_key(txm->params[0], txm->params[1]))->get_base_addr();

      // // Customer
      txm->cown_ptrs[2] =
        index->customer_table.get_row_addr(Customer::hash_key(txm->params[0], txm->params[1], txm->params[2]))
          ->get_base_addr();

      // Stock
      for (int i = 0; i < txm->params[50]; i++)
      {
        txm->cown_ptrs[3 + i] =
          index->stock_table
            .get_row_addr(Stock::hash_key(txm->params[20 + i], txm->params[5 + i])) // i_w_id, i_id
            ->get_base_addr();
      }

      // Item
      for (int i = 0; i < txm->params[50]; i++)
      {
        txm->cown_ptrs[18 + i] = index->item_table.get_row_addr(Item::hash_key(txm->params[5 + i]))->get_base_addr();
      }
    }
    else if (txm->txn_type == 1)
    {
      // Warehouse
      txm->cown_ptrs[0] = index->warehouse_table.get_row_addr(Warehouse::hash_key(txm->params[0]))->get_base_addr();

      // District
      txm->cown_ptrs[1] =
        index->district_table.get_row_addr(District::hash_key(txm->params[0], txm->params[1]))->get_base_addr();

      // Customer
      txm->cown_ptrs[2] =
        index->customer_table.get_row_addr(Customer::hash_key(txm->params[0], txm->params[1], txm->params[2]))
          ->get_base_addr();
    }

    return sizeof(TPCCTransactionMarshalled);
  }

  static int prefetch_cowns(const char* input)
  {
    auto txm = reinterpret_cast<const TPCCTransactionMarshalled*>(input);

    for (int i = 0; i < 33; i++)
      __builtin_prefetch(reinterpret_cast<const void*>(txm->cown_ptrs[i]), 1, 3);

    return sizeof(TPCCTransactionMarshalled);
  }
#endif // INDEXER

#define NEWORDER_END() \
  { \
    cown_ptr<Order> o_cown = make_cown<Order>(o); \
    cown_ptr<NewOrder> no_cown = make_cown<NewOrder>(no); \
    index->order_table.insert_row(order_hash_key, std::move(o_cown)); \
    index->new_order_table.insert_row(neworder_hash_key, std::move(no_cown)); \
    TxCounter::instance().incr(); \
    TxCounter::instance().log_latency(init_time); \
  }

  static int parse_and_process(const char* input, ts_type init_time)
  {
    const TPCCTransactionMarshalled* txm = reinterpret_cast<const TPCCTransactionMarshalled*>(input);

    // New Order
    if (txm->txn_type == 0)
    {
      // Warehouse
      cown_ptr<Warehouse> w = get_cown_ptr_from_addr<Warehouse>(reinterpret_cast<void*>(txm->cown_ptrs[0]));
      cown_ptr<District> d = get_cown_ptr_from_addr<District>(reinterpret_cast<void*>(txm->cown_ptrs[1]));
      cown_ptr<Customer> c = get_cown_ptr_from_addr<Customer>(reinterpret_cast<void*>(txm->cown_ptrs[2]));
      uint32_t item_number = txm->params[50];

      switch (item_number)
      {
        case 1:   __NEW_ORDER_CASE(1)
        case 2:   __NEW_ORDER_CASE(2) 
        case 3:   __NEW_ORDER_CASE(3) 
        case 4:   __NEW_ORDER_CASE(4) 
        case 5:   __NEW_ORDER_CASE(5) 
        case 6:   __NEW_ORDER_CASE(6) 
        case 7:   __NEW_ORDER_CASE(7) 
        case 8:   __NEW_ORDER_CASE(8) 
        case 9:   __NEW_ORDER_CASE(9) 
        case 10:  __NEW_ORDER_CASE(10)
        case 11:  __NEW_ORDER_CASE(11)
        case 12:  __NEW_ORDER_CASE(12)
        case 13:  __NEW_ORDER_CASE(13)
        case 14:  __NEW_ORDER_CASE(14)
        case 15:  __NEW_ORDER_CASE(15)
                
        default:
        {
          break;
        }
      }
    }

    else if (txm->txn_type == 1)
    {
      cown_ptr<Warehouse> w = get_cown_ptr_from_addr<Warehouse>(reinterpret_cast<void*>(txm->cown_ptrs[0]));
      cown_ptr<District> d = get_cown_ptr_from_addr<District>(reinterpret_cast<void*>(txm->cown_ptrs[1]));
      cown_ptr<Customer> c = get_cown_ptr_from_addr<Customer>(reinterpret_cast<void*>(txm->cown_ptrs[2]));
      uint32_t h_amount = txm->params[52];

      WHEN(d, c) << [=]PARAMS(auto _d, auto _c) {
        WAREHOUSE_OP();
        // Update district balance
        _d->d_ytd += h_amount;

        // Update customer balance (case 1)
        _c->c_balance -= h_amount;
        _c->c_ytd_payment += h_amount;
        _c->c_payment_cnt += 1;

        TxCounter::instance().log_latency(init_time);
        TxCounter::instance().incr();
      };
    }

    return sizeof(TPCCTransactionMarshalled);
  }

  TPCCTransaction(const TPCCTransaction&) = delete;
  TPCCTransaction& operator=(const TPCCTransaction&) = delete;
};

Database* TPCCTransaction::index;

int main(int argc, char** argv)
{
  if (argc != 6 || strcmp(argv[1], "-n") != 0)
  {
    fprintf(stderr,
            "Usage: ./program -n core_cnt"
            " <dispatcher_input_file> -i <inter_arrival>\n");
    return -1;
  }

  uint8_t core_cnt = atoi(argv[2]);
  uint8_t max_core = std::thread::hardware_concurrency();

  char* input_file = argv[3];
  char* gen_file = argv[5];

  // Create rows (cowns) with huge pages and via static allocation
  TPCCTransaction::index = new Database();

#ifdef SINGLE_TABLE
  // Big table to store all tpcc related stuff
  void* tpcc_arr_addr_warehouse = static_cast<void*>(
    aligned_alloc_hpage(1024 * (TSIZE_WAREHOUSE + TSIZE_DISTRICT + (2 * TSIZE_CUSTOMER) + TSIZE_STOCK + TSIZE_ITEM +
                                TSIZE_HISTORY + TSIZE_ORDER + TSIZE_ORDER_LINE + TSIZE_NEW_ORDER))
  );
  
  void* tpcc_arr_addr_district = static_cast<void*>(static_cast<char*>(tpcc_arr_addr_warehouse) + 1024 * TSIZE_WAREHOUSE);
  void* tpcc_arr_addr_customer = static_cast<void*>(static_cast<char*>(tpcc_arr_addr_district) + 1024 * TSIZE_DISTRICT);
  void* tpcc_arr_addr_stock = static_cast<void*>(static_cast<char*>(tpcc_arr_addr_customer) + 2 * 1024 * TSIZE_CUSTOMER);
  void* tpcc_arr_addr_item = static_cast<void*>(static_cast<char*>(tpcc_arr_addr_stock) + 1024 * TSIZE_STOCK);
  void* tpcc_arr_addr_history = static_cast<void*>(static_cast<char*>(tpcc_arr_addr_item) + 1024 * TSIZE_ITEM);
  void* tpcc_arr_addr_order = static_cast<void*>(static_cast<char*>(tpcc_arr_addr_history) + 1024 * TSIZE_HISTORY);
  void* tpcc_arr_addr_order_line = static_cast<void*>(static_cast<char*>(tpcc_arr_addr_order) + 1024 * TSIZE_ORDER);
  void* tpcc_arr_addr_new_order = static_cast<void*>(static_cast<char*>(tpcc_arr_addr_order_line) + 1024 * TSIZE_ORDER_LINE);
  
#else
  // Big table to store all tpcc related stuff
  void* tpcc_arr_addr_warehouse = static_cast<void*>(aligned_alloc_hpage(1024 * TSIZE_WAREHOUSE));
  void* tpcc_arr_addr_district = static_cast<void*>(aligned_alloc_hpage(1024 * TSIZE_DISTRICT));
  void* tpcc_arr_addr_customer = static_cast<void*>(aligned_alloc_hpage(2 * 1024 * TSIZE_CUSTOMER));
  void* tpcc_arr_addr_stock = static_cast<void*>(aligned_alloc_hpage(1024 * TSIZE_STOCK));
  void* tpcc_arr_addr_item = static_cast<void*>(aligned_alloc_hpage(1024 * TSIZE_ITEM));
  void* tpcc_arr_addr_history = static_cast<void*>(aligned_alloc_hpage(1024 * TSIZE_HISTORY));
  void* tpcc_arr_addr_order = static_cast<void*>(aligned_alloc_hpage(1024 * TSIZE_ORDER));
  void* tpcc_arr_addr_order_line = static_cast<void*>(aligned_alloc_hpage(1024 * TSIZE_ORDER_LINE));
  void* tpcc_arr_addr_new_order = static_cast<void*>(aligned_alloc_hpage(1024 * TSIZE_NEW_ORDER));
#endif

  TPCCGenerator gen(TPCCTransaction::index, tpcc_arr_addr_warehouse, tpcc_arr_addr_district, tpcc_arr_addr_customer,
  tpcc_arr_addr_stock, tpcc_arr_addr_item, tpcc_arr_addr_history, tpcc_arr_addr_order, tpcc_arr_addr_order_line, tpcc_arr_addr_new_order);

  TPCCTransaction::index->warehouse_table.start_addr = (void*)tpcc_arr_addr_warehouse;
  TPCCTransaction::index->district_table.start_addr = (void*)tpcc_arr_addr_district;
  TPCCTransaction::index->customer_table.start_addr = (void*)tpcc_arr_addr_customer;
  TPCCTransaction::index->stock_table.start_addr = (void*)tpcc_arr_addr_stock;
  TPCCTransaction::index->item_table.start_addr = (void*)tpcc_arr_addr_item;
  TPCCTransaction::index->history_table.start_addr = (void*)tpcc_arr_addr_history;
  TPCCTransaction::index->order_table.start_addr = (void*)tpcc_arr_addr_order;
  TPCCTransaction::index->order_line_table.start_addr = (void*)tpcc_arr_addr_order_line;
  TPCCTransaction::index->new_order_table.start_addr = (void*)tpcc_arr_addr_new_order;

  gen.generateWarehouses();
  gen.generateDistricts();
  gen.generateCustomerAndHistory();
  gen.generateItems();
  gen.generateStocks();
  gen.generateOrdersAndOrderLines();

  build_pipelines<TPCCTransaction>(core_cnt - 1, input_file, gen_file);

  // Cleanup
  delete TPCCTransaction::index;
}

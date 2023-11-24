#pragma once
#include "db.hpp"
#include "entries.hpp"
#include "generator.hpp"
#include "rand.hpp"

#include <cpp/when.h>

using namespace verona::rt;
using namespace verona::cpp;

class TPCCGenerator
{
protected:
  Rand r;
  uint32_t num_warehouses = 1;
  Database* db;
  uint8_t* tpcc_arr_addr;
  uint8_t* tpcc_arr_addr_end;

  // Table addresses
  uint8_t* warehouse_table_addr;
  uint8_t* district_table_addr;
  uint8_t* customer_table_addr;
  uint8_t* history_table_addr;
  uint8_t* order_table_addr;
  uint8_t* new_order_table_addr;
  uint8_t* order_line_table_addr;
  uint8_t* item_table_addr;
  uint8_t* stock_table_addr;

public:
  TPCCGenerator(
    Database* _db, uint8_t* _tpcc_arr_addr, uint8_t* _tpcc_arr_addr_end)
  : db(_db),
    tpcc_arr_addr(_tpcc_arr_addr),
    tpcc_arr_addr_end(_tpcc_arr_addr_end),
    num_warehouses(NUM_WAREHOUSES)
  {
    warehouse_table_addr = tpcc_arr_addr;
    district_table_addr =
      warehouse_table_addr + (sizeof(Warehouse) * num_warehouses);
    customer_table_addr = district_table_addr +
      (sizeof(District) * num_warehouses * DISTRICTS_PER_WAREHOUSE);
    history_table_addr = customer_table_addr +
      (sizeof(Customer) * num_warehouses * DISTRICTS_PER_WAREHOUSE *
       CUSTOMERS_PER_DISTRICT);
    order_table_addr = history_table_addr +
      (sizeof(History) * num_warehouses * DISTRICTS_PER_WAREHOUSE *
       CUSTOMERS_PER_DISTRICT);
    new_order_table_addr = order_table_addr +
      (sizeof(Order) * num_warehouses * DISTRICTS_PER_WAREHOUSE *
       INITIAL_ORDERS_PER_DISTRICT);
    order_line_table_addr = new_order_table_addr +
      (sizeof(NewOrder) * num_warehouses * DISTRICTS_PER_WAREHOUSE *
       INITIAL_ORDERS_PER_DISTRICT);
    item_table_addr = order_line_table_addr +
      (sizeof(OrderLine) * num_warehouses * DISTRICTS_PER_WAREHOUSE *
       INITIAL_ORDERS_PER_DISTRICT);
    stock_table_addr = item_table_addr + (sizeof(Item) * NUM_ITEMS);
  }

  // TPC-C Reference
  // https://www.tpc.org/TPC_Documents_Current_Versions/pdf/tpc-c_v5.11.0.pdf
  // Ref: page 65
  void generateWarehouses()
  {
    std::cout << "Generating warehouses ..." << std::endl;

    for (uint64_t w_id = 1; w_id <= num_warehouses; w_id++)
    {
      Warehouse _warehouse = Warehouse(w_id);

      _warehouse.w_tax = (double)(rand() % 2000) / 10000;
      strcpy(_warehouse.w_name, r.generateRandomString(6, 10).c_str());
      strcpy(_warehouse.w_street_1, r.generateRandomString(10, 20).c_str());
      strcpy(_warehouse.w_street_2, r.generateRandomString(10, 20).c_str());
      strcpy(_warehouse.w_city, r.generateRandomString(10, 20).c_str());
      strcpy(_warehouse.w_state, r.generateRandomString(2).c_str());
      strcpy(_warehouse.w_zip, r.generateRandomString(9).c_str());

      _warehouse.w_ytd = 300000;

      uint8_t* row_addr = warehouse_table_addr + (sizeof(Warehouse) * (w_id - 1));
      db->warehouse_table.insert_row(
        _warehouse.hash_key(),
        make_cown_custom<Warehouse>(row_addr, _warehouse));
    }

    return;
  }

  void generateDistricts()
  {
    std::cout << "Generating districts ..." << std::endl;

    for (uint32_t w_id = 1; w_id <= num_warehouses; w_id++)
    {
      for (uint32_t d_id = 1; d_id <= DISTRICTS_PER_WAREHOUSE; d_id++)
      {
        District _district = District(w_id, d_id);

        _district.d_tax = (double)(rand() % 2000) / 100;
        strcpy(_district.d_name, r.generateRandomString(6, 10).c_str());
        strcpy(_district.d_street_1, r.generateRandomString(10, 20).c_str());
        strcpy(_district.d_street_2, r.generateRandomString(10, 20).c_str());
        strcpy(_district.d_city, r.generateRandomString(10, 20).c_str());
        strcpy(_district.d_state, r.generateRandomString(2).c_str());
        strcpy(_district.d_zip, r.generateRandomString(9).c_str());

        _district.d_ytd = 300000;
        _district.d_next_o_id = 3001;

        uint8_t* row_addr = district_table_addr +
          (sizeof(District) * ((w_id - 1) * DISTRICTS_PER_WAREHOUSE + d_id));

        db->district_table.insert_row(
          _district.hash_key(),
          make_cown_custom<District>(row_addr, _district));
      }
    }

    return;
  }

  void generateCustomerAndHistory()
  {
    std::cout << "Generating customers and history ..." << std::endl;

    for (uint32_t w_id = 1; w_id <= num_warehouses; w_id++)
    {
      for (uint32_t d_id = 1; d_id <= DISTRICTS_PER_WAREHOUSE; d_id++)
      {
        for (uint32_t c_id = 1; c_id <= CUSTOMERS_PER_DISTRICT; c_id++)
        {
          Customer _customer = Customer(w_id, d_id, c_id);
          strcpy(_customer.c_first, r.generateRandomString(8, 16).c_str());
          strcpy(_customer.c_middle, "OE");

          if (c_id <= 1000)
          {
            strcpy(_customer.c_last, r.getCustomerLastName(c_id - 1).c_str());
          }
          else
          {
            strcpy(
              _customer.c_last, r.getNonUniformCustomerLastNameLoad().c_str());
          }

          _customer.c_discount = (double)(rand() % 5000) / 10000;
          strcpy(
            _customer.c_credit, (r.randomNumber(0, 99) > 10) ? "GC" : "BC");
          _customer.c_credit_lim = 50000;
          _customer.c_balance = -10.0;
          _customer.c_ytd_payment = 10;
          _customer.c_payment_cnt = 1;
          _customer.c_delivery_cnt = 0;
          strcpy(_customer.c_street_1, r.generateRandomString(10, 20).c_str());
          strcpy(_customer.c_street_2, r.generateRandomString(10, 20).c_str());
          strcpy(_customer.c_city, r.generateRandomString(10, 20).c_str());
          strcpy(_customer.c_state, r.generateRandomString(3).c_str());
          strcpy(_customer.c_zip, r.randomNStr(4).c_str());
          strcpy(_customer.c_phone, r.randomNStr(16).c_str());
          strcpy(_customer.c_middle, "OE");
          strcpy(_customer.c_data, r.generateRandomString(300, 500).c_str());
          _customer.c_since = r.GetCurrentTime();

            uint8_t* row_addr = customer_table_addr +
              (sizeof(Customer) * ((w_id - 1) * DISTRICTS_PER_WAREHOUSE *
                                   CUSTOMERS_PER_DISTRICT +
                                   (d_id - 1) * CUSTOMERS_PER_DISTRICT +
                                   (c_id - 1)));

          db->customer_table.insert_row(
            _customer.hash_key(), make_cown_custom<Customer>(row_addr, _customer));

          // History
          History _history = History(w_id, d_id, c_id);
          _history.h_amount = 10.00;
          strcpy(_history.h_data, r.generateRandomString(12, 24).c_str());
          _history.h_date = r.GetCurrentTime();

          uint8_t* history_row_addr = history_table_addr +
            (sizeof(History) * ((w_id - 1) * DISTRICTS_PER_WAREHOUSE *
                                 CUSTOMERS_PER_DISTRICT +
                                 (d_id - 1) * CUSTOMERS_PER_DISTRICT +
                                 (c_id - 1)));

          db->history_table.insert_row(
            _history.hash_key(), make_cown_custom<History>(history_row_addr, _history));
        }
      }
    }
    return;
  }

  void generateItems()
  {
    std::cout << "Generating items ..." << std::endl;

    for (uint32_t i_id = 1; i_id <= NUM_ITEMS; i_id++)
    {
      Item _item = Item(i_id);
      strcpy(_item.i_name, r.generateRandomString(14, 24).c_str());
      strcpy(_item.i_data, r.generateRandomString(26, 50).c_str());
      _item.i_price = (double)(rand() % 9900 + 100) / 100;
      _item.i_im_id = r.randomNumber(1, 10000);

      if (r.randomNumber(0, 100) > 10)
      {
        strcpy(_item.i_data, r.generateRandomString(26, 50).c_str());
      }
      else
      {
        uint64_t rand_pos = r.randomNumber(0, 25);
        std::string first_part = r.generateRandomString(rand_pos);
        std::string last_part = r.generateRandomString(50 - rand_pos);
        strcpy(_item.i_data, (first_part + "ORIGINAL" + last_part).c_str());
      }

    uint8_t *row_addr = item_table_addr + (sizeof(Item) * (i_id - 1));

      db->item_table.insert_row(_item.hash_key(), make_cown_custom<Item>(row_addr, _item));
    }
  }

  void generateStocks()
  {
    std::cout << "Generating stocks ..." << std::endl;

    for (uint32_t w_id = 1; w_id <= num_warehouses; w_id++)
    {
      for (uint32_t i_id = 1; i_id <= NUM_ITEMS; i_id++)
      {
        Stock _stock = Stock(w_id, i_id);
        _stock.s_quantity = r.randomNumber(10, 100);

        strcpy(_stock.s_dist_01, r.generateRandomString(24).c_str());
        strcpy(_stock.s_dist_02, r.generateRandomString(24).c_str());
        strcpy(_stock.s_dist_03, r.generateRandomString(24).c_str());
        strcpy(_stock.s_dist_04, r.generateRandomString(24).c_str());
        strcpy(_stock.s_dist_05, r.generateRandomString(24).c_str());
        strcpy(_stock.s_dist_06, r.generateRandomString(24).c_str());
        strcpy(_stock.s_dist_07, r.generateRandomString(24).c_str());
        strcpy(_stock.s_dist_08, r.generateRandomString(24).c_str());
        strcpy(_stock.s_dist_09, r.generateRandomString(24).c_str());
        strcpy(_stock.s_dist_10, r.generateRandomString(24).c_str());

        _stock.s_ytd = 0;
        _stock.s_order_cnt = 0;
        _stock.s_remote_cnt = 0;
        strcpy(_stock.s_data, r.generateRandomString(26, 50).c_str());

        if (r.randomNumber(0, 100) > 10)
        {
          strcpy(_stock.s_data, r.generateRandomString(26, 50).c_str());
        }
        else
        {
          uint64_t rand_pos = r.randomNumber(0, 25);
          std::string first_part = r.generateRandomString(rand_pos);
          std::string last_part = r.generateRandomString(50 - rand_pos);
          strcpy(_stock.s_data, (first_part + "ORIGINAL" + last_part).c_str());
        }

        uint8_t* row_addr = stock_table_addr + (sizeof(Stock) * ((w_id - 1) * NUM_ITEMS + (i_id - 1)));

        db->stock_table.insert_row(_stock.hash_key(), make_cown_custom<Stock>(row_addr, _stock));
      }
    }
  }

  void generateOrdersAndOrderLines()
  {
    std::cout << "Generating orders and order lines ..." << std::endl;

    for (uint32_t w_id = 1; w_id <= num_warehouses; w_id++)
    {
      for (uint32_t d_id = 1; d_id <= DISTRICTS_PER_WAREHOUSE; d_id++)
      {
        std::vector<uint32_t> customer_id_permutation =
          r.make_permutation(1, CUSTOMERS_PER_DISTRICT + 1);

        for (uint32_t o_id = 1; o_id <= INITIAL_ORDERS_PER_DISTRICT; o_id++)
        {
          Order _order = Order(w_id, d_id, o_id);
          _order.o_c_id = customer_id_permutation[o_id - 1];
          _order.o_ol_cnt = r.randomNumber(5, 15);
          _order.o_carrier_id = r.randomNumber(1, 10);
          _order.o_entry_d = r.GetCurrentTime();

          // OrderLine
          for (uint32_t ol_number = 1; ol_number <= _order.o_ol_cnt;
               ol_number++)
          {
            OrderLine _order_line = OrderLine(w_id, d_id, o_id, ol_number);
            _order_line.ol_i_id = r.randomNumber(1, NUM_ITEMS);
            _order_line.ol_supply_w_id = w_id;
            _order_line.ol_quantity = 5;
            _order_line.ol_amount =
              (_order.o_id > 2100) ? 0.00 : r.randomNumber(1, 999999) / 100.0;
            _order_line.ol_delivery_d =
              (_order.o_id > 2100) ? _order.o_entry_d : 0;
            strcpy(
              _order_line.ol_dist_info, r.generateRandomString(24).c_str());

            uint8_t* row_addr = order_line_table_addr + (sizeof(OrderLine) *
              ((w_id - 1) * DISTRICTS_PER_WAREHOUSE * INITIAL_ORDERS_PER_DISTRICT +
               (d_id - 1) * INITIAL_ORDERS_PER_DISTRICT + (o_id - 1) *
                 _order.o_ol_cnt + (ol_number - 1)));

            db->order_line_table.insert_row(
              _order_line.hash_key(), make_cown_custom<OrderLine>(row_addr, _order_line));
          }

        uint8_t* row_addr = order_table_addr + (sizeof(Order) *
          ((w_id - 1) * DISTRICTS_PER_WAREHOUSE * INITIAL_ORDERS_PER_DISTRICT +
           (d_id - 1) * INITIAL_ORDERS_PER_DISTRICT + (o_id - 1)));

          db->order_table.insert_row(
            _order.hash_key(), make_cown_custom<Order>(row_addr, _order));
        }
      }
    }
    return;
  }
};

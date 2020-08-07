#pragma once
#include <string>

class Config {
 public:
  int thread_cnt;
  std::string schema_file;
  std::string cc_protocol;

  /**
   * 2pl
   */
  int count_lock_node_index;
  int count_lock_node_property;
  int count_lock_adjacency_index;
  int count_lock_edge_property;
  bool print_mid_rst = false;
  size_t mid_rst_max_row = 20;

  static Config* get() {
    static Config cfg;
    return &cfg;
  }
};
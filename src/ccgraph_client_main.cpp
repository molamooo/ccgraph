#include "ccgraph_client.hpp"
#include "utils/logger.hpp"
#include "config.hpp"

int main(int argc, char** argv) {
  std::string addr;
  if (argc == 1) addr = "127.0.0.1:55555";
  CCGraphClient client(addr);
  
  char buf[2000];
  while (true) {
    std::cerr << "> ";
    if (!std::cin.getline(buf, 2000)) {
      LOG_ERROR("error when reading line");
      return 0;
    }
    if (buf[0] == '\0') continue;
    if (strcmp(buf, "exit") == 0 || strcmp(buf, "quit") == 0) {return 0;}
    if (strcmp(buf, "shutdown") == 0) {
      client.ShutDown();
      return 0;
    }
    RetCode rc;
    std::vector<std::vector<std::string>> result_table;
    std::vector<std::string> result_table_col_alias;
    if (strncmp(buf, "query ", strlen("query ")) == 0) {
      char* next_bar = std::strchr(buf+6, '|');
      if (next_bar) *next_bar = '\0';
      std::string qname = buf+6;
      std::vector<std::string> params;

      while(next_bar) {
        char* head_of_this_param = next_bar+1;
        next_bar = std::strchr(head_of_this_param, '|');
        if (next_bar) *next_bar = '\0';
        params.push_back(head_of_this_param);
      }
      client.RunTxn(qname, params, false, rc, result_table_col_alias, result_table);
      if (rc == kFatal) {
        std::cout << result_table[0][0] << "\n";
        continue;
      }
      if (rc == kAbort) {
        std::cout << "Abort : " << result_table[0][0] << "\n";
        continue;
      }
      if (rc == kConflict) {
        std::cout << "Conflict : " << result_table[0][0] << "\n";
        continue;
      }
      for (size_t j = 0; j < result_table_col_alias.size(); j++) {
        if (j != 0) std::cout << ", ";
        std::cout << result_table_col_alias[j];
      }
      std::cout << "\n";
      for (size_t i = 0; i < result_table.size(); i++) {
        for (size_t j = 0; j < result_table[i].size(); j++) {
          if (j != 0) std::cout << ", ";
          std::cout << result_table[i][j];
        }
        std::cout << "\n";
      }
      continue;
    }
    client.RunCmd(buf, rc, result_table);
    if (rc == kFatal) {
      std::cout << result_table[0][0];
      continue;
    }
    if (rc == kAbort) {
      std::cout << "Abort : " << result_table[0][0];
      continue;
    }
    for (size_t i = 0; i < result_table.size(); i++) {
      for (size_t j = 0; j < result_table[i].size(); j++) {
        if (j != 0) std::cout << ", ";
        std::cout << result_table[i][j];
      }
      std::cout << "\n";
    }
  }
}
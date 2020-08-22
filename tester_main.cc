#include "node.hpp"
#include "edge.hpp"
#include "obj_allocator.hpp"
#include "exceptions.hpp"

#include "cc_framework_core.hpp"
#include "logger.hpp"
#include "graphstore/graph_loader.hpp"

#include <iostream>
#include <folly/futures/Future.h>
#include <folly/init/Init.h>

using VFuture=folly::Future<folly::Unit>;
using VPromise=folly::Promise<folly::Unit>;

void recursive() {
  static int a = 0;
  a++;
  if (a == 10000) {
    std::cout << " reach 10000\n";
    while(1);
  }
  folly::makeFuture()
    // .via(folly::getGlobalCPUExecutor())
    .thenValue([](folly::Unit){
      recursive();
    });
}

int main (int argc, char** argv) {
  folly::init(&argc, &argv);
  simplelogger::setLogDebugLevel(SIMPLOG_INFO);

  // folly::Promise<int> p;
  // folly::Future<int> f = p.getFuture();
  // std::move(f).thenValue([](int)->int{
  //   throw Unimplemented("Test");
  // }).thenValue([](int){
  //   std::cout << "This should not be shown\n";
  //   return folly::makeFuture((int)3);
  // }).thenValue([](int){
  //   std::cout << "This should not be shown again\n";
  //   return folly::makeFuture((int)4);
  // }).thenError(folly::tag_t<std::exception>{}, [](std::exception const & e){
  //   std::cout << e.what() << " is the error msg" << std::endl;
  //   return 0;
  // }).thenValue([](int){
  //   std::cout << "This should not be shown last\n";
  //   return folly::makeFuture((int)4);
  // });
  // p.setValue(1);
  folly::Promise<int> p;
  folly::Future<int> f = p.getFuture().thenValue([](int)->int{
    throw Unimplemented("Test");
  }).thenValue([](int){
    std::cout << "This should not be shown\n";
    return folly::makeFuture((int)3);
  });
  std::cout << "the exception future is ready?" << f.isReady() << std::endl;
  p.setValue(1);
  std::cout << "the exception future is ready?" << f.isReady() << std::endl;


  

  CCFrameworkCore * ccfcore = new CCFrameworkCore;

  Config & config = *Config::get();;

  config.cc_protocol = "2pl";
  config.count_lock_adjacency_index = 1000;
  config.count_lock_edge_property = 1000;
  config.count_lock_node_index = 0;
  config.count_lock_node_property = 1000;
  config.schema_file = "/home/sxn/ccgraph/graph_desc/schema_test";
  config.thread_cnt = 1;

  ccfcore->init();
  LOG_INFO("CCFrameworkCore init done");
  LOG_INFO("Loading graph using /home/sxn/ccgraph/graph_desc/datafile_descriptor");
  ccfcore->loadGraph("/home/sxn/ccgraph/graph_desc/datafile_descriptor");
  LOG_INFO("CCFrameworkCore load done"); 

  // recursive();

  char buf[2000];
  while (true) {
    std::cerr << "> ";
    if (!std::cin.getline(buf, 2000)) {
      LOG_ERROR("error when reading line");
      return 0;
    }
    if (buf[0] == '\0') continue;
    if (strcmp(buf, "exit") == 0 || strcmp(buf, "quit") == 0) {return 0;}
    if (strncmp(buf, "set_rst_max_row", strlen("set_rst_max_row")) == 0) {
      char* next_space = std::strchr(buf, ' ');
      if (next_space == nullptr) {
        LOG_ERROR("missing parameter");
        continue;
      }
      uint64_t lens = std::stoull(next_space+1);
      Config::get()->mid_rst_max_row = lens;
      continue;
    }
    if (strncmp(buf, "set_print_mid_rst", strlen("set_print_mid_rst")) == 0) {
      char* next_space = std::strchr(buf, ' ');
      if (next_space == nullptr) {
        LOG_ERROR("missing parameter");
        continue;
      }
      if (strcmp(next_space+1, "1") == 0 || strcmp(next_space+1, "true") == 0 || strcmp(next_space+1, "on") == 0)
        Config::get()->print_mid_rst = true;
      else if (strcmp(next_space+1, "0") == 0 || strcmp(next_space+1, "false") == 0 || strcmp(next_space+1, "off") == 0)
        Config::get()->print_mid_rst = false;
      else
        LOG_ERROR("wrong param: %s", next_space+1);
      continue;
    }
    if (strncmp(buf, "set_log_level", strlen("set_log_level")) == 0) {
      char* next_space = std::strchr(buf, ' ');
      if (next_space == nullptr) {
        LOG_ERROR("missing parameter");
        continue;
      }
      if (strcmp(next_space+1, "info") == 0 )
        simplelogger::setLogDebugLevel(SIMPLOG_INFO);
      else if (strcmp(next_space+1, "error") == 0 )
        simplelogger::setLogDebugLevel(SIMPLOG_ERROR);
      else if (strcmp(next_space+1, "warn") == 0 )
        simplelogger::setLogDebugLevel(SIMPLOG_WARN);
      else if (strcmp(next_space+1, "fatal") == 0 )
        simplelogger::setLogDebugLevel(SIMPLOG_FATAL);
      else if (strcmp(next_space+1, "debug") == 0 )
        simplelogger::setLogDebugLevel(SIMPLOG_DEBUG);
      else if (strcmp(next_space+1, "verbose") == 0 )
        simplelogger::setLogDebugLevel(SIMPLOG_VERBOSE);
      else
        LOG_ERROR("wrong param: %s", next_space+1);
      continue;
    }
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
      auto f = ccfcore->runQuery(qname, params);
      f.wait();
      auto q_ptr = f.value();
      
      Result& rst = q_ptr->get_final_rst();
      LOG_VERBOSE("printing final rst ");
      rst.print();
    }

  }
}
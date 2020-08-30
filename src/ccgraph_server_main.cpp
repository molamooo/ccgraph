#include "ccgraph.grpc.pb.h"
#include "cc_framework_core.hpp"

#include <vector>
#include <grpcpp/grpcpp.h>
#include <grpcpp/resource_quota.h>
#include <grpcpp/health_check_service_interface.h>
#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include <folly/init/Init.h>

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerBuilderOption;
using grpc::ServerContext;
using grpc::ResourceQuota;
using grpc::Status;

using CCGraphRPC::CallParam;
using CCGraphRPC::CommandParam;
using CCGraphRPC::Results;
using CCGraphRPC::StartParam;

void set_rpc_code(Results* response, RetCode rc) {
  if (rc == kOk) response->set_code(CCGraphRPC::kOk);
  else if (rc == kAbort) response->set_code(CCGraphRPC::kAbort);
  else if (rc == kConflict) response->set_code(CCGraphRPC::kConflict);
  else if (rc == kFatal) response->set_code(CCGraphRPC::kFatal);
}

class CCGraphServerImpl final : public CCGraphRPC::CCGraphServer::Service {
 private:
  folly::Promise<folly::Unit> _exit_p;
 public:
  CCFrameworkCore* _cc_core = nullptr;
  folly::Future<folly::Unit> exit_future() { return _exit_p.getFuture(); }
  Status RunTxn(ServerContext *context, const  CallParam *request, Results *response) override {
    std::vector<std::string> params;
    for (size_t i = 0; i < request->param_list_size(); i++) {
      params.push_back(request->param_list(i));
    }
    measure_ctx * m_ctx = new measure_ctx;
    try {
      m_ctx->txn_time.start();
      // todo: implement an async server
      auto req_ptr = _cc_core->runQuery(request->txn_name(), params, request->retry(), m_ctx).wait().get();
      if (req_ptr->_rc == kOk) {
        response->set_code(CCGraphRPC::kOk);
        auto & rst = req_ptr->get_final_rst();
        for (size_t j = 0; j < rst.get_cols(); j++) {
          response->add_col_name(rst.get_col_alias(j));
        }
        for (size_t i = 0; i < rst.get_rows(); i++) {
          auto row = response->add_table();
          for (size_t j = 0; j < rst.get_cols(); j++) {
            row->add_one_row(std::move(rst.get_string(i, j)));
          }
        }
        // fixme: place measurement
        response->add_measure(m_ctx->lock_block_time.collect);
        response->add_measure(m_ctx->cc_time.collect);
        response->add_measure(m_ctx->txn_time.collect);
        response->add_measure(m_ctx->used_locks.collect);
        response->add_measure(m_ctx->reused_locks.collect);
        response->add_measure(m_ctx->blocked_locks.collect);
        response->add_measure(m_ctx->retries.collect);
        delete m_ctx;
        return Status::OK;
      }
      delete m_ctx;
      if (req_ptr->_rc != kAbort && 
          req_ptr->_rc != kConflict)
        throw FatalException("query rc must be ok or abort");

      set_rpc_code(response, req_ptr->_rc);
      response->add_table()->add_one_row(req_ptr->_abort_msg);
      return Status::OK;
    } catch (std::exception & e) {
      delete m_ctx;
      std::cerr << e.what() << "\n";
      response->set_code(CCGraphRPC::kFatal);
      response->add_table()->add_one_row(e.what());
      _exit_p.setValue();
      return Status::OK;
    }
  }
  Status Shutdown(ServerContext *context, const StartParam *request, StartParam *response) override {
    _exit_p.setValue();
    return Status::OK;
  }
  Status Command(ServerContext *context, const CommandParam *request, Results *response) override {
    std::string cmd = request->command();
    if (cmd[0] == '\0') return Status::OK;
    if (cmd == "exit" || cmd == "quit") {
      _exit_p.setValue();
      return Status::OK;
    }
    if (cmd.substr(0, strlen("set_rst_max_row")) == "set_rst_max_row") {
      const char* next_space = std::strchr(cmd.c_str(), ' ');
      if (next_space == nullptr) {
        response->set_code(CCGraphRPC::kAbort);
        response->add_table()->add_one_row("missing parameter");
        LOG_ERROR("missing parameter");
        return Status::OK;
      }
      uint64_t lens = std::stoull(next_space+1);
      Config::get()->mid_rst_max_row = lens;
      return Status::OK;
    }
    if (strncmp(cmd.c_str(), "set_print_mid_rst", strlen("set_print_mid_rst")) == 0) {
      const char* next_space = std::strchr(cmd.c_str(), ' ');
      if (next_space == nullptr) {
        response->set_code(CCGraphRPC::kAbort);
        response->add_table()->add_one_row("missing parameter");
        LOG_ERROR("missing parameter");
        return Status::OK;
      }

      response->set_code(CCGraphRPC::kOk);
      if (strcmp(next_space+1, "1") == 0 || strcmp(next_space+1, "true") == 0 || strcmp(next_space+1, "on") == 0)
        Config::get()->print_mid_rst = true;
      else if (strcmp(next_space+1, "0") == 0 || strcmp(next_space+1, "false") == 0 || strcmp(next_space+1, "off") == 0)
        Config::get()->print_mid_rst = false;
      else {
        response->set_code(CCGraphRPC::kAbort);
        response->add_table()->add_one_row((Formatter() << "wrong param: " << next_space + 1).str());
        LOG_ERROR("wrong param: %s", next_space+1);
      }
      return Status::OK;
    }
    if (strncmp(cmd.c_str(), "set_log_level", strlen("set_log_level")) == 0) {
      const char* next_space = std::strchr(cmd.c_str(), ' ');
      if (next_space == nullptr) {
        response->set_code(CCGraphRPC::kAbort);
        response->add_table()->add_one_row("missing parameter");
        LOG_ERROR("missing parameter");
        return Status::OK;
      }
      response->set_code(CCGraphRPC::kOk);
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
      else {
        response->set_code(CCGraphRPC::kAbort);
        response->add_table()->add_one_row((Formatter() << "wrong param: " << next_space + 1).str());
        LOG_ERROR("wrong param: %s", next_space+1);
      }
      return Status::OK;
    }

    response->set_code(CCGraphRPC::kAbort);
    response->add_table()->add_one_row((Formatter() << "unknown command: " << cmd).str());
    LOG_ERROR("unknown command: %s", cmd.c_str());
    return Status::OK;
    
  }
};


int main (int argc, char** argv) {
  folly::init(&argc, &argv);
  simplelogger::setLogDebugLevel(SIMPLOG_INFO);

  CCFrameworkCore * ccfcore = new CCFrameworkCore;

  Config & config = *Config::get();;

  config.cc_protocol = "2pl";
  config.count_lock_adjacency_index = 10000;
  config.count_lock_edge_property = 10000;
  config.count_lock_node_index = 0;
  config.count_lock_node_property = 10000;
  config.schema_file = "/home/sxn/ccgraph/graph_desc/schema_test";
  config.thread_cnt = 40;

  ccfcore->init();
  LOG_INFO("CCFrameworkCore init done");
  LOG_INFO("Loading graph using /home/sxn/ccgraph/graph_desc/datafile_descriptor");
  ccfcore->loadGraph("/home/sxn/ccgraph/graph_desc/datafile_descriptor");
  LOG_INFO("CCFrameworkCore load done"); 

  std::string server_address("127.0.0.1:55555");
  CCGraphServerImpl service;
  service._cc_core = ccfcore;

  grpc::EnableDefaultHealthCheckService(true);
  grpc::reflection::InitProtoReflectionServerBuilderPlugin();

  ServerBuilder builder;
  // Listen on the given address without any authentication mechanism.
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  // Register "service" as the instance through which we'll communicate with
  // clients. In this case it corresponds to an *synchronous* service.
  builder.RegisterService(&service);
  ResourceQuota rq;
  rq.SetMaxThreads(46);
  builder.SetResourceQuota(rq);
  // Finally assemble the server.
  std::unique_ptr<Server> server(builder.BuildAndStart());
  std::cout << "Server listening on " << server_address << std::endl;
  auto f = service.exit_future();
  f.wait();
  server->Shutdown();
  server->Wait();
  delete ccfcore;
}
#pragma once
#include <iostream>

#include <grpcpp/grpcpp.h>

#include "ccgraph.grpc.pb.h"
#include "consts.hpp"
#include "formatter.hpp"

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;

class CCGraphClient {
public:
  CCGraphClient(const std::string &addr)
      : stub_(CCGraphRPC::CCGraphServer::NewStub(
            grpc::CreateChannel(addr, grpc::InsecureChannelCredentials()))) {}

  void RunTxn(const std::string &txn_name,
              const std::vector<std::string> &params,
              const bool retry,
              RetCode & rc,
              std::vector<std::string> & result_table_col_alias,
              std::vector<std::vector<std::string>> & result_table) {
    CCGraphRPC::CallParam rqst;
    rqst.set_txn_name(txn_name);
    for (auto & param : params) {
      rqst.add_param_list(param);
    }
    ClientContext ctx;
    CCGraphRPC::Results rply;
    Status status = stub_->RunTxn(&ctx, rqst, &rply);
    if (!status.ok()) {
      rc = kFatal;
      result_table.push_back({Formatter() << "GRPC Error: " << status.error_code() << ": " << status.error_message()});
      return;
    }

    CCGraphRPC::Code c = rply.code();
    if (c == CCGraphRPC::kOk) {
      rc = RetCode::kOk;
      for (size_t j = 0; j < rply.col_name_size(); j++) {
        result_table_col_alias.push_back(rply.col_name(j));
      }
      for (size_t i = 0; i < rply.table_size(); i++) {
        result_table.push_back({});
        for (size_t j = 0; j < rply.table(i).one_row_size(); j++) {
          result_table.back().push_back(rply.table(i).one_row(j));
        }
      }
    } else if (c == CCGraphRPC::kAbort) {
      rc = RetCode::kAbort;
    } else if (c == CCGraphRPC::kFatal) {
      rc = RetCode::kFatal;
    }
  }
  void ShutDown() {
    CCGraphRPC::StartParam rqst;
    ClientContext ctx;
    CCGraphRPC::StartParam rply;
    Status status = stub_->Shutdown(&ctx, rqst, &rply);
    if (!status.ok()) {
      std::cout << status.error_code() << ": " << status.error_message()
                << std::endl;
      return;
    }
  }
  void RunCmd(const std::string & cmd, RetCode & rc, std::vector<std::vector<std::string>> & result_table) {
    CCGraphRPC::CommandParam rqst;
    rqst.set_command(cmd);
    ClientContext ctx;
    CCGraphRPC::Results rply;
    Status status = stub_->Command(&ctx, rqst, &rply);
    if (!status.ok()) {
      rc = kFatal;
      result_table.push_back({Formatter() << "GRPC Error: " << status.error_code() << ": " << status.error_message()});
      return;
    }

    CCGraphRPC::Code c = rply.code();
    if (c == CCGraphRPC::kOk) {
      rc = RetCode::kOk;
      for (size_t i = 0; i < rply.table_size(); i++) {
        result_table.push_back({});
        for (size_t j = 0; j < rply.table(i).one_row_size(); j++) {
          result_table.back().push_back(rply.table(i).one_row(j));
        }
      }
    } else if (c == CCGraphRPC::kAbort) {
      rc = RetCode::kAbort;
    } else if (c == CCGraphRPC::kFatal) {
      rc = RetCode::kFatal;
    }

  }

private:
  std::unique_ptr<CCGraphRPC::CCGraphServer::Stub> stub_;
};
#pragma once
#include "node_index.hpp"
#include "edge_index.hpp"
#include "lock.hpp"

#include "store/string_server.hpp"
#include <unordered_map>
#include <fstream>
#include <limits>
#include <thread>

class GraphLoader {
 private:
  NodeIndex* _node_index;
  LabelEdgeIndex* _edge_index;
  SchemaManager* _schema;
  std::function<RetCode(std::string, std::vector<std::string>)> _lambda;
  bool do_txn = false;


  enum ColumnType {
    kNodeId = 0,
    kEdgeId1,
    kEdgeId2,
    kLabel,
    kPropIdx
  };

  struct ColumnMapper {
    // if fix_label is false, this need to be set when data line is read and the label is find out
    size_t internal_prop_idx;
    std::string prop_name = "";
    ColumnType col_type;

    // this is only useful for edge
    std::string base_label_of_ex_id;
  };

  struct HeaderFileCtx {
    std::string header_file_name = "";
    bool is_node = false;
    bool fix_label = true; // other wise, label is inside the data file.
    // this is only useful for node.

    // if fix_label is true
    label_t label = 0;
    std::string label_name = "";
    // if false
    size_t label_col=0;

    std::vector<ColumnMapper> column_descriptor;
  };
  struct DataFileCtx {
    std::string data_file_name = "";
    size_t header_file_index = 0;
  };

  std::vector<HeaderFileCtx> headers;
  std::vector<DataFileCtx> data_files;

  // return false for no more new_header

  void construct_column_mapper(HeaderFileCtx* header_ctx) {
    std::ifstream f(header_ctx->header_file_name);
    char buf[200];
    if (!f.getline(buf, 200)) throw FatalException("Read header file fail");
    label_t & label = header_ctx->label;
    if (header_ctx->fix_label == false) {header_ctx->is_node = true;}
    else {
      label = _schema->get_label_by_name(header_ctx->label_name);
      header_ctx->is_node = _schema->IsNode(label);
    }

    char* col_ptr = buf;
    char* bar_ptr = strchr(buf, '|');
    if (bar_ptr) *bar_ptr = '\0';

    if (header_ctx->is_node) {
      if (strncmp(col_ptr, "id:ID", 5) != 0) {
        throw FatalException("The first column of node header must be id:ID");
      }
      char* possible_base_label = col_ptr + strlen("id:ID") + 1;
      possible_base_label[strlen(possible_base_label) - 1] = '\0';
      header_ctx->column_descriptor.push_back({std::numeric_limits<size_t>::max(), "", kNodeId, possible_base_label});
    } else {
      if (strncmp(col_ptr, ":START_ID", 9) != 0) {
        throw FatalException("The first column of edge header must be :START_ID");
      }
      char* possible_base_label = col_ptr + strlen(":START_ID") + 1;
      possible_base_label[strlen(possible_base_label) - 1] = '\0';
      header_ctx->column_descriptor.push_back({std::numeric_limits<size_t>::max(), "", kEdgeId1, possible_base_label});
      col_ptr = bar_ptr+1;
      bar_ptr = strchr(col_ptr, '|');
      if (bar_ptr) *bar_ptr = '\0';
      if (strncmp(col_ptr, ":END_ID", 7) != 0) {
        throw FatalException(Formatter() << "The sec column of edge header must be :END_ID, header_file is " << header_ctx->header_file_name);
      }
      possible_base_label = col_ptr + strlen(":END_ID") + 1;
      possible_base_label[strlen(possible_base_label) - 1] = '\0';
      header_ctx->column_descriptor.push_back({std::numeric_limits<size_t>::max(), "", kEdgeId2, possible_base_label});
    }

    while(bar_ptr != nullptr) {
      // bar_ptr points to the head of this col
      col_ptr = bar_ptr+1;
      bar_ptr = strchr(col_ptr, '|');
      if (bar_ptr) *bar_ptr = '\0';

      if (strncmp(col_ptr, ":LABEL", 6) == 0) {
        header_ctx->label_col = header_ctx->column_descriptor.size();
        header_ctx->column_descriptor.push_back({std::numeric_limits<size_t>::max(), "", ColumnType::kLabel, ""});
        continue;
      }

      char* seperator_ptr = strchr(col_ptr, ':');
      if (seperator_ptr == nullptr) throw FatalException("There must be a ':' in the column");
      *seperator_ptr = '\0';
      std::string prop_name = col_ptr;

      if (header_ctx->fix_label) {
        size_t prop_idx = _schema->get_prop_idx(label, prop_name);
        header_ctx->column_descriptor.push_back({prop_idx, prop_name, ColumnType::kPropIdx, ""});
        // LOG_VERBOSE("graph loading: mapping prop \"%s:%s\" to %llu, schema name is %s", col_ptr, seperator_ptr+1, prop_idx, _schema->get_prop_desc(label, prop_idx)._name.c_str());
      } else {
        header_ctx->column_descriptor.push_back({std::numeric_limits<size_t>::max(), prop_name, ColumnType::kPropIdx, ""});
      }
    }
  }

  bool new_header(std::ifstream & f) {
    char buf[200];

    bool found = false;
    HeaderFileCtx header_ctx;
    
    while(f.getline(buf, 200)) {
      if (strcmp(buf, "") == 0) continue;
      if (buf[0] == '#') continue;
      if (strncmp(buf, "node", 4) == 0) {
        header_ctx.is_node= true;
        found = true;
      } else if (strncmp(buf, "edge", 4) == 0) {
        header_ctx.is_node = false;
        found = true;
      } else {
        throw FatalException(Formatter() << "Unrecognized line in datafile descriptor: \"" << buf << "\"");
      }

      if (buf[5] == '\0') {
        header_ctx.fix_label = false;
      } else {
        header_ctx.fix_label = true;
        header_ctx.label_name = buf + 5;
      }
      break;
    }

    if (found == false) return false;

    // std::cout << "Found header " << buf << std::endl;

    if(!f.getline(buf, 200)) {
      throw FatalException("There must be at least a header line");
    }

    header_ctx.header_file_name = buf;

    size_t header_idx = headers.size();
    headers.push_back(header_ctx);

    while(f.getline(buf, 200)) {
      if (strcmp(buf, "") == 0) break;
      if (buf[0] == '#') continue;
      DataFileCtx data_ctx;
      data_ctx.data_file_name = buf;
      data_ctx.header_file_index = header_idx;
      data_files.push_back(data_ctx);
    }

    construct_column_mapper(&headers.at(header_idx));
    return true;
  }

  Node* locate_node_via_external_id(labeled_id_t ex_id, std::string base_label) {
    Node* n1;
    std::vector<std::string> labels_to_try = _schema->possible_label_to_try(base_label);
    for (auto possible_label_name : labels_to_try) {
      ex_id.label = _schema->get_label_by_name(possible_label_name);
      n1 = _node_index->GetNodeViaLabeledId(ex_id);
      if (n1 != nullptr) break;
    }
    if (n1 == nullptr) throw FatalException(Formatter() << "Cannot find external id=" << ex_id.id << ", base label is " << base_label);
    return n1;
  }

  bool doLoadOneDataLine(const DataFileCtx* ctx, std::ifstream& f) {
    const HeaderFileCtx & header_ctx = headers[ctx->header_file_index];
    label_t label = header_ctx.label; // if not fix_label, do not use this label
    char buf[2200];
    if (!f.getline(buf, 2200)) {return false;}

    if (buf[0] == '\0') {
      return true;
    }

    char* col_ptr = buf;
    char* bar_ptr = buf-1;

    std::vector<char*> fields_buf;

    // go through all cols to find out the id and label
    labeled_id_t id1, id2;
    size_t current_col_idx = 0;
    while(bar_ptr != nullptr) {
      col_ptr = bar_ptr+1;
      bar_ptr = strchr(col_ptr, '|');
      if (bar_ptr != nullptr) *bar_ptr = '\0';
      fields_buf.push_back(col_ptr);
      switch(header_ctx.column_descriptor.at(current_col_idx).col_type) {
        case kNodeId: {
          id1 = std::strtoull(col_ptr, nullptr, 10);
          break;
        }
        case kEdgeId1: {
          id1 = std::strtoull(col_ptr, nullptr, 10);
          break;
        }
        case kEdgeId2: {
          id2 = std::strtoull(col_ptr, nullptr, 10);
          break;
        }
        case kLabel: {
          label = _schema->get_label_by_name(col_ptr);
          break;
        }
        case kPropIdx: {
          break;
        }
      }
      current_col_idx++;
    }
    // if (label == _schema->get_label_by_name("Person") && id1 == 13194139534154) {
    //   for (size_t i = 0; i < fields_buf.size(); i++) {
    //     LOG_VERBOSE("data col[%llu]: \"%s\"", i, fields_buf.at(i));
    //   }
    // }
    if (do_txn == false) {
      insert_obj(header_ctx, ctx, id1, id2, label, fields_buf);
    } else {
      insert_obj_via_txn(header_ctx, ctx, id1, id2, label, fields_buf);
    }
    
    // if (label == _schema->get_label_by_name("Person") && id1 == 13194139534154) {
    //   LOG_VERBOSE("the first 3 prop is :|%s|%s|%s|", 
    //     StringServer::get()->get(((uint64_t*)head_of_prop_to_place)[0]).c_str(), 
    //     StringServer::get()->get(((uint64_t*)head_of_prop_to_place)[1]).c_str(), 
    //     StringServer::get()->get(((uint64_t*)head_of_prop_to_place)[2]).c_str());
    //   uint64_t fn_id, ln_id, g_id;
    //   _schema->get_prop((uint8_t*)head_of_prop_to_place, _schema->get_label_by_name("Person"), _schema->get_prop_idx("Person", "firstName"), (uint8_t*)&fn_id);
    //   _schema->get_prop((uint8_t*)head_of_prop_to_place, _schema->get_label_by_name("Person"), _schema->get_prop_idx("Person", "lastName"), (uint8_t*)&ln_id);
    //   _schema->get_prop((uint8_t*)head_of_prop_to_place, _schema->get_label_by_name("Person"), _schema->get_prop_idx("Person", "gender"), (uint8_t*)&g_id);
    //   LOG_VERBOSE("the firstName prop is :\"%s\", ln prop is \"%s\", gender prop is \"%s\"",
    //     StringServer::get()->get(fn_id).c_str(), 
    //     StringServer::get()->get(ln_id).c_str(), 
    //     StringServer::get()->get(g_id).c_str());
    // }
    return true;
  }

  void insert_obj(
      const HeaderFileCtx & header_ctx, 
      const DataFileCtx* ctx,
      labeled_id_t id1, 
      labeled_id_t id2,
      label_t label,
      std::vector<char*> & fields_buf
    ) {
    void* head_of_prop_to_place = nullptr;
    if (header_ctx.is_node) {
      if (id1 == 8796093023738) {
        std::cout << "a node with id = 8796093023738, label is " << (int)label << ", data file is " << ctx->data_file_name << std::endl;
      }
      id1.label = label;
      Node* n = _node_index->InsertNode(label, id1);
      head_of_prop_to_place = n->_prop;
      // n->
      if (id1 == 8796093023738) {
        Node* n = _node_index->GetNodeViaLabeledId(label, id1);
        LOG_VERBOSE("node %d:%llu is inserted at 0x%x, in id is %llu", label, id1.id, n, n->_internal_id.id);
      }
    } else {
      // need to find out the internal id of node1 and node2
      std::string base_label_of_id1 = header_ctx.column_descriptor[0].base_label_of_ex_id;
      std::string base_label_of_id2 = header_ctx.column_descriptor[1].base_label_of_ex_id;

      if ((id1 == 8796093023738 || id2 == 8796093023738) && label == 22) {
        LOG_VERBOSE("insert an edge of 8796093023738, id2=%llu", id1, id2);
      }

      Node *n1 = locate_node_via_external_id(id1, base_label_of_id1), *n2 = locate_node_via_external_id(id2, base_label_of_id2);
      if ((n1->_external_id == 8796093023738 || n2->_external_id == 8796093023738) && label == 22) {
        LOG_VERBOSE("insert an edge of 8796093023738, n1.exid=%llu, n2.exid=%llu,id1=%llu, id2=%llu", n1->_external_id.id, n2->_external_id.id, id1.id, id2.id);
      }
      bool _;
      Edge *e = _edge_index->TouchEdge(label, n1->_internal_id, n2->_internal_id, _);
      head_of_prop_to_place = e->_prop;
      e->_external_id1 = n1->_external_id;
      e->_external_id2 = n2->_external_id;
      e->_node1 = n1;
      e->_node2 = n2;
      if ((n1->_external_id == 8796093023738 || n2->_external_id == 8796093023738) && label == 22) {
        Edge* e = _edge_index->GetEdge(22, n1->_internal_id, n2->_internal_id);
        LOG_VERBOSE("edge %llu-[:%d]->%llu is inserted at 0x%x", id1.id, label, id2.id, e);
        std::vector<Node*> neighbours;
        internal_id_t in_id = n1->_internal_id;;
        if ((n2->_external_id == 8796093023738)) {
          in_id = n2->_internal_id;
        }
        _edge_index->GetAllNeighbour(22, in_id, dir_bidir, neighbours);
        LOG_VERBOSE("(%llu) has %llu neighbours. they are:", 8796093023738, neighbours.size());
        for (size_t i = 0; i < neighbours.size(); i++) {
          LOG_VERBOSE("  (%llu)", neighbours.at(i)->_external_id.id);
        }

      }
    }

    for (size_t i = 0; i < fields_buf.size(); i++) {
      if (header_ctx.column_descriptor[i].col_type != kPropIdx) continue;
      size_t prop_idx = header_ctx.column_descriptor[i].internal_prop_idx;
      if (header_ctx.fix_label == false) prop_idx = _schema->get_prop_idx(label, header_ctx.column_descriptor[i].prop_name);
      switch (_schema->get_prop_desc(label, prop_idx)._type) {
        case kUINT64: {
          uint64_t val = strtoull(fields_buf[i], nullptr, 10);
          _schema->set_prop((uint8_t*)head_of_prop_to_place, label, prop_idx, (uint8_t*)&val);
          // if (label == _schema->get_label_by_name("Person") && id1 == 13194139534154) {
          //   LOG_VERBOSE("placing col[%llu], a uint64 \"%s\" to propidx=%llu, prop_name in header ctx is \"%s\"", i, fields_buf[i], prop_idx, header_ctx.column_descriptor[i].prop_name.c_str());
          // }
          break;
        }
        case kUINT32: {
          uint32_t val = strtoul(fields_buf[i], nullptr, 10);
          _schema->set_prop((uint8_t*)head_of_prop_to_place, label, prop_idx, (uint8_t*)&val);
          // if (label == _schema->get_label_by_name("Person") && id1 == 13194139534154) {
          //   LOG_VERBOSE("placing col[%llu], a uint32 \"%s\" to propidx=%llu, prop_name in header ctx is \"%s\"", i, fields_buf[i], prop_idx, header_ctx.column_descriptor[i].prop_name.c_str());
          // }
          break;
        }
        case kUINT8: {
          uint8_t val = strtoul(fields_buf[i], nullptr, 10);
          _schema->set_prop((uint8_t*)head_of_prop_to_place, label, prop_idx, (uint8_t*)&val);
          // if (label == _schema->get_label_by_name("Person") && id1 == 13194139534154) {
          //   LOG_VERBOSE("placing col[%llu], a uint8 \"%s\" to propidx=%llu, prop_name in header ctx is \"%s\"", i, fields_buf[i], prop_idx, header_ctx.column_descriptor[i].prop_name.c_str());
          // }
          break;
        }
        case kINT64: {
          int64_t val = strtoll(fields_buf[i], nullptr, 10);
          _schema->set_prop((uint8_t*)head_of_prop_to_place, label, prop_idx, (uint8_t*)&val);
          // if (label == _schema->get_label_by_name("Person") && id1 == 13194139534154) {
          //   LOG_VERBOSE("placing col[%llu], a int64 \"%s\" to propidx=%llu, prop_name in header ctx is \"%s\"", i, fields_buf[i], prop_idx, header_ctx.column_descriptor[i].prop_name.c_str());
          // }
          break;
        }
        case kDOUBLE: {
          double val = strtod(fields_buf[i], nullptr);
          _schema->set_prop((uint8_t*)head_of_prop_to_place, label, prop_idx, (uint8_t*)&val);
          // if (label == _schema->get_label_by_name("Person") && id1 == 13194139534154) {
          //   LOG_VERBOSE("placing col[%llu], a double \"%s\" to propidx=%llu, prop_name in header ctx is \"%s\"", i, fields_buf[i], prop_idx, header_ctx.column_descriptor[i].prop_name.c_str());
          // }
          break;
        }
        case kSTRING: {
          uint64_t id = StringServer::get()->touch(fields_buf[i]);
          _schema->set_prop((uint8_t*)head_of_prop_to_place, label, prop_idx, (uint8_t*)&id);
          // if (label == _schema->get_label_by_name("Person") && id1 == 13194139534154) {
          //   LOG_VERBOSE("placing col[%llu], a string \"%s\" to propidx=%llu, prop_name in header ctx is \"%s\"", i, fields_buf[i], prop_idx, header_ctx.column_descriptor[i].prop_name.c_str());
          // }
          break;
        }
      }
    }
  }


  void insert_obj_via_txn(
      const HeaderFileCtx & header_ctx, 
      const DataFileCtx* ctx,
      labeled_id_t id1, 
      labeled_id_t id2,
      label_t label,
      std::vector<char*> & fields_buf) {
    // void* head_of_prop_to_place = nullptr;

    std::vector<std::string> params(_schema->get_prop_count(label));

    for (size_t i = 0; i < fields_buf.size(); i++) {
      if (header_ctx.column_descriptor[i].col_type != kPropIdx) continue;
      size_t prop_idx = header_ctx.column_descriptor[i].internal_prop_idx;
      if (header_ctx.fix_label == false) prop_idx = _schema->get_prop_idx(label, header_ctx.column_descriptor[i].prop_name);
      if (prop_idx >= params.size()) params.resize(prop_idx+1);
      params.at(prop_idx) = fields_buf[i];
      if (id1 == 343597719619) {
        std::cout << "placing " << i << "-th col of data file to " << prop_idx << "-th prop col, prop name is " << header_ctx.column_descriptor[i].prop_name << "\n";
        std::cout << "this prop is " << fields_buf[i] << "\n";
      }
    }
    if (header_ctx.is_node) {
      // if (id1 == 8796093023738) {
      //   std::cout << "a node with id = 8796093023738, label is " << (int)label << ", data file is " << ctx->data_file_name << std::endl;
      // }
      id1.label = label;
      char buf[30];
      sprintf(buf, "%lu", id1.id);
      params.insert(params.begin(), buf);
      params.insert(params.begin(), _schema->get_label_name(label));
      while (_lambda("ldbc_insert_node", params) != kOk) {
        // std::cout << "txn conflict when inserting node "<< id1.label << ":" << id1.id << "\n" ;
      }
      // if (id1 == 1287) {
      //   Node* n = _node_index->GetNodeViaLabeledId(label, id1);
      //   if (n == nullptr) {
      //     LOG_INFO("no such node with id=1287");
      //   } else {
      //     LOG_INFO("node %d:%llu is inserted at 0x%x, in id is %llu", label, id1.id, n, n->_internal_id.id);
      //   }
      // }
    } else {
      // need to find out the internal id of node1 and node2
      std::string base_label_of_id1 = header_ctx.column_descriptor[0].base_label_of_ex_id;
      std::string base_label_of_id2 = header_ctx.column_descriptor[1].base_label_of_ex_id;
      Node *n1 = locate_node_via_external_id(id1, base_label_of_id1), *n2 = locate_node_via_external_id(id2, base_label_of_id2);

      char buf[30];
      sprintf(buf, "%lu", id2.id);
      params.insert(params.begin(), buf);
      params.insert(params.begin(), _schema->get_label_name(n2->_external_id.label));
      sprintf(buf, "%lu", id1.id);
      params.insert(params.begin(), buf);
      params.insert(params.begin(), _schema->get_label_name(n1->_external_id.label));
      params.insert(params.begin(), _schema->get_label_name(label));
      while (_lambda("ldbc_insert_edge", params) != kOk) {
        // std::cout << "txn conflict when inserting edge "<< id1.label << ":" << id1.id << "->"<< id2.label << ":" << id2.id  << "\n" ;
      }

      // bool _;
      // Edge *e = _edge_index->TouchEdge(label, n1->_internal_id, n2->_internal_id, _);
      // e->_external_id1 = n1->_external_id;
      // e->_external_id2 = n2->_external_id;
      // e->_node1 = n1;
      // e->_node2 = n2;
    }
    
  }
  
  void doLoadOneDataFile(const DataFileCtx* ctx) {
    std::ifstream f(ctx->data_file_name);

    while(doLoadOneDataLine(ctx, f));
  }

 public:
  GraphLoader(
      NodeIndex* node_index, 
      LabelEdgeIndex* edge_index, 
      SchemaManager* schema,
      std::function<RetCode(std::string, std::vector<std::string>)> lambda) {
    _node_index = node_index;
    _edge_index = edge_index;
    _schema = schema;
    _lambda = lambda;
  }

  void prepare(const std::string & descption_file) {
    std::ifstream f(descption_file);
    while(new_header(f)) ;
  }

  void doLoad() {
    std::vector<DataFileCtx*> node_jobs, edge_jobs;
    for (auto & df : data_files) {
      if (headers[df.header_file_index].is_node == false) {
        edge_jobs.push_back(&df);
      }
      else {
        node_jobs.push_back(&df);
      }
    }



    const size_t loader_cnt = 40;
    std::vector<std::thread> loader_threads;

    for (size_t t = 0; t < loader_cnt; t++) {
      std::thread loader_thread([this, &node_jobs, t](){
        for (size_t i = t; i < node_jobs.size(); i += loader_cnt)
          doLoadOneDataFile(node_jobs[i]);
      });
      loader_threads.push_back(std::move(loader_thread));
    }
    for (auto & t : loader_threads) {
      t.join();
    }
    loader_threads.clear();

    std::cout << "there are " << locker_count.load() << " lockers after node load\n";

    for (size_t t = 0; t < loader_cnt; t++) {
      std::thread loader_thread([this, &edge_jobs, t](){
        for (size_t i = t; i < edge_jobs.size(); i += loader_cnt)
          doLoadOneDataFile(edge_jobs[i]);
      });
      loader_threads.push_back(std::move(loader_thread));
    }
    for (auto & t : loader_threads) {
      t.join();
    }
    std::cout << "there are " << locker_count.load() << " lockers after edge load\n";
  }
};
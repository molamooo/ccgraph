#include "query_step.hpp"
#include "build_seq.hpp"

#include "string_server.hpp"

SchemaManager* _schema = SchemaManager::get();

static label_t Person, City, Company, University, Country, Post, Comment, Forum, Continent, Tag, TagClass,
        knows, isLocatedIn, workAt, studyAt, hasCreator, isPartOf, hasTag, containerOf, hasMember, likes, replyOf, hasInterest, hasType;

void check_label_initialized() {
  static bool ready = false;
  static std::mutex mu;
  if (ready == true) {
    return;
  }
  mu.lock();
  if (ready == true) {
    mu.unlock(); return;
  }
  Person = _schema->get_label_by_name("Person");
  City = _schema->get_label_by_name("City");
  Company = _schema->get_label_by_name("Company");
  University = _schema->get_label_by_name("University");
  Country = _schema->get_label_by_name("Country");
  Continent = _schema->get_label_by_name("Continent");
  Tag = _schema->get_label_by_name("Tag");
  TagClass = _schema->get_label_by_name("TagClass");
  Post = _schema->get_label_by_name("Post");
  Comment = _schema->get_label_by_name("Comment");
  Forum = _schema->get_label_by_name("Forum");
  knows = _schema->get_label_by_name("knows");
  isLocatedIn = _schema->get_label_by_name("isLocatedIn");
  workAt = _schema->get_label_by_name("workAt");
  studyAt = _schema->get_label_by_name("studyAt");
  hasCreator = _schema->get_label_by_name("hasCreator");
  isPartOf = _schema->get_label_by_name("isPartOf");
  hasTag = _schema->get_label_by_name("hasTag");
  containerOf = _schema->get_label_by_name("containerOf");
  hasMember = _schema->get_label_by_name("hasMember");
  likes = _schema->get_label_by_name("likes");
  replyOf = _schema->get_label_by_name("replyOf");
  hasInterest = _schema->get_label_by_name("hasInterest");
  hasType = _schema->get_label_by_name("hasType");
  mu.unlock();
}

class LDBCQuery1 {
 private:
  uint64_t _person_id;
  uint64_t _first_name;

 public:
  void build(std::vector<std::string> params, Query* q_ptr) {
    check_label_initialized();
    _person_id = std::stoull(params[0]);
    StringServer* ss = StringServer::get();
    _first_name = ss->touch(params[1]);
    SeqQueryBuilder builder;

    StepCtx start_node_id_ctx = builder.put_const(Result::kLabeledNodeId, _person_id, "param_person_id", Person);
    StepCtx first_name_id_ctx = builder.put_const(Result::kSTRING, _first_name, "param_first_name");

    start_node_id_ctx
      .get_node(Person, "param_person_id", "start_person_node")
      .filter_simple([](ResultItem v){return v != 0;}, "start_person_node")
      // fix: the original ldbc result contains the start node. this is wrong, but we need to be the same
      .get_neighbour_varlen(knows, dir_bidir, 0, 3, "start_person_node", {"friend_node", "depth"}) // done: both direction
      .place_prop(0, Result::kSTRING, "friend_node", "first_name")
      .place_prop(1, Result::kSTRING, "friend_node", "last_name")
      .filter(first_name_id_ctx, CompareOp::kEq, {"first_name", "param_first_name"})
      .sort({"depth", "last_name", "friend_node"}, {true, true, true}, 20)

      .get_all_edge(studyAt, dir_out, "friend_node", "study_at_edge", true)
      .place_prop(studyAt, "classYear", Result::kUINT32, "study_at_edge", "study_time")
      .get_node(University, "study_at_edge", "university_node")
      .place_prop(University, "name", Result::kSTRING, "university_node", "uni_name")
      .get_all_neighbour(isLocatedIn, dir_out, "university_node", "university_city_node")
      .place_prop(City, "name", Result::kSTRING, "university_city_node", "uni_city_name")
      .select_group(
        {"depth", "last_name", "friend_node"}, 
        {"study_time", "uni_name", "uni_city_name"}, 
        {GroupByStep::kMakeSet, GroupByStep::kMakeSet, GroupByStep::kMakeSet,}, 
        {{{0, Result::kUINT32, CompareOp::kNe, ""}}, {{0, Result::kSTRING, CompareOp::kNe, ""}}, {{0, Result::kSTRING, CompareOp::kNe, ""}}}, 
        {"friendUniversityYear", "friendUniversityName", "friendUniversityCityName"})

      .get_all_edge(workAt, dir_out, "friend_node", "work_at_edge", true)
      .place_prop(workAt, "workFrom", Result::kUINT32, "work_at_edge", "work_time")
      .get_node(Company, "work_at_edge", "company_node")
      .place_prop(Company, "name", Result::kSTRING, "company_node", "comp_name")
      .get_all_neighbour(isLocatedIn, dir_out, "company_node", "company_city_node")
      .place_prop(Company, "name", Result::kSTRING, "company_city_node", "comp_city_name")
      .select_group(
        {"depth", "last_name", "friend_node", "friendUniversityYear", "friendUniversityName", "friendUniversityCityName"}, 
        {"work_time", "comp_name","comp_city_name"}, 
        {GroupByStep::kMakeSet, GroupByStep::kMakeSet, GroupByStep::kMakeSet,}, 
        {{{0, Result::kUINT32, CompareOp::kNe, ""}}, {{0, Result::kSTRING, CompareOp::kNe, ""}}, {{0, Result::kSTRING, CompareOp::kNe, ""}}}, 
        {"friendCompanyYear", "friendCompanyName", "friendCompanyCityName"})
      .sort({"depth", "last_name", "friend_node"}, {true, true, true}, 20)
      .get_all_neighbour(isLocatedIn, dir_out, "friend_node", "city_node");
    
    builder.new_col_to_ret(Result::kUINT64, "friend_node", kNodeLabeledId, "friendId");
    builder.new_col_to_ret(Result::kSTRING, "friend_node", _schema->get_prop_idx("Person", "lastName"), "friendLastName");
    builder.new_col_to_ret(Result::kUINT64, "depth", "distanceFromPerson");
    builder.new_col_to_ret(Result::kUINT64, "friend_node", _schema->get_prop_idx("Person", "birthday"), "friendBiethday");
    builder.new_col_to_ret(Result::kUINT64, "friend_node", _schema->get_prop_idx("Person", "creationDate"), "friendCreationDate");
    builder.new_col_to_ret(Result::kSTRING, "friend_node", _schema->get_prop_idx("Person", "gender"), "friendGender");
    builder.new_col_to_ret(Result::kSTRING, "friend_node", _schema->get_prop_idx("Person", "browserUsed"), "friendBrowserUsed");
    builder.new_col_to_ret(Result::kSTRING, "friend_node", _schema->get_prop_idx("Person", "locationIP"), "friendLocationIp");
    builder.new_col_to_ret(Result::kSTRING, "friend_node", _schema->get_prop_idx("Person", "email"), "friendEmails");
    builder.new_col_to_ret(Result::kSTRING, "friend_node", _schema->get_prop_idx("Person", "speaks"), "friendLanguages");
    builder.new_col_to_ret(Result::kSTRING, "city_node", _schema->get_prop_idx("City", "name"), "friendCityName");

    builder.new_col_to_ret(Result::kSTRING, "friendUniversityName", "friendUniversityName");
    builder.new_col_to_ret(Result::kSTRING, "friendUniversityYear", "friendUniversityYear");
    builder.new_col_to_ret(Result::kSTRING, "friendUniversityCityName", "friendUniversityCityName");
    builder.new_col_to_ret(Result::kSTRING, "friendCompanyName", "friendCompanyName");
    builder.new_col_to_ret(Result::kSTRING, "friendCompanyYear", "friendCompanyYear");
    builder.new_col_to_ret(Result::kSTRING, "friendCompanyCityName", "friendCompanyCityName");
    builder.write_to_query(q_ptr);
  }
};

class LDBCQuery2 {
 private:
  uint64_t _person_id;
  uint64_t _max_date;
 public:
  void build(std::vector<std::string> params, Query* q_ptr) {
    check_label_initialized();
    _person_id = std::stoull(params[0]);
    _max_date = std::stoull(params[1]);
    SeqQueryBuilder builder;

    StepCtx start_node_id_ctx = builder.put_const(Result::kLabeledNodeId, _person_id, "param_person_id", Person);
    StepCtx date_ctx = builder.put_const(Result::kUINT64, _max_date, "param_max_date");

    start_node_id_ctx
      .get_node(Person, "param_person_id", "start_person_node")
      .filter_simple([](ResultItem v){return v != 0;}, "start_person_node")
      .get_all_neighbour(knows, dir_bidir, "start_person_node", "friend_node") // done: both direction
      .get_all_neighbour(hasCreator, dir_in, "friend_node", "message_node")
      .place_prop(_schema->get_prop_idx("Comment", "creationDate"), Result::kUINT64, "message_node", "message_creation_date")
      .filter(date_ctx, CompareOp::kLE, {"message_creation_date", "param_max_date"})
      .sort({"message_creation_date", "message_node"}, {false, true}, 20);

    builder.new_col_to_ret(Result::kUINT64, "friend_node", kNodeLabeledId);
    builder.new_col_to_ret(Result::kSTRING, "friend_node", _schema->get_prop_idx("Person", "firstName"));
    builder.new_col_to_ret(Result::kSTRING, "friend_node", _schema->get_prop_idx("Person", "lastName"));
    builder.new_col_to_ret(Result::kUINT64, "message_node", kNodeLabeledId);
    builder.new_col_to_ret(Result::kSTRING, "message_node", _schema->get_prop_idx("Comment", "content"));
    builder.new_col_to_ret(Result::kSTRING, "message_node", _schema->get_prop_idx("Post", "imageFile"));
    builder.new_col_to_ret(Result::kUINT64, "message_creation_date");
    builder.write_to_query(q_ptr);
  }
};

class LDBCQuery3 {
 private:
  uint64_t _person_id;
  uint64_t _country_x, _country_y;
  uint64_t _min_date, _max_date;
 public:
  void build(std::vector<std::string> params, Query* q_ptr) {
    check_label_initialized();
    _person_id = std::stoull(params[0]);
    _min_date = std::stoull(params[1]);
    _max_date = std::stoull(params[2]);
    _country_x = StringServer::get()->touch(params[3]);
    _country_y = StringServer::get()->touch(params[4]);
    SeqQueryBuilder builder;

    StepCtx start_node_id_ctx = builder.put_const(Result::kLabeledNodeId, _person_id, "param_person_id", Person);
    StepCtx max_date = builder.put_const(Result::kUINT64, _max_date, "param_max_date");
    StepCtx min_date = builder.put_const(Result::kUINT64, _min_date, "param_min_date");
    StepCtx country_x = builder.put_const(Result::kSTRING, _country_x, "param_country_x");
    StepCtx country_y = builder.put_const(Result::kSTRING, _country_y, "param_country_y");
    // StepCtx zero_const = builder.put_const(Result::kUINT64, 0, "const_zero");

    start_node_id_ctx
      .get_node(Person, "param_person_id", "start_person_node")
      .filter_simple([](ResultItem v){return v != 0;}, "start_person_node")
      .get_neighbour_varlen(knows, dir_bidir, 1, 2, "start_person_node", {"friend_node", "depth"}) // done: both direction

      .get_all_neighbour(isLocatedIn, dir_out, "friend_node", "friend_city_node")
      .get_all_neighbour(isPartOf, dir_out, "friend_city_node", "friend_country_node")
      .place_prop(_schema->get_prop_idx("Country", "name"), Result::kSTRING, "friend_country_node", "friend_country_name")
      // .place_prop(0, Result::kSTRING, "friend_country_node", "friend_country_name")
      .filter(country_x, CompareOp::kNe, {"friend_country_name", "param_country_x"})
      .filter(country_y, CompareOp::kNe, {"friend_country_name", "param_country_y"})
      .get_all_neighbour(hasCreator, dir_in, "friend_node", "message_node")
      .place_prop(_schema->get_prop_idx("Comment", "creationDate"), Result::kUINT64, "message_node", "message_date")
      // .place_prop(1, Result::kUINT64, "message_node", "message_date")

      .filter(max_date, CompareOp::kL,  {"message_date", "param_max_date"})
      .filter(min_date, CompareOp::kGe, {"message_date", "param_min_date"})
      .get_all_neighbour(isLocatedIn, dir_out, "message_node", "message_country_node")
      .place_prop(_schema->get_prop_idx("Country", "name"), Result::kSTRING, "message_country_node", "message_country_name")
      // .place_prop(0, Result::kSTRING, "message_country_node", "message_country_name")
      .select_group(
        {"friend_node"}, 
        {"message_country_name", "message_country_name"}, 
        {GroupByStep::kCount, GroupByStep::kCount},
        {{{_country_x, Result::kSTRING, CompareOp::kEq, ""}}, {{_country_y, Result::kSTRING, CompareOp::kEq, ""}}},
        {"xCount", "yCount"})
      // .filter(zero_const, CompareOp::kG, {"xCount", "const_zero"})
      // .filter(zero_const, CompareOp::kG, {"yCount", "const_zero"})
      .algeo(MathOp::kAdd, {"xCount", "yCount"}, "count")
      .sort({"count", "friend_node"}, {false, true}, 20);

    builder.new_col_to_ret(Result::kUINT64, "friend_node", QueryRstColType::kNodeLabeledId);
    builder.new_col_to_ret(Result::kSTRING, "friend_node", _schema->get_prop_idx("Person", "firstName"));
    builder.new_col_to_ret(Result::kSTRING, "friend_node", _schema->get_prop_idx("Person", "lastName"));
    builder.new_col_to_ret(Result::kUINT64, "xCount");
    builder.new_col_to_ret(Result::kUINT64, "yCount");
    builder.new_col_to_ret(Result::kUINT64, "count");

    builder.write_to_query(q_ptr);
  }
};


class LDBCQuery4 {
 private:
  uint64_t _person_id;
  uint64_t _min_date, _max_date;
 public:
  void build(std::vector<std::string> params, Query* q_ptr) {
    check_label_initialized();
    _person_id = std::stoull(params[0]);
    _min_date = std::stoull(params[1]);
    _max_date = std::stoull(params[2]);
    SeqQueryBuilder builder;

    StepCtx start_node_id_ctx = builder.put_const(Result::kLabeledNodeId, _person_id, "param_person_id", Person);
    StepCtx max_date = builder.put_const(Result::kUINT64, _max_date, "param_max_date");
    // StepCtx min_date = builder.put_const(Result::kUINT64, _min_date, "param_min_date");
    StepCtx zero_const = builder.put_const(Result::kUINT64, 0, "const_zero");

    start_node_id_ctx
      .get_node(Person, "param_person_id", "start_person_node")
      .filter_simple([](ResultItem v){return v != 0;}, "start_person_node")
      .get_all_neighbour(knows, dir_bidir, "start_person_node", "friend_node") // done: both direction
      // get all message neighbour
      .get_all_neighbour(hasCreator, dir_in, "friend_node", "message_node")
      // filter out comment, only left post
      .filter_label(Post, kEq, "message_node")
      // place date
      // .place_prop(1, Result::kUINT64, "message_node", "message_date")
      .place_prop(_schema->get_prop_idx("Comment", "creationDate"), Result::kUINT64, "message_node", "message_date")
      .filter(max_date, kL, {"message_date", "param_max_date"})
      // get all tag neighbour
      .get_all_neighbour(hasTag, dir_out, "message_node", "tag_node")
      // select group: tag as key, filter date, 2 cols
      .select_group(
        {"tag_node"}, 
        {"message_date", "message_date"}, 
        {GroupByStep::kCount, GroupByStep::kCount}, 
        {{{_min_date, Result::kUINT64, kL, ""}},{{_min_date, Result::kUINT64, kGe, ""}}},
        {"before_count", "after_count"})
      // filter before count, must be 0
      .filter(zero_const, kEq, {"before_count", "const_zero"})
      // place name property
      // .place_prop(0, Result::kSTRING, "tag_node", "tag_name")
      .place_prop(_schema->get_prop_idx("Tag", "name"), Result::kSTRING, "tag_node", "tag_name")
      // sort 
      .sort({"after_count", "tag_name"}, {false, true}, 10);

    builder.new_col_to_ret(Result::kSTRING, "tag_name");
    builder.new_col_to_ret(Result::kUINT64, "after_count");
    builder.write_to_query(q_ptr);
  }
};

class LDBCQuery5 {
 private:
  uint64_t _person_id;
  uint64_t _date;
 public:
  void build(std::vector<std::string> params, Query* q_ptr) {
    check_label_initialized();
    _person_id = std::stoull(params[0]);
    _date = std::stoull(params[1]);
    SeqQueryBuilder builder;

    StepCtx start_node_id_ctx = builder.put_const(Result::kLabeledNodeId, _person_id, "param_person_id", Person);
    StepCtx date = builder.put_const(Result::kUINT64, _date, "param_date");
    StepCtx null_edge = builder.put_const(Result::kEdge, 0, "const_null");

    start_node_id_ctx
      .get_node(Person, "param_person_id", "start_person_node")
      .filter_simple([](ResultItem v){return v != 0;}, "start_person_node")
      .get_neighbour_varlen(knows, dir_bidir, 1, 2, "start_person_node", {"friend_node", "depth"}) // done: both direction
      // get all message neighbour
      .get_all_neighbour(hasCreator, dir_in, "friend_node", "message_node")
      // filter out comment, only left post
      .filter_label(Post, kEq, "message_node")
      // get post's forum
      .get_all_neighbour(containerOf, dir_in, "message_node", "forum_node")
      // get the edge between forum and user
      .get_single_edge(hasMember, dir_out, {"forum_node", "friend_node"}, "member_edge")
      .filter(null_edge, kNe, {"member_edge", "const_null"})
      // .place_prop(0, Result::kUINT64, "member_edge", "join_date")
      .place_prop(_schema->get_prop_idx("hasMember", "joinDate"), Result::kUINT64, "member_edge", "join_date")
      .filter(date, kG, {"join_date", "param_date"})
      .select_group(
        {"forum_node"}, // forum node
        {"message_node"},   // msgs
        {GroupByStep::kCount}, 
        {{}},
        {"postCount"})
      //     postCount                 forum node
      .sort({"postCount", "forum_node"}, {false, true}, 20);

    builder.new_col_to_ret(Result::kSTRING, "forum_node", _schema->get_prop_idx("Forum", "title"));
    builder.new_col_to_ret(Result::kUINT64, "postCount");
    builder.write_to_query(q_ptr);
  }
};


class LDBCQuery6 {
 private:
  uint64_t _person_id;
  uint64_t _tag_name;
 public:
  void build(std::vector<std::string> params, Query* q_ptr) {
    check_label_initialized();
    _person_id = std::stoull(params[0]);
    _tag_name = StringServer::get()->touch(params[1]);
    SeqQueryBuilder builder;

    StepCtx start_node_id_ctx = builder.put_const(Result::kLabeledNodeId, _person_id, "param_person_id", Person);
    StepCtx tag_name = builder.put_const(Result::kSTRING, _tag_name, "param_tag_name");
    // StepCtx null_edge = builder.put_const(Result::kEdge, 0);

    start_node_id_ctx
      .get_node(Person, "param_person_id", "start_person_node")
      .filter_simple([](ResultItem v){return v != 0;}, "start_person_node")
      .get_neighbour_varlen(knows, dir_bidir, 1, 2, "start_person_node", {"friend_node", "depth"}) // done: both direction
      .get_all_neighbour(hasCreator, dir_in, "friend_node", "message_node")
      // todo: a better plan that does not need to query twice
      .filter_label(Post, kEq, "message_node")
      .get_all_neighbour(hasTag, dir_out, "message_node", "the_tag") // first tag
      .place_prop(0, Result::kSTRING, "the_tag", "the_tag_name")
      .filter(tag_name, kEq, {"the_tag_name", "param_tag_name"})
      .get_all_neighbour(hasTag, dir_out, "message_node", "other_tag") // other tags
      .place_prop(0, Result::kSTRING, "other_tag", "other_tag_name")
      .filter(tag_name, kNe, {"other_tag_name", "param_tag_name"})
      .select_group({"other_tag", "other_tag_name"}, {"message_node"}, {GroupByStep::kCount}, {{}}, {"count"}) // group_rst
      .sort({"count", "other_tag_name"}, {false, true}, 10);

    builder.new_col_to_ret(Result::kSTRING, "other_tag_name");
    builder.new_col_to_ret(Result::kUINT64, "count");
    builder.write_to_query(q_ptr);
  }
};


class LDBCQuery7 {
 private:
  uint64_t _person_id;
 public:
  void build(std::vector<std::string> params, Query* q_ptr) {
    check_label_initialized();
    _person_id = std::stoull(params[0]);
    SeqQueryBuilder builder;

    StepCtx start_node_id_ctx = builder.put_const(Result::kLabeledNodeId, _person_id, "param_person_id", Person);

    start_node_id_ctx
      .get_node(Person, "param_person_id", "start_person_node")
      .filter_simple([](ResultItem v){return v != 0;}, "start_person_node")
      .get_all_neighbour(hasCreator, dir_in, "start_person_node", "message_node")
      .get_all_edge(likes, dir_in, "message_node", "like_edge")
      .place_prop(0, Result::kUINT64, "like_edge", "like_date")
      .get_node(Person, "like_edge", "like_person_node")
      .sort({"like_date", "like_person_node", "message_node"}, {false, true, true})
      .select_group({"start_person_node", "like_person_node"}, {"like_date", "message_node"}, {GroupByStep::Aggregator::kFirst, GroupByStep::Aggregator::kFirst}, {{}, {}}, {"max_like_date", "the_message_node"})
      .sort({"max_like_date", "like_person_node"}, {false, true}, 20) // todo: preserve order in selece group
      // .place_prop(_schema->get_prop_idx("Comment", "creationDate"), Result::kUINT64, "the_message_node", "message_date")
      // .algeo(kTimeSubMin, {"like_date", "message_date"}, "latency")
      .get_single_edge(knows, dir_bidir, {"like_person_node", "start_person_node"}, "know_edge"); // done: both direction

    builder.new_col_to_ret(Result::kUINT64, "like_person_node", QueryRstColType::kNodeLabeledId);
    builder.new_col_to_ret(Result::kSTRING, "like_person_node", _schema->get_prop_idx("Person", "firstName"));
    builder.new_col_to_ret(Result::kSTRING, "like_person_node", _schema->get_prop_idx("Person", "lastName"));
    builder.new_col_to_ret(Result::kUINT64, "max_like_date");
    builder.new_col_to_ret(Result::kUINT64, "the_message_node", QueryRstColType::kNodeLabeledId);
    // fixme: make sure the loading procedure is correct
    builder.new_col_to_ret(Result::kSTRING, "the_message_node", _schema->get_prop_idx("Comment", "content"));
    builder.new_col_to_ret(Result::kSTRING, "the_message_node", _schema->get_prop_idx("Post", "imageFile"));
    builder.new_col_to_ret(Result::kUINT64, "the_message_node", _schema->get_prop_idx("Post", "creationDate"));
    // builder.new_col_to_ret(Result::kUINT64, "message_date");
    // fixme: if return a pointer, the result is changed into a flag
    builder.new_col_to_ret(Result::kUINT64, "know_edge");
    
    builder.write_to_query(q_ptr);
  }
};

class LDBCQuery8 {
 private:
  uint64_t _person_id;
 public:
  void build(std::vector<std::string> params, Query* q_ptr) {
    check_label_initialized();
    _person_id = std::stoull(params[0]);
    SeqQueryBuilder builder;

    StepCtx start_node_id_ctx = builder.put_const(Result::kLabeledNodeId, _person_id, "param_person_id", Person);
    // StepCtx null_edge = builder.put_const(Result::kEdge, 0);

    start_node_id_ctx
      .get_node(Person, "param_person_id", "start_person_node")
      .filter_simple([](ResultItem v){return v != 0;}, "start_person_node")
      .get_all_neighbour(hasCreator, dir_in, "start_person_node", "message_node")
      .get_all_neighbour(replyOf, dir_in, "message_node", "comment_node")
      // .place_prop(1, Result::kUINT64, "comment_node", "comment_date") // create date
      .place_prop(_schema->get_prop_idx("Comment", "creationDate"), Result::kUINT64, "comment_node", "comment_date")
      .get_all_neighbour(hasCreator, dir_out, "comment_node", "comment_author_node") // person
      .sort({"comment_date", "comment_node"}, {false, true}, 20);

    builder.new_col_to_ret(Result::kUINT64, "comment_author_node", QueryRstColType::kNodeLabeledId);
    builder.new_col_to_ret(Result::kSTRING, "comment_author_node", _schema->get_prop_idx("Person", "firstName"));
    builder.new_col_to_ret(Result::kSTRING, "comment_author_node", _schema->get_prop_idx("Person", "lastName"));
    builder.new_col_to_ret(Result::kUINT64, "comment_date");
    builder.new_col_to_ret(Result::kUINT64, "comment_node", QueryRstColType::kNodeLabeledId);
    builder.new_col_to_ret(Result::kSTRING, "comment_node", _schema->get_prop_idx("Comment", "content"));
    builder.write_to_query(q_ptr);
  }
};


class LDBCQuery9 {
 private:
  uint64_t _person_id, _max_date;
 public:
  void build(std::vector<std::string> params, Query* q_ptr) {
    check_label_initialized();
    _person_id = std::stoull(params[0]);
    _max_date = std::stoull(params[1]);
    SeqQueryBuilder builder;

    StepCtx start_node_id_ctx = builder.put_const(Result::kLabeledNodeId, _person_id, "param_person_id", Person);
    StepCtx max_date = builder.put_const(Result::kUINT64, _max_date, "param_max_date");
    // StepCtx null_edge = builder.put_const(Result::kEdge, 0);

    start_node_id_ctx
      .get_node(Person, "param_person_id", "start_person_node")
      .filter_simple([](ResultItem v){return v != 0;}, "start_person_node")
      .get_neighbour_varlen(knows, dir_bidir, 1, 2, "start_person_node", {"friend_node", "depth"}) // done: both direction
      .get_all_neighbour(hasCreator, dir_in, "friend_node", "message_node")
      // .place_prop(1, Result::kUINT64, "message_node", "message_date")
      .place_prop(_schema->get_prop_idx("Comment", "creationDate"), Result::kUINT64, "message_node", "message_date")
      .filter(max_date, kL, {"message_date", "param_max_date"})
      .sort({"message_date", "message_node"}, {false, true}, 20);

    builder.new_col_to_ret(Result::kUINT64, "friend_node", QueryRstColType::kNodeLabeledId);
    builder.new_col_to_ret(Result::kSTRING, "friend_node", _schema->get_prop_idx("Person", "firstName"));
    builder.new_col_to_ret(Result::kSTRING, "friend_node", _schema->get_prop_idx("Person", "lastName"));
    builder.new_col_to_ret(Result::kUINT64, "message_node", QueryRstColType::kNodeLabeledId);
    builder.new_col_to_ret(Result::kSTRING, "message_node", _schema->get_prop_idx("Comment", "content"));
    builder.new_col_to_ret(Result::kSTRING, "message_node", _schema->get_prop_idx("Post", "imageFile"));
    builder.new_col_to_ret(Result::kUINT64, "message_date");
    builder.write_to_query(q_ptr);
  }
};

class LDBCQuery10 {
 private:
  uint64_t _person_id;
  uint64_t _month;
 public:
  void build(std::vector<std::string> params, Query* q_ptr) {
    check_label_initialized();
    _person_id = std::stoull(params[0]);
    _month = std::stoul(params[1]);
    SeqQueryBuilder builder;

    StepCtx start_node_id_ctx = builder.put_const(Result::kLabeledNodeId, _person_id, "param_person_id", Person);

    auto score_ctx = start_node_id_ctx
      .get_node(Person, "param_person_id", "start_person_node")
      .filter_simple([](ResultItem v){return v != 0;}, "start_person_node")
      .get_neighbour_varlen(knows, dir_bidir, 1, 2, "start_person_node", {"friend_node", "depth"}) // done: both direction
      .filter_simple([](ResultItem v){
        return v == 2;
      }, "depth")
      // .place_prop(3, Result::kUINT64, "friend_node", "friend_birthday")
      .place_prop(_schema->get_prop_idx("Person", "birthday"), Result::kUINT64, "friend_node", "friend_birthday")
      // filter, on or after 21st of this month, before 22nd of next month
      .filter_simple([month = this->_month](ResultItem val)->bool{
        uint64_t md = val % 10000;
        uint64_t min_eq = month * 100 + 21;
        uint64_t max_ne = (month == 12) ? (122) : (month * 100 + 122);
        if (month != 12) {
          return min_eq <= md && md < max_ne;
        } else {
          return md < max_ne || min_eq <= md;
        }
      }, "friend_birthday")
      .get_all_neighbour(hasCreator, dir_in, "friend_node", "post_node", Post, true)
      // .filter_simple([](ResultItem val)->bool{
      //     Node* n = (Node*)val;
      //     if (n->_external_id == 8796093023668) return true;
      //     return false;
      //   }, "friend_node")
      // .filter_label(Post, kEq, "post_node")
      .get_all_neighbour(hasTag, dir_out, "post_node", "tag_node", true)
      .get_single_edge(hasInterest, dir_in, {"tag_node", "start_person_node"}, "has_interest_edge")
      .select_group(
        {"friend_node", "post_node"}, 
        {"has_interest_edge"}, 
        {GroupByStep::kCount}, 
        {{{0, Result::kEdge, kNe, ""}}},
        {"common_tag_count"})
      .select_group(
        {"friend_node"}, 
        {"common_tag_count", "common_tag_count"}, 
        {GroupByStep::kCount, GroupByStep::kCount},
        // common                   uncommon
        {{{0, Result::kUINT64, kG, ""}},{{0, Result::kUINT64, kEq, ""}, {0, Result::kNode, kNe, "post_node"}}},
        {"common", "uncommon"})
      // minus the common, uncommon
      .algeo(kSub, {"common", "uncommon"}, "score");
    score_ctx._step->get_rst().force_change_schema(score_ctx._step->get_rst().get_col_idx_by_alias("score"), Result::kINT64);
    score_ctx
      .sort({"score", "friend_node"}, {false, true}, 10)
      .get_all_neighbour(isLocatedIn, dir_out, "friend_node", "friend_city_node");

    builder.new_col_to_ret(Result::kUINT64, "friend_node", QueryRstColType::kNodeLabeledId, "id");
    builder.new_col_to_ret(Result::kSTRING, "friend_node", _schema->get_prop_idx("Person", "firstName"), "fname");
    builder.new_col_to_ret(Result::kSTRING, "friend_node", _schema->get_prop_idx("Person", "lastName"), "lname");
    builder.new_col_to_ret(Result::kINT64, "score", "score");
    builder.new_col_to_ret(Result::kUINT64, "common", "common");
    builder.new_col_to_ret(Result::kUINT64, "uncommon", "uncommon");
    builder.new_col_to_ret(Result::kSTRING, "friend_node", _schema->get_prop_idx("Person", "gender"), "gender");
    builder.new_col_to_ret(Result::kSTRING, "friend_city_node", _schema->get_prop_idx("City", "name"), "city");
    builder.write_to_query(q_ptr);   
  }
};


class LDBCQuery11 {
 private:
  uint64_t _person_id, _country_name;
  uint32_t _year;
 public:
  void build(std::vector<std::string> params, Query* q_ptr) {
    check_label_initialized();
    _person_id = std::stoull(params[0]);
    _country_name = StringServer::get()->touch(params[1]);
    _year = std::stoull(params[2]);
    SeqQueryBuilder builder;

    StepCtx start_node_id_ctx = builder.put_const(Result::kLabeledNodeId, _person_id, "param_person_id", Person);
    StepCtx year_ctx = builder.put_const(Result::kUINT32, _year, "param_year");
    StepCtx country_name_ctx = builder.put_const(Result::kSTRING, _country_name, "param_contry_name");

    start_node_id_ctx
      .get_node(Person, "param_person_id", "start_person_node")
      .filter_simple([](ResultItem v){return v != 0;}, "start_person_node")
      .get_neighbour_varlen(knows, dir_bidir, 1, 2, "start_person_node", {"friend_node", "depth"}) // done: both direction
      .get_all_edge(workAt, dir_out, "friend_node", "work_at_edge")
      .place_prop(0, Result::kUINT32, "work_at_edge", "works_from")
      // compare year, it is directly stored as year.
      .filter(year_ctx, kL, {"works_from", "param_year"})
      .get_node(Company, "work_at_edge", "company_node")
      .get_all_neighbour(isLocatedIn, dir_out, "company_node", "country_node")
      .place_prop(0, Result::kSTRING, "country_node", "country_name")
      .filter(country_name_ctx, kEq, {"country_name", "param_contry_name"})
      .place_prop(0, Result::kSTRING, "company_node", "company_name")
      .sort({"works_from", "friend_node", "company_name"}, {true, true, false}, 10);

    builder.new_col_to_ret(Result::kUINT64, "friend_node", QueryRstColType::kNodeLabeledId);
    builder.new_col_to_ret(Result::kSTRING, "friend_node", _schema->get_prop_idx("Person", "firstName"));
    builder.new_col_to_ret(Result::kSTRING, "friend_node", _schema->get_prop_idx("Person", "lastName"));
    builder.new_col_to_ret(Result::kSTRING, "company_name");
    builder.new_col_to_ret(Result::kUINT64, "works_from");
    builder.write_to_query(q_ptr);
  }
};


class LDBCQuery12 {
 private:
  uint64_t _person_id, _tagclass_name;
 public:
  void build(std::vector<std::string> params, Query* q_ptr) {
    check_label_initialized();
    _person_id = std::stoull(params[0]);
    _tagclass_name = StringServer::get()->touch(params[1]);
    SeqQueryBuilder builder;

    StepCtx start_node_id_ctx = builder.put_const(Result::kLabeledNodeId, _person_id, "param_person_id", Person);
    StepCtx tag_class_name = builder.put_const(Result::kSTRING, _tagclass_name, "param_tag_class_name");

    start_node_id_ctx
      .get_node(Person, "param_person_id", "start_person_node")
      .filter_simple([](ResultItem v){return v != 0;}, "start_person_node")
      .get_all_neighbour(knows, dir_bidir, "start_person_node", "friend_node") // done: both direction
      .get_all_neighbour(hasCreator, dir_in, "friend_node", "comment_node")
      .filter_label(Comment, kEq, "comment_node")
      .get_all_neighbour(replyOf, dir_out, "comment_node", "post_node")
      .filter_label(Post, kEq, "post_node")
      .get_all_neighbour(hasTag, dir_out, "post_node", "tag_node")
      .get_all_neighbour(hasType, dir_out, "tag_node", "tag_class_node")
      .get_neighbour_varlen(hasType, dir_out, 0, UINT64_MAX, "tag_class_node", {"base_tag_class_node", ""})
      .place_prop(0, Result::kSTRING, "base_tag_class_node", "tag_class_name")
      .filter(tag_class_name, kEq, {"tag_class_name", "param_tag_class_name"})
      .select_group({"friend_node", "tag_node"}, {"comment_node"}, {GroupByStep::kCount}, {{}}, {"comments_per_tag"})
      .place_prop(0, Result::kSTRING, "tag_node", "tag_name")
      .select_group({"friend_node"}, {"comments_per_tag", "tag_name"}, {GroupByStep::kSum, GroupByStep::kMakeSet}, {{},{}}, {"count", "tag_names"})
      .sort({"count", "friend_node"}, {false, true}, 20);

    builder.new_col_to_ret(Result::kUINT64, "friend_node", QueryRstColType::kNodeLabeledId);
    builder.new_col_to_ret(Result::kSTRING, "friend_node", _schema->get_prop_idx("Person", "firstName"));
    builder.new_col_to_ret(Result::kSTRING, "friend_node", _schema->get_prop_idx("Person", "lastName"));
    builder.new_col_to_ret(Result::kSTRING, "tag_names");
    builder.new_col_to_ret(Result::kUINT64, "count");
    builder.write_to_query(q_ptr);
  }
};


class LDBCNeighbour {
 private:
  uint64_t _person_id, _min_depth, _max_depth;
 public:
  void build(std::vector<std::string> params, Query* q_ptr) {
    check_label_initialized();
    _person_id = std::stoull(params[0]);
    _min_depth = std::stoull(params[1]);
    _max_depth = std::stoull(params[2]);
    SeqQueryBuilder builder;

    StepCtx start_node_id_ctx = builder.put_const(Result::kLabeledNodeId, _person_id, "param_person_id", Person);

    start_node_id_ctx
      .get_node(Person, "param_person_id", "start_person_node")
      .filter_simple([](ResultItem v){return v != 0;}, "start_person_node")

    builder.new_col_to_ret(Result::kUINT64, "friend_node", QueryRstColType::kNodeLabeledId);
    builder.new_col_to_ret(Result::kUINT64, "depth");
    builder.new_col_to_ret(Result::kSTRING, "friend_node", _schema->get_prop_idx("Person", "firstName"));
    builder.new_col_to_ret(Result::kSTRING, "friend_node", _schema->get_prop_idx("Person", "lastName"));
    builder.write_to_query(q_ptr);
  }
};

class LDBCUpdateBuilder {
 private:
  using str = std::string;
 public:
  SeqQueryBuilder b;
  StepCtx insertPerson(std::vector<std::string> params) {
    StepCtx person_id = b.put_const(Result::kLabeledNodeId, std::stoull(params[0]), "param_person_id", Person);
    StepCtx first_name = b.put_const(Result::kSTRING, StringServer::get()->touch(params[1]), "param_first_name");
    StepCtx last_name = b.put_const(Result::kSTRING, StringServer::get()->touch(params[2]), "param_last_name");
    StepCtx gender = b.put_const(Result::kSTRING, StringServer::get()->touch(params[3]), "param_gender");
    StepCtx birthday = b.put_const(Result::kUINT64, std::stoull(params[4]), "param_birthday");
    StepCtx email = b.put_const(Result::kSTRING, StringServer::get()->touch(params[5]), "param_email");
    StepCtx speaks = b.put_const(Result::kSTRING, StringServer::get()->touch(params[6]), "param_speaks");
    StepCtx browserUsed = b.put_const(Result::kSTRING, StringServer::get()->touch(params[7]), "param_browserUsed");
    StepCtx locationIP = b.put_const(Result::kSTRING, StringServer::get()->touch(params[8]), "param_locationIP");
    StepCtx creation_date = b.put_const(Result::kUINT64, std::stoull(params[9]), "param_creation_date");

    return person_id
      .insert_node(Person, "param_person_id", "person_node")
      .place_prop_back(Person, "firstName", Result::kSTRING, first_name, "param_first_name", "person_node")
      .place_prop_back(Person, "lastName", Result::kSTRING, last_name, "param_last_name", "person_node")
      .place_prop_back(Person, "gender", Result::kSTRING, gender, "param_gender", "person_node")
      .place_prop_back(Person, "birthday", Result::kUINT64, birthday, "param_birthday", "person_node")
      .place_prop_back(Person, "creationDate", Result::kUINT64, creation_date, "param_creation_date", "person_node")
      .place_prop_back(Person, "locationIP", Result::kSTRING, locationIP, "param_locationIP", "person_node")
      .place_prop_back(Person, "browserUsed", Result::kSTRING, browserUsed, "param_browserUsed", "person_node")
      .place_prop_back(Person, "speaks", Result::kSTRING, speaks, "param_speaks", "person_node")
      .place_prop_back(Person, "email", Result::kSTRING, email, "param_email", "person_node");
  }
  /**
   * @brief 
   * 
   * @param params 
   * @param node1 : the step to share the result table.
   * @param n1_alias 
   * @param node2 
   * @param n2_alias 
   * @return StepCtx 
   */
  StepCtx insertEdge(label_t label, std::string param, StepCtx & rst_table, str n1_alias, dir_t dir, StepCtx & node2, str n2_alias, str dst_alias) {
    // StepCtx person_id = b.put_const(Result::kLabeledNodeId, std::stoull(params[0]), "param_person_id", Person);
    StepCtx edge_prop; Result::ColumnType ty; str prop_name;
    if (label == hasMember || label == knows || label == likes) {
      ty = Result::kUINT64;
      edge_prop = b.put_const(Result::kUINT64, std::stoull(param), "p");
      if (label == hasMember) prop_name = "joinDate";
      else prop_name = "creationDate";
    } else if (label == studyAt || label == workAt) {
      ty = Result::kUINT32;
      edge_prop = b.put_const(Result::kUINT32, std::stoull(param), "p");
      if (label == studyAt) prop_name = "classYear";
      else prop_name = "workFrom";
    }

    StepCtx to_ret = rst_table.insert_edge(label, node2, {n1_alias, n2_alias}, dir, dst_alias);
    if (edge_prop._step == nullptr) {
      return to_ret;
    }
    return to_ret.place_prop_back(label, prop_name, ty, edge_prop, "p", dst_alias);
  }

  StepCtx getNode(labeled_id_t id, str dst_alias, StepCtx* prev) {
    StepCtx ret;
    if (prev == nullptr) {
      ret = b.put_const(Result::kLabeledNodeId, id.id, "p", id.label);
    } else {
      ret = prev->put_const(Result::kLabeledNodeId, id.id, "p", id.label);
    }
    return ret.get_node(id.label, "p", dst_alias);
  }

  StepCtx insertEdge(label_t label, std::string param, labeled_id_t id1, dir_t dir, labeled_id_t id2, str dst_alias) {
    StepCtx node2 = getNode(id2, "node2", nullptr);
    StepCtx node1 = getNode(id1, "node1", &node2);
    // node2._step->append_next(node1._step);
    return insertEdge(label, param, node1, "node1", dir, node2, "node2", dst_alias);
  }
  void write_to_query(Query* q) {
    b.write_to_query(q);
  }
};

class LDBCInsertPerson {
 public:
  void build(std::vector<std::string> params, Query* q_ptr) {
    check_label_initialized();
    LDBCUpdateBuilder ub;
    ub.insertPerson(params);
    ub.write_to_query(q_ptr);
  }
};
class LDBCInsertKnows {
 public:
  void build(std::vector<std::string> params, Query* q_ptr) {
    check_label_initialized();
    LDBCUpdateBuilder ub;
    ub.insertEdge(knows, params[2], {std::stoull(params[0]), Person}, dir_out, {std::stoull(params[1]), Person}, "edge");
    ub.write_to_query(q_ptr);
  }
};
/**
 * do insert at load phase. can handle all ldbc edge label;
 */
class LDBCInsertEdge {
 public:
  void build(std::vector<std::string> params, Query* q_ptr) {
    check_label_initialized();
    LDBCUpdateBuilder ub;
    label_t edge_l = _schema->get_label_by_name(params[0]),
            id1_l = _schema->get_label_by_name(params[1]),
            id2_l = _schema->get_label_by_name(params[3]);
    uint64_t id1 = std::stoull(params[2]),
             id2 = std::stoull(params[4]);
    ub.insertEdge(edge_l, params.size() == 6?params[5]:"", {id1, id1_l}, dir_out, {id2, id2_l}, "edge");
    ub.write_to_query(q_ptr);
  }
};
class LDBCInsertNode {
  SeqQueryBuilder b;
  void build_person(std::vector<std::string> params, Query* q_ptr) {
    size_t idx = 1;
    std::vector<StepCtx> ctxs(1);
    StepCtx person_id     = b.put_const(Result::kLabeledNodeId, std::stoull(params[idx++]), "param_person_id", Person); ctxs.push_back(person_id);
    StepCtx first_name    = b.put_const(Result::kSTRING,        StringServer::get()->touch(params[idx++]), "param_first_name"); ctxs.push_back(first_name);
    StepCtx last_name     = b.put_const(Result::kSTRING,        StringServer::get()->touch(params[idx++]), "param_last_name"); ctxs.push_back(last_name);
    StepCtx gender        = b.put_const(Result::kSTRING,        StringServer::get()->touch(params[idx++]), "param_gender"); ctxs.push_back(gender);
    StepCtx birthday      = b.put_const(Result::kUINT64,        std::stoull(params[idx++]), "param_birthday"); ctxs.push_back(birthday);
    StepCtx email         = b.put_const(Result::kSTRING,        StringServer::get()->touch(params[idx++]), "param_email"); ctxs.push_back(email);
    StepCtx speaks        = b.put_const(Result::kSTRING,        StringServer::get()->touch(params[idx++]), "param_speaks"); ctxs.push_back(speaks);
    StepCtx browserUsed   = b.put_const(Result::kSTRING,        StringServer::get()->touch(params[idx++]), "param_browserUsed"); ctxs.push_back(browserUsed);
    StepCtx locationIP    = b.put_const(Result::kSTRING,        StringServer::get()->touch(params[idx++]), "param_locationIP"); ctxs.push_back(locationIP);
    StepCtx creation_date = b.put_const(Result::kUINT64,        std::stoull(params[idx++]), "param_creation_date"); ctxs.push_back(creation_date);

    if (params[1] == "1521") {
      for (size_t i = 1; i < params.size(); i++) {
        std::cout << i << "-th param is " << params[i] << "\n";
        ResultItem val = ctxs[i]._step->get_rst().get(0, 0);
        if (val != 20100127000805210 && val != 19860114) {
          std::cout << StringServer::get()->get(val) << "\n";
        }

      }
    }

    person_id
      .insert_node(Person, "param_person_id", "person_node")
      .place_prop_back(Person, "firstName", Result::kSTRING, first_name, "param_first_name", "person_node")
      .place_prop_back(Person, "lastName", Result::kSTRING, last_name, "param_last_name", "person_node")
      .place_prop_back(Person, "gender", Result::kSTRING, gender, "param_gender", "person_node")
      .place_prop_back(Person, "birthday", Result::kUINT64, birthday, "param_birthday", "person_node")
      .place_prop_back(Person, "email", Result::kSTRING, email, "param_email", "person_node")
      .place_prop_back(Person, "speaks", Result::kSTRING, speaks, "param_speaks", "person_node")
      .place_prop_back(Person, "browserUsed", Result::kSTRING, browserUsed, "param_browserUsed", "person_node")
      .place_prop_back(Person, "locationIP", Result::kSTRING, locationIP, "param_locationIP", "person_node")
      .place_prop_back(Person, "creationDate", Result::kUINT64, creation_date, "param_creation_date", "person_node");
    b.write_to_query(q_ptr);
  }
  void build_forum(std::vector<std::string> params, Query* q_ptr) {
    size_t idx = 1;
    StepCtx _id     = b.put_const(Result::kLabeledNodeId, std::stoull(params[idx++]), "param_id", Person);
    StepCtx _title    = b.put_const(Result::kSTRING,        StringServer::get()->touch(params[idx++]), "param_tile");
    StepCtx _cdate     = b.put_const(Result::kUINT64,        std::stoull(params[idx++]), "param_cdate");
    _id
      .insert_node(Forum, "param_id", "_node")
      .place_prop_back(Forum, "title", Result::kSTRING, _title, "param_tile", "_node")
      .place_prop_back(Forum, "creationDate", Result::kUINT64, _cdate, "param_cdate", "_node");
    b.write_to_query(q_ptr);
  }
  void build_msg(std::vector<std::string> params, Query* q_ptr, label_t label) {
    size_t idx = 1;
    StepCtx _id       = b.put_const(Result::kLabeledNodeId, std::stoull(params[idx++]), "param_id", Person);
    StepCtx _cdate    = b.put_const(Result::kUINT64,        std::stoull(params[idx++]), "param_cdate");
    StepCtx _ip       = b.put_const(Result::kSTRING,        StringServer::get()->touch(params[idx++]), "param_ip");
    StepCtx _browser  = b.put_const(Result::kSTRING,        StringServer::get()->touch(params[idx++]), "param_browser");
    StepCtx _content  = b.put_const(Result::kSTRING,        StringServer::get()->touch(params[idx++]), "param_content");
    StepCtx _length   = b.put_const(Result::kUINT32,        std::stoull(params[idx++]), "param_length");
    StepCtx _image    = b.put_const(Result::kSTRING,        StringServer::get()->touch(params[idx++]), "param_image");
    StepCtx _language = b.put_const(Result::kSTRING,        StringServer::get()->touch(params[idx++]), "param_language");
    _id
      .insert_node(label, "param_id", "_node")
      .place_prop_back(label, "creationDate", Result::kUINT64, _cdate, "param_cdate", "_node")
      .place_prop_back(label, "locationIP", Result::kSTRING, _ip, "param_ip", "_node")
      .place_prop_back(label, "browserUsed", Result::kSTRING, _browser, "param_browser", "_node")
      .place_prop_back(label, "content", Result::kSTRING, _content, "param_content", "_node")
      .place_prop_back(label, "length", Result::kUINT64, _length, "param_length", "_node")
      .place_prop_back(label, "imageFile", Result::kSTRING, _image, "param_image", "_node")
      .place_prop_back(label, "language", Result::kSTRING, _language, "param_language", "_node");
    b.write_to_query(q_ptr);
  }
  void build_name_url(std::vector<std::string> params, Query* q_ptr, label_t label) {
    size_t idx = 1;
    StepCtx _id     = b.put_const(Result::kLabeledNodeId, std::stoull(params[idx++]), "param_id", Person);
    StepCtx _name    = b.put_const(Result::kSTRING,        StringServer::get()->touch(params[idx++]), "param_name");
    StepCtx _url     = b.put_const(Result::kSTRING,        StringServer::get()->touch(params[idx++]), "param_url");
    _id
      .insert_node(label, "param_id", "_node")
      .place_prop_back(label, "name", Result::kSTRING, _name, "param_name", "_node")
      .place_prop_back(label, "url", Result::kSTRING, _url, "param_url", "_node");
    b.write_to_query(q_ptr);
  }
 public:
  void build(std::vector<std::string> params, Query* q_ptr) {
    check_label_initialized();
    label_t node_l = _schema->get_label_by_name(params[0]);
    if (node_l == Person) build_person(params, q_ptr);
    else if (node_l == City ||
             node_l == Country ||
             node_l == Continent ||
             node_l == Tag ||
             node_l == TagClass ||
             node_l == Company ||
             node_l == University)
      build_name_url(params, q_ptr, node_l);
    else if (node_l == Forum)
      build_forum(params, q_ptr);
    else if (node_l == Comment ||
             node_l == Post)
      build_msg(params, q_ptr, node_l);
    else
      throw QueryException("no such labeled node");
  }
};
class LDBCQuerPerson {
 public:
  void build(std::vector<std::string> params, Query* q_ptr) {
    check_label_initialized();
    LDBCUpdateBuilder ub;
    ub.getNode({std::stoull(params[0]), Person}, "person", nullptr)
      .filter_simple([](ResultItem v){return v != 0;}, "person");
    ub.b.new_col_to_ret(Result::kSTRING, "person", _schema->get_prop_idx("Person", "firstName"), "firstName");
    ub.b.new_col_to_ret(Result::kSTRING, "person", _schema->get_prop_idx("Person", "lastName"), "lastName");
    ub.b.new_col_to_ret(Result::kSTRING, "person", _schema->get_prop_idx("Person", "gender"), "gender");
    ub.b.new_col_to_ret(Result::kUINT64, "person", _schema->get_prop_idx("Person", "birthday"), "birthday");
    ub.b.new_col_to_ret(Result::kUINT64, "person", _schema->get_prop_idx("Person", "creationDate"), "creationDate");
    ub.b.new_col_to_ret(Result::kSTRING, "person", _schema->get_prop_idx("Person", "locationIP"), "locationIP");
    ub.b.new_col_to_ret(Result::kSTRING, "person", _schema->get_prop_idx("Person", "browserUsed"), "browserUsed");
    ub.b.new_col_to_ret(Result::kSTRING, "person", _schema->get_prop_idx("Person", "speaks"), "speaks");
    ub.b.new_col_to_ret(Result::kSTRING, "person", _schema->get_prop_idx("Person", "email"), "email");
    ub.write_to_query(q_ptr);
  }
};

class LDBCQueryPost {
 public:
  void build(std::vector<std::string> params, Query* q_ptr) {
    check_label_initialized();
    LDBCUpdateBuilder ub;
    ub.getNode({std::stoull(params[0]), Post}, "node", nullptr)
      .filter_simple([](ResultItem v){return v != 0;}, "node");
    ub.b.new_col_to_ret(Result::kUINT64, "node", _schema->get_prop_idx("Post", "creationDate"), "creationDate");
    ub.b.new_col_to_ret(Result::kSTRING, "node", _schema->get_prop_idx("Post", "locationIP"), "locationIP");
    ub.b.new_col_to_ret(Result::kSTRING, "node", _schema->get_prop_idx("Post", "browserUsed"), "browserUsed");
    ub.b.new_col_to_ret(Result::kSTRING, "node", _schema->get_prop_idx("Post", "content"), "content");
    ub.b.new_col_to_ret(Result::kUINT64, "node", _schema->get_prop_idx("Post", "length"), "length");
    ub.b.new_col_to_ret(Result::kSTRING, "node", _schema->get_prop_idx("Post", "imageFile"), "imageFile");
    ub.b.new_col_to_ret(Result::kSTRING, "node", _schema->get_prop_idx("Post", "language"), "language");
    ub.write_to_query(q_ptr);
  }
};
class LDBCQueryComment {
 public:
  void build(std::vector<std::string> params, Query* q_ptr) {
    check_label_initialized();
    LDBCUpdateBuilder ub;
    ub.getNode({std::stoull(params[0]), Comment}, "node", nullptr)
      .filter_simple([](ResultItem v){return v != 0;}, "node");
    ub.b.new_col_to_ret(Result::kUINT64, "node", _schema->get_prop_idx("Comment", "creationDate"), "creationDate");
    ub.b.new_col_to_ret(Result::kSTRING, "node", _schema->get_prop_idx("Comment", "locationIP"), "locationIP");
    ub.b.new_col_to_ret(Result::kSTRING, "node", _schema->get_prop_idx("Comment", "browserUsed"), "browserUsed");
    ub.b.new_col_to_ret(Result::kSTRING, "node", _schema->get_prop_idx("Comment", "content"), "content");
    ub.b.new_col_to_ret(Result::kUINT64, "node", _schema->get_prop_idx("Comment", "length"), "length");
    ub.b.new_col_to_ret(Result::kSTRING, "node", _schema->get_prop_idx("Comment", "imageFile"), "imageFile");
    ub.b.new_col_to_ret(Result::kSTRING, "node", _schema->get_prop_idx("Comment", "language"), "language");
    ub.write_to_query(q_ptr);
  }
};
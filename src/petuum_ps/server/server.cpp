// author: jinliang

#include "petuum_ps/server/server.hpp"
#include "petuum_ps/util/class_register.hpp"
#include "petuum_ps/oplog/serialized_oplog_reader.hpp"
#include <utility>

namespace petuum {

Server::Server() {}

Server::~Server() {}

void Server::AddClientBgPair(int32_t client_id, int32_t bg_id) {
  client_bg_map_[client_id].push_back(bg_id);
  client_ids_.push_back(client_id);
  client_clocks_.AddClock(client_id, 0);
}

void Server::Init() {
  for (auto iter = client_bg_map_.begin();
      iter != client_bg_map_.end(); iter++){
    VectorClock vector_clock(iter->second);
    client_vector_clock_map_[iter->first] = vector_clock;

    for (auto bg_iter = iter->second.begin();
	 bg_iter != iter->second.end(); bg_iter++) {
      bg_version_map_[*bg_iter] = -1;
    }
  }
}

void Server::Init(const std::string &snapshot_dir, int32_t snapshot_clock,
  int32_t server_id, int32_t resume_clock) {
  for (auto iter = client_bg_map_.begin();
      iter != client_bg_map_.end(); iter++){
    VectorClock vector_clock(iter->second);
    client_vector_clock_map_[iter->first] = vector_clock;

    for (auto bg_iter = iter->second.begin();
	 bg_iter != iter->second.end(); bg_iter++) {
      bg_version_map_[*bg_iter] = -1;
    }
  }

  snapshot_dir_ = snapshot_dir;
  snapshot_clock_ = snapshot_clock;
  server_id_ = server_id;
  resume_clock_ = resume_clock;
}

void Server::CreateTable(int32_t table_id, TableInfo &table_info){
  auto ret = tables_.emplace(table_id, std::move(ServerTable(table_info)));
  CHECK(ret.second);

  VLOG(0) << "Table created!";

  if (resume_clock_ <= 0)
    return;

  boost::unordered_map<int32_t, ServerTable>::iterator table_iter
      = tables_.find(table_id);

  table_iter->second.ReadSnapShot(snapshot_dir_, server_id_, table_id,
    resume_clock_);
}

ServerRow *Server::FindCreateRow(int32_t table_id, int32_t row_id){
  // access ServerTable via reference to avoid copying
  auto iter = tables_.find(table_id);
  CHECK(iter != tables_.end());

  ServerTable &server_table = iter->second;
  ServerRow *server_row = server_table.FindRow(row_id);
  if(server_row != 0)
    return server_row;

  // clock number if not used on server
  RowMetadata metadata;
  server_row = server_table.CreateRow(row_id, metadata);

  return server_row;
}

bool Server::Clock(int32_t client_id, int32_t bg_id) {
  int new_clock = client_vector_clock_map_[client_id].Tick(bg_id);
  VLOG(0) << "Server::Clock(), new_clock = " << new_clock;
  if (new_clock == 0)
    return false;

  new_clock = client_clocks_.Tick(client_id);
  VLOG(0) << "Server::Clock(), client_clocks_, changed = " << new_clock;

  if (new_clock) {
    if (snapshot_clock_ <= 0 || new_clock % snapshot_clock_ != 0)
      return true;
    for (auto table_iter = tables_.begin(); table_iter != tables_.end();
         table_iter++) {
      table_iter->second.TakeSnapShot(snapshot_dir_, server_id_,
        table_iter->first, new_clock);
    }
    return true;
  }

  return false;
}

void Server::AddRowRequest(int32_t bg_id, int32_t table_id, int32_t row_id,
  int32_t clock) {

  ServerRowRequest server_row_request;
  server_row_request.bg_id = bg_id;
  server_row_request.table_id = table_id;
  server_row_request.row_id = row_id;
  server_row_request.clock = clock;

  if (clock_bg_row_requests_.count(clock) == 0) {
    clock_bg_row_requests_.insert(std::make_pair(clock,
      boost::unordered_map<int32_t, std::vector<ServerRowRequest> >()));
  }
  if (clock_bg_row_requests_[clock].count(bg_id) == 0) {
    clock_bg_row_requests_[clock].insert(std::make_pair(bg_id,
      std::vector<ServerRowRequest>()));
  }
  clock_bg_row_requests_[clock][bg_id].push_back(server_row_request);
}

void Server::GetFulfilledRowRequests(std::vector<ServerRowRequest> *requests) {
  int32_t clock = client_clocks_.get_min_clock();
  requests->clear();
  auto iter = clock_bg_row_requests_.find(clock);

  if(iter == clock_bg_row_requests_.end())
    return;
  boost::unordered_map<int32_t,
    std::vector<ServerRowRequest> > &bg_row_requests = iter->second;

  for (auto bg_iter = bg_row_requests.begin(); bg_iter != bg_row_requests.end();
    bg_iter++) {
    requests->insert(requests->end(), bg_iter->second.begin(),
      bg_iter->second.end());
  }

  clock_bg_row_requests_.erase(clock);
}

void Server::ApplyOpLog(const void *oplog, int32_t bg_thread_id,
  uint32_t version) {

  CHECK_EQ(bg_version_map_[bg_thread_id] + 1, version);
  bg_version_map_[bg_thread_id] = version;

  SerializedOpLogReader oplog_reader(oplog);
  bool to_read = oplog_reader.Restart();

  //VLOG(0) << "!!!!!!!!! ApplyOpLog, to_read = " << to_read;
  if(!to_read)
    return;

  int32_t table_id;
  int32_t row_id;
  const int32_t * column_ids; // the variable pointer points to const memory
  int32_t num_updates;
  bool started_new_table;
  const void *updates = oplog_reader.Next(&table_id, &row_id, &column_ids,
    &num_updates, &started_new_table);
  //VLOG(0) << "!!!!!!!!! ApplyOpLog, updates = " << updates;

  ServerTable *server_table;
  if (updates != 0) {
    auto table_iter = tables_.find(table_id);
    server_table = &(table_iter->second);
  }

  while (updates != 0) {
    bool found
      = server_table->ApplyRowOpLog(row_id, column_ids, updates, num_updates);
    VLOG(0) << "Update row_id = " << row_id
	    << " num_updates = " << num_updates;
    if (!found) {
      RowMetadata metadata(client_clocks_.get_min_clock());
      server_table->CreateRow(row_id, metadata);
      server_table->ApplyRowOpLog(row_id, column_ids, updates, num_updates);
    }

    updates = oplog_reader.Next(&table_id, &row_id, &column_ids,
      &num_updates, &started_new_table);
    if(updates == 0)
      break;
    if(started_new_table){
      auto table_iter = tables_.find(table_id);
      server_table = &(table_iter->second);
    }
  }
}

int32_t Server::GetMinClock() {
  return client_clocks_.get_min_clock();
}

int32_t Server::GetBgVersion(int32_t bg_thread_id) {
  return bg_version_map_[bg_thread_id];
}

}  // namespace petuum

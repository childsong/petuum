// author: jinliang

#pragma once
#include "petuum_ps/server/server_row.hpp"
#include "petuum_ps/util/class_register.hpp"
#include "petuum_ps/include/configs.hpp"

#include <string>
#include <boost/unordered_map.hpp>
#include <map>
#include <utility>

namespace petuum {

class ServerTable : boost::noncopyable {
public:
  explicit ServerTable(const TableInfo &table_info):
    table_info_(table_info) {}

  // Move constructor: storage gets other's storage, leaving other
  // in an unspecified but valid state.
  ServerTable(ServerTable && other):
    table_info_(other.table_info_),
    storage_(std::move(other.storage_)) {}

  ServerRow *FindRow(int32_t row_id) {
    auto row_iter = storage_.find(row_id);
    if(row_iter == storage_.end())
      return 0;
    return &(row_iter->second);
  }

  ServerRow *CreateRow(int32_t row_id, RowMetadata &metadata) {
    int32_t row_type = table_info_.row_type;
    AbstractRow *row_data
      = ClassRegistry<AbstractRow>::GetRegistry().CreateObject(row_type);
    row_data->Init(table_info_.row_capacity);
    storage_.insert(std::make_pair(row_id, ServerRow(metadata, row_data)));
    return &(storage_[row_id]);
  }

  bool ApplyRowOpLog(int32_t row_id, const int32_t *column_ids,
    const void *updates, int32_t num_updates){
    auto row_iter = storage_.find(row_id);
    if (row_iter == storage_.end()) {
      VLOG(0) << "Row " << row_id << " is not found!";
      return false;
    }
    row_iter->second.ApplyBatchInc(column_ids, updates, num_updates);
    return true;
  }

  void MakeSnapShotFileName(const std::string &snapshot_dir, int32_t server_id,
    int32_t table_id, int32_t clock, std::string *filename) const;

  void TakeSnapShot(const std::string &snapshot_dir, int32_t server_id,
    int32_t table_id, int32_t clock) const;

  void ReadSnapShot(const std::string &snapshot_dir, int32_t server_id,
    int32_t table_id, int32_t clock);

private:
  TableInfo table_info_;
  boost::unordered_map<int32_t, ServerRow> storage_;
};

}

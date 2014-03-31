// table.hpp
// author: jinliang

#pragma once

#include "petuum_ps/include/row_access.hpp"
#include "petuum_ps/client/client_table.hpp"

#include <boost/utility.hpp>
#include <vector>

namespace petuum {

// UPDATE can be V = {int, double, ...} when the entry is simple numerical
// type. If the entry type is a struct, UPDATE can be id:val pair, like {1:5}
// which increment field 1 of struct by 5.
template<typename UPDATE>
class UpdateBatch : boost::noncopyable {
public:
  UpdateBatch():
    col_ids_(0),
    updates_(0) { }

  ~UpdateBatch() { }

  explicit UpdateBatch(int32_t num_updates):
    col_ids_(num_updates), updates_(num_updates) { }

  void Update(int32_t column_id, const UPDATE& update) {
    col_ids_.push_back(column_id);
    updates_.push_back(update);
  }

  const std::vector<int32_t> &GetColIDs() const {
    return col_ids_;
  }

  const UPDATE* GetUpdates() const {
    return updates_.data();
  }

  int32_t GetBatchSize() const {
    return updates_.size();
  }

private:
  std::vector<int32_t> col_ids_;
  std::vector<UPDATE> updates_;
};

// User table is stores a lightweight pointer to ClientTable.
template<typename UPDATE>
class Table {
public:
  Table():
    system_table_(0){}
  Table(ClientTable* system_table):
    system_table_(system_table){}

  Table(const Table &table):
    system_table_(table.system_table_){}

  Table & operator = (const Table &table){
    system_table_ = table.system_table_;
    return *this;
  }

  // row_accessor helps maintain the reference count to prevent premature
  // cache eviction. The lock
  void Get(int32_t row_id, RowAccessor* row_accessor){
    system_table_->Get(row_id, row_accessor);
  }

  void Inc(int32_t row_id, int32_t column_id, UPDATE update){
    system_table_->Inc(row_id, column_id, &update);
  }

  void BatchInc(int32_t row_id, const UpdateBatch<UPDATE>& update_batch){
    system_table_->BatchInc(row_id, update_batch.GetColIDs().data(),
      update_batch.GetUpdates(), update_batch.GetBatchSize());
  }

  int32_t get_row_type() const {
    return system_table_->get_row_type();
  }

private:
  ClientTable* system_table_;
};
}   // namespace petuum

// author: jinliang

#include "petuum_ps/client/row_metadata.hpp"
#include "petuum_ps/include/abstract_row.hpp"
#include "boost/noncopyable.hpp"

#pragma once

namespace petuum {

// Disallow copy to avoid shared ownership of row_data.
// Allow move sematic for it to be stored in STL containers.
class ServerRow : boost::noncopyable {
public:
  ServerRow() {}
  ServerRow(const RowMetadata& metadata, AbstractRow *row_data):
    metadata_(metadata),
    row_data_(row_data) {}

  ~ServerRow() {
    if(row_data_ != 0)
      delete row_data_;
  }

  ServerRow(ServerRow && other):
    metadata_(other.metadata_),
    row_data_(other.row_data_) {
    other.row_data_ = 0;
  }

  RowMetadata &get_metadata() {
    return metadata_;
  }

  void ApplyBatchInc(const int32_t *column_ids,
    const void *update_batch, int32_t num_updates) {
    row_data_->ApplyBatchIncUnsafe(column_ids, update_batch, num_updates);
  }

  size_t SerializedSize() const {
    return row_data_->SerializedSize();
  }

  size_t Serialize(void *bytes) const {
    return row_data_->Serialize(bytes);
  }

private:
  RowMetadata metadata_;
  AbstractRow *row_data_;
};
}

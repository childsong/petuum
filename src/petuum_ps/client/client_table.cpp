
#include <glog/logging.h>
#include "petuum_ps/client/client_table.hpp"
#include "petuum_ps/util/class_register.hpp"
#include "petuum_ps/consistency/ssp_consistency_controller.hpp"

namespace petuum {

ClientTable::ClientTable(int32_t table_id, const ClientTableConfig &config):
  table_id_(table_id), row_type_(config.table_info.row_type),
  sample_row_(ClassRegistry<AbstractRow>::GetRegistry().CreateObject(
    row_type_)),
  oplog_(table_id, config.oplog_capacity, sample_row_),
  process_storage_(config.process_cache_capacity) {
  if (GlobalContext::get_consistency_model() == SSP) {
    try {
      consistency_controller_ = new SSPConsistencyController(config.table_info,
        table_id, process_storage_, oplog_, sample_row_);
    } catch (std::bad_alloc &e) {
      LOG(FATAL) << "Bad alloc exception";
    }
  } else {
    LOG(FATAL) << "Not yet support consistency model "
      << GlobalContext::get_consistency_model();
  }
}

ClientTable::~ClientTable() {
  delete consistency_controller_;
  delete sample_row_;
}

void ClientTable::Get(int32_t row_id, RowAccessor *row_accessor) {
  return consistency_controller_->Get(row_id, row_accessor);
}

void ClientTable::Inc(int32_t row_id, int32_t column_id, const void *update) {
  return consistency_controller_->Inc(row_id, column_id, update);
}

void ClientTable::BatchInc(int32_t row_id, const int32_t* column_ids,
  const void* updates, int32_t num_updates) {
  return consistency_controller_->BatchInc(row_id, column_ids, updates,
    num_updates);
}

}  // namespace petuum

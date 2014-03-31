// Author: Jinliang

#pragma once

#include <boost/noncopyable.hpp>
#include <map>
#include <vector>
#include <mutex>
#include <utility>

#include "petuum_ps/util/lock.hpp"
#include "petuum_ps/oplog/oplog_partition.hpp"
#include "petuum_ps/thread/context.hpp"

namespace petuum {

class LockGuardedOpLogPartition : boost::noncopyable {
public:

  explicit LockGuardedOpLogPartition(int32_t capacity,
    const AbstractRow *sample_row, int32_t table_id):
    oplog_partition_ptr_(
      new OpLogPartition(capacity, sample_row, table_id)),
    capacity_(capacity),
    sample_row_(sample_row),
    table_id_(table_id){ }

  ~LockGuardedOpLogPartition() {
    if(oplog_partition_ptr_ != 0)
      delete oplog_partition_ptr_;
  }

  LockGuardedOpLogPartition(LockGuardedOpLogPartition &&other) {
    // smtx_ is unlock at construction
    oplog_partition_ptr_ = other.oplog_partition_ptr_;
    other.oplog_partition_ptr_ = 0;
    capacity_ = other.capacity_;
    sample_row_ = other.sample_row_;
    table_id_ = other.table_id_;
  }

  OpLogPartition *Reset() {
    smtx_.lock();
    OpLogPartition *oplog_partition_ptr = oplog_partition_ptr_;
    oplog_partition_ptr_ = new OpLogPartition(capacity_,
      sample_row_, table_id_);
    smtx_.unlock();
    return oplog_partition_ptr;
  }

  void Inc(int32_t row_id, int32_t column_id, const void *delta) {
    smtx_.lock_shared();
    oplog_partition_ptr_->Inc(row_id, column_id, delta);
    smtx_.unlock();
  }

  void BatchInc(int32_t row_id, const int32_t *column_ids, const void *deltas,
    int32_t num_updates) {
    smtx_.lock_shared();
    oplog_partition_ptr_->BatchInc(row_id, column_ids, deltas,
      num_updates);
    smtx_.unlock();
  }

  // Search for the OpLog of a row. Return false if not found.
  bool FindOpLog(int32_t row_id, OpLogAccessor *oplog_accessor) {
    smtx_.lock_shared();
    bool found = oplog_partition_ptr_->FindOpLog(row_id, oplog_accessor);
    if (found) {
      // oplog_accessor is responsible for releasing the lock
      oplog_accessor->SetPartitionLock(&smtx_);
      return true;
    }
    smtx_.unlock();
    return false;
  }

  // Search for the OpLog of a row. Insert an empty one if not found.
  void FindInsertOpLog(int32_t row_id, OpLogAccessor *oplog_accessor) {
    smtx_.lock_shared();
    oplog_partition_ptr_->FindInsertOpLog(row_id, oplog_accessor);
    oplog_accessor->SetPartitionLock(&smtx_);
  }

private:
  // The smtx_ shared lock guarantees the exclusive access to oplog_partion_ptr_
  SharedMutex smtx_;
  OpLogPartition *oplog_partition_ptr_;
  int32_t capacity_;
  const AbstractRow *sample_row_;
  int32_t table_id_;
};

// The OpLog storage for parameter server.  OpLogs are stored by table. Access
// to different tables are concurrent and lock-free.  Within each table,
// OpLogs are partitioned into N OpLogPartition (N is the number of bg
// threads). Accesses to different partitions are concurrent and lock-free;
// accesses to the same partition are guarded by a shared lock.  Accesses that
// may replace that OpLog partition with a new one are required to acquire the
// exclusive lock. Accesses that only modify or read the OpLogs are only
// required to acquire the shared lock.

// Relies on GlobalContext to be properly initialized.

// OpLogs for a particular table.
class TableOpLog : boost::noncopyable {
public:
  TableOpLog(int32_t table_id, int32_t partitioned_oplog_capacity,
    const AbstractRow *sample_row):
    table_id_(table_id),
    oplog_partitions_() {
      for (int32_t i = 0; i < GlobalContext::get_num_bg_threads(); ++i) {
        oplog_partitions_.emplace_back(partitioned_oplog_capacity, 
          sample_row, table_id);
      }
    }

  ~TableOpLog() { }

  // Once created, all accesses are concurrent.
  OpLogPartition *ResetOpLogPartition(int32_t partition_num) {
    return oplog_partitions_[partition_num].Reset();
  }

  void Inc(int32_t row_id, int32_t column_id, const void *delta) {
    int32_t partition_num = GlobalContext::GetBgPartitionNum(table_id_, row_id);
    oplog_partitions_[partition_num].Inc(row_id, column_id, delta);
  }

  void BatchInc(int32_t row_id, const int32_t *column_ids, const void *deltas,
    int32_t num_updates) {
    int32_t partition_num = GlobalContext::GetBgPartitionNum(table_id_, row_id);
    oplog_partitions_[partition_num].BatchInc(row_id, column_ids, deltas,
      num_updates);
  }

  bool FindOpLog(int32_t row_id, OpLogAccessor *oplog_accessor) {
    int32_t partition_num = GlobalContext::GetBgPartitionNum(table_id_, row_id);
    return oplog_partitions_[partition_num].FindOpLog(row_id, oplog_accessor);
  }

  void FindInsertOpLog(int32_t row_id, OpLogAccessor *oplog_accessor) {
    int32_t partition_num = GlobalContext::GetBgPartitionNum(table_id_, row_id);
    oplog_partitions_[partition_num].FindInsertOpLog(row_id, oplog_accessor);
  }

private:
  int32_t table_id_;
  std::vector<LockGuardedOpLogPartition> oplog_partitions_;
  
};

}   // namespace petuum

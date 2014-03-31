#pragma once

#include "petuum_ps/include/abstract_row.hpp"
#include "petuum_ps/client/client_row.hpp"
#include <boost/utility.hpp>
#include <vector>
#include <utility>

namespace petuum {

class ProcessStorage;

// RowAccessor is a "smart pointer" for ROW: row_accessor.Get() gives a const
// reference.  We disallow copying of RowAccessor to avoid unnecessary
// manipulation of reference count.
class RowAccessor : boost::noncopyable {
public:
  RowAccessor() : client_row_ptr_(0), row_data_ptr_(0) { }

  ~RowAccessor() {
    if (client_row_ptr_ != 0) {
      client_row_ptr_->DecRef();
    }
  }

  // The returned reference is guaranteed to be valid only during the
  // lifetime of this RowAccessor.
  template<typename ROW>
  inline const ROW& Get() {
    return *(dynamic_cast<ROW*>(row_data_ptr_.get()));
  }

private:
  friend class ProcessStorage;
  friend class BgWorkers;
  friend class SSPConsistencyController;

  // Does not take ownership of client_row_ptr. Note that accesses to
  // ClientRow::SwapAndDestroy() and ClientRow::GetRowDataPtr() must be
  // mutually exclusive as the former modifies ClientRow::row_data_pptr_.
  inline void SetClientRow(ClientRow* client_row_ptr) {
    client_row_ptr_ = client_row_ptr;
    client_row_ptr_->IncRef();
    client_row_ptr_->GetRowDataPtr(&row_data_ptr_);
  }

  // Return client row which will stay alive throughout the lifetime of
  // RowAccessor.
  inline const ClientRow* GetClientRow() {
    return client_row_ptr_;
  }

  AbstractRow *GetAbstractRow() {
    return row_data_ptr_.get();
  }

  ClientRow* client_row_ptr_;

  std::shared_ptr<AbstractRow> row_data_ptr_;
};
}  // namespace petuum

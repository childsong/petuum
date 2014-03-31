
#include "petuum_ps/include/abstract_row.hpp"
#include "petuum_ps/client/client_row.hpp"
#include <memory>

namespace petuum {

ClientRow::ClientRow(const RowMetadata &metadata, AbstractRow* row_data) : 
  num_refs_(0),
  metadata_(metadata),
  row_data_pptr_(new std::shared_ptr<AbstractRow>) {
  (*row_data_pptr_).reset(row_data);
}

}  // namespace petuum

#pragma once

#include <map>
#include <boost/noncopyable.hpp>

#include "petuum_ps/oplog/oplog_partition.hpp"

namespace petuum {

class BgOpLog : boost::noncopyable {
public:
  BgOpLog(){}

  ~BgOpLog(){
    for(auto iter = table_oplog_map_.begin(); iter != table_oplog_map_.end();
      iter++){
      delete iter->second;
    }
  }
  // takes ownership of the oplog partition
  void Add(int32_t table_id, OpLogPartition *oplog_partition_ptr){
    table_oplog_map_[table_id] = oplog_partition_ptr;
  }

  OpLogPartition* Get(int32_t table_id){
    return table_oplog_map_[table_id];
  }
private:
  std::map<int32_t, OpLogPartition*> table_oplog_map_;

};

}

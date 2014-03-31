#pragma once

#include <pthread.h>
#include <map>
#include <vector>

#include "petuum_ps/include/configs.hpp"
#include "petuum_ps/client/client_table.hpp"
#include "petuum_ps/thread/context.hpp"
#include "petuum_ps/thread/ps_msgs.hpp"
#include "petuum_ps/oplog/oplog_partition.hpp"
#include "petuum_ps/thread/bg_oplog.hpp"
#include "petuum_ps/comm_bus/comm_bus.hpp"
#include "petuum_ps/thread/row_request_mgr.hpp"
namespace petuum {

// Relies on GlobalContext being properly initalized.

class BgWorkers {
public:
  static void Init(int32_t id_st, std::map<int32_t, ClientTable* > *tables_);

  static void ShutDown();

  static void ThreadRegister();
  static void ThreadDeregister();

  // Assuming table does not yet exist
  static bool CreateTable(int32_t table_id,
      const ClientTableConfig& table_config);
  static void WaitCreateTable();
  static bool RequestRow(int32_t table_id, int32_t row_id, int32_t clock);
  static void ClockAllTables();
  static void SendOpLogsAllTables();

private:
  
  struct BgContext {
  public:
    uint32_t version; // version of the data, increment when a set of OpLogs 
                       // are sent out; may wrap around
                       // More specifically, version denotes the version of the 
                       // OpLogs that haven't been sent out.
    RowRequestMgr row_request_mgr;
    
    // initialized by BgThreadMain(), used in CreateSendOpLogs()
    // For server x, table y, the size of serialized OpLog is ...
    std::map<int32_t, std::map<int32_t, size_t> > server_table_oplog_size_map;
    // The OpLog msg to each server
    std::map<int32_t, ClientSendOpLogMsg* > server_oplog_msg_map;
    // map server id to oplog msg size
    std::map<int32_t, size_t> server_oplog_msg_size_map;
    // size of oplog per table, reused across multiple tables
    std::map<int32_t, size_t> table_server_oplog_size_map;
  };

  static void *BgThreadMain(void *thread_id);
  
  /* Helper functions*/
  /* Communication functions */
  static void ConnectToNameNodeOrServer(int32_t server_id);
  static void ConnectToBg(int32_t bg_id);
  static void SendToAllLocalBgThreads(void *msg, int32_t size);   
  
  static void HandleCreateTables();
  static void BgServerHandshake();

  /* Operate on thread specific BgContext*/
  static void CheckForwardRowRequestToServer(int32_t app_thread_id, 
    RowRequestMsg &row_request_msg);
  static void HandleServerRowRequestReply(
    ServerRowRequestReplyMsg &server_row_request_reply_msg);
  
  static void CreateSendOpLogs(BgOpLog *bg_oplog, bool is_clock);
  static void ShutDownClean();
  static std::vector<pthread_t> threads_;
  static std::vector<int32_t> thread_ids_; 
  static std::map<int32_t, ClientTable* > *tables_;
  static int32_t id_st_;

  static pthread_barrier_t init_barrier_;
  static pthread_barrier_t create_table_barrier_;

  static boost::thread_specific_ptr<BgContext> bg_context_;
  static CommBus *comm_bus_;

  static CommBus::RecvFunc CommBusRecvAny;
  static CommBus::RecvTimeOutFunc CommBusRecvTimeOutAny;
  static CommBus::SendFunc CommBusSendAny;
};

}  // namespace petuum

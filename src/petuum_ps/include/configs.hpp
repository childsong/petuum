#pragma once

#include <inttypes.h>
#include <stdint.h>
#include <map>
#include <vector>

#include "petuum_ps/include/host_info.hpp"

namespace petuum {

enum ConsistencyModel {
  // Stale synchronous parallel.
  SSP = 0,

  // Value-bound (between pair-wise processes) Asynchronous parallel.
  VAP = 1,

  // Value-bound + SSP.
  // TODO(wdai): Week or strong VAP?
  ClockVAP = 2
};

struct TableGroupConfig {

  TableGroupConfig():
  aggressive_clock(false) { }

  // ================= Global Parameters ===================
  // Global parameters have to be the same across all processes.

  // Total number of servers in the system.
  int32_t num_total_server_threads;

  // Total number of tables the PS will have. Each init thread must make
  // num_tables CreateTable() calls.
  int32_t num_tables;

  // Total number of clients in the system.
  int32_t num_total_clients;

  // Number of total background worker threads in the system.
  int32_t num_total_bg_threads;

  // ===================== Local Parameters ===================
  // Local parameters can differ between processes, but have to sum up to global
  // parameters.

  // Number of local server threads.
  int32_t num_local_server_threads;

  // Number of local applications threads, including init thread.
  int32_t num_local_app_threads;

  // Number of local background worker threads.
  int32_t num_local_bg_threads;

  // IDs of all servers.
  std::vector<int32_t> server_ids;

  // mapping server ID to host info.
  std::map<int32_t, HostInfo> host_map;

  // My client id.
  int32_t client_id;

  // Each thread within the process (including app, server, and bg threads) is
  // assigned a globally unique ID. It is required that the IDs of threads in
  // the same process are in a contiguous range [local_id_min, local_id_max],
  // inclusive.
  int32_t local_id_min;
  int32_t local_id_max;

  // If set to true, oplog send is triggered on every Clock() call.
  // If set to false, oplog is only sent if the process clock (representing all
  // app threads) has advanced.
  // Aggressive clock may reduce memory footprint and improve the per-clock
  // convergence rate in the cost of performance.
  // Default is false (suggested).
  bool aggressive_clock;

  ConsistencyModel consistency_model;

  // In Async+pushing,
  int32_t server_ring_size;

  int32_t snapshot_clock;
  int32_t resume_clock;
  std::string snapshot_dir;
};

// TableInfo is shared between client and server.
struct TableInfo {
  // table_staleness is used for SSP and ClockVAP.
  int32_t table_staleness;

  // A table can only have one type of row. The row_type is defined when
  // calling TableGroup::RegisterRow().
  int32_t row_type;

  // row_capacity can mean different thing for different row_type. For example
  // in vector-backed dense row it is the max number of columns. This
  // parameter is ignored for sparse row.
  int32_t row_capacity;
};

// ClientTableConfig is used by client only.
struct ClientTableConfig {
  TableInfo table_info;

  // In # of rows.
  int32_t process_cache_capacity;

  // In # of rows.
  int32_t thread_cache_capacity;

  // TODO(wdai)
  int32_t oplog_capacity;
};

}  // namespace petuum

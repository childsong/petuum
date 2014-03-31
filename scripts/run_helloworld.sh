#!/usr/bin/env bash

if [ $# -ne 7 ]; then
  echo ""
  echo "Usage: $0 <host_list> <server_list> <client_worker_threads> <staleness> <snapshot_dir> <snapshot_clock> <resume_clock>"
  echo ""
  echo "<host_list>: a file that contains of list of host IPs with 1 IP per line, such as"
  echo "192.168.1.1"
  echo "192.168.1.2"
  echo "..."
  echo "Each host will run one helloworld process."
  echo ""
  echo "<server_file>: the server file, as explained in the manual"
  echo ""
  echo "<client_worker_threads>: How many client worker threads to spawn per machine, at least 1"
  echo ""
  echo "<staleness>: SSP staleness setting; set to 0 for Bulk Synchronous Parallel mode."
  echo ""
  echo "<snapshot_dir>: directory to store snapshots, as explained in the manual"
  echo ""
  echo "<snapshot_clock>: how frequent a snapshot is taken, as explained in the manual"
  echo ""
  echo "<resume_clock>: from which snapshot to recover, as explained in the manual; set to 0 if not for recovery"
  exit
fi


script_path=`readlink -f $0`
script_dir=`dirname $script_path`
project_root=`dirname $script_dir`

prog="${project_root}/apps/helloworld/bin/helloworld"
hostfile=$(readlink -f $1)
serverfile=$(readlink -f $2)
snapshot_dir=$(readlink -f $5)

echo $prog
echo $hostfile
echo $serverfile

num_server_threads_per_client=1
num_app_threads_per_client=$3
num_bg_threads_per_client=1

# global config
num_total_clients=1
num_total_server_threads=$(( num_total_clients*num_server_threads_per_client ))
num_total_bg_threads=$(( num_total_clients*num_bg_threads_per_client ))

# local config
local_server_thread_id_start=1
local_id_min=0
local_id_max=$(( local_id_min+num_server_threads_per_client\
    +num_bg_threads_per_client+num_app_threads_per_client ))

# application config
num_tables=1
num_iterations=10
staleness=0

snapshot_clock=$6
resume_clock=$7

host_list=`cat $hostfile | awk '{ print $1 }'`
host_arr=($host_list)
host_arr=()
for host in $host_list; do
    host_arr=("${host_arr[@]}" $host)
done

num_hosts=${#host_arr[@]}
num_total_clients=$num_hosts

ssh_options="-oStrictHostKeyChecking=no -oUserKnownHostsFile=/dev/null"

ssh $ssh_options ${host_arr[0]} mkdir -p ${snapshot_dir};
GLOG_logtostderr=true GLOG_v=-1 GLOG_minloglevel=4 \
    $prog \
    --num_total_server_threads $num_total_server_threads \
    --num_total_bg_threads $num_total_bg_threads \
    --num_total_clients $num_total_clients \
    --num_tables $num_tables \
    --num_server_threads $num_server_threads_per_client \
    --num_app_threads $num_app_threads_per_client \
    --num_bg_threads $num_bg_threads_per_client \
    --local_id_min $local_id_min \
    --local_id_max $local_id_max \
    --client_id 0 \
    --hostfile ${serverfile} \
    --num_iterations $num_iterations \
    --staleness $staleness \
    --snapshot_clock $snapshot_clock \
    --resume_clock $resume_clock \
    --snapshot_dir ${snapshot_dir} &

if [ $num_total_clients -eq 1 ]; then
    exit 0
fi

sleep 5s

client_id=1
max_client_id=$(( num_total_clients-1 ))
for client_id in `seq 1 $max_client_id`; do
    local_id_min=$(( local_id_max+1 ))
    local_id_max=$(( local_id_min+num_server_threads_per_client\
	+num_bg_threads_per_client+num_app_threads_per_client-1 ))

    ssh $ssh_options ${host_arr[$client_id]} mkdir -p ${snapshot_dir};
    GLOG_logtostderr=true GLOG_v=-1 GLOG_minloglevel=4 \
	$prog \
	--num_total_server_threads $num_total_server_threads \
	--num_total_bg_threads $num_total_bg_threads \
	--num_total_clients $num_total_clients \
	--num_tables $num_tables \
	--num_server_threads $num_server_threads_per_client \
	--num_app_threads $num_app_threads_per_client \
	--num_bg_threads $num_bg_threads_per_client \
	--local_id_min $local_id_min \
	--local_id_max $local_id_max \
	--client_id ${client_id} \
	--hostfile ${serverfile} \
	--num_iterations $num_iterations \
	--staleness $staleness \
        --snapshot_clock $snapshot_clock \
        --resume_clock $resume_clock \
        --snapshot_dir ${snapshot_dir} &
done
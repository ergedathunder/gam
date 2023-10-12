#! /usr/bin/env bash
bin=`dirname "$0"`
bin=`cd "$bin"; pwd`
SRC_HOME=$bin
SRC_HOME_155="/home/xeg/last/gam/pagerank"
SRC_HOME_141="/home/xeg/last/gam/pagerank"
SRC_HOME_158="/home/xeg/last/gam/pagerank"
SRC_HOME_159="/home/xeg/last/gam/pagerank"
SRC_HOME_160="/home/xeg/last/gam/pagerank"

slaves=$bin/slaves
log_file=$bin/log
log_file_155=$SRC_HOME_155/log
log_file_141=$SRC_HOME_141/log
log_file_158=$SRC_HOME_158/log
log_file_159=$SRC_HOME_159/log
log_file_160=$SRC_HOME_160/log


master_ip=10.77.110.155
master_port=5732


run() {
    no_array=$1
    no_node=$2
    no_worker=$3
    no_run=$4
    
    node_all=5
    is_read=1
    is_sync=0
    see_time=1
    sleep_time=1
    no_steps=10
    old_IFS=$IFS
    IFS=$'\n'
    i=0
    for slave in `cat "$slaves"`
    do
        ip=`echo $slave | cut -d ' ' -f1`
        port=`echo $slave | cut -d ' ' -f2`
        node_id=$((i + 1))
        if [ $i = 0 ]; then
            is_master=1
            master_ip=$ip
        else
            is_master=0
        fi
        if [ $port == $ip ]; then
            port=1234
        fi
        
        if [ $i = 0 ]; then
            ssh $ip "$SRC_HOME/pagerank --no_node $no_node --no_worker $no_worker --no_array $no_array --node_id $node_id --ip_master $master_ip --ip_worker $ip --port_worker $port --is_master $is_master --port_master $master_port --no_run $no_run --is_read $is_read  | tee -a '$log_file'.$port" &
            echo "$SRC_HOME/pagerank --no_node $no_node --no_array $no_array --ip_master $master_ip --ip_worker $ip --port_worker $port --is_master $is_master --port_master $master_port --no_run $no_run --is_read $is_read"
            
            elif [ $i = 1 ]; then
            ssh xeg@$ip "$SRC_HOME_158/pagerank --no_node $no_node --no_worker $no_worker --no_array $no_array --node_id $node_id --ip_master $master_ip --ip_worker $ip --port_worker $port --is_master $is_master --port_master $master_port --no_run $no_run --is_read $is_read  | tee -a '$log_file_158'.$port" &
            echo "$SRC_HOME_158/pagerank --no_node $no_node --no_array $no_array --ip_master $master_ip --ip_worker $ip --port_worker $port --is_master $is_master --port_master $master_port --no_run $no_run --is_read $is_read"
            
            elif [ $i = 2 ]; then
            ssh xeg@$ip "$SRC_HOME_159/pagerank --no_node $no_node --no_worker $no_worker --no_array $no_array --node_id $node_id --ip_master $master_ip --ip_worker $ip --port_worker $port --is_master $is_master --port_master $master_port --no_run $no_run --is_read $is_read  | tee -a '$log_file_159'.$port" &
            echo "$SRC_HOME_159/pagerank --no_node $no_node --no_array $no_array --ip_master $master_ip --ip_worker $ip --port_worker $port --is_master $is_master --port_master $master_port --no_run $no_run --is_read $is_read"
            
        else
            ssh xeg@$ip "$SRC_HOME_160/pagerank --no_node $no_node --no_worker $no_worker --no_array $no_array --node_id $node_id --ip_master $master_ip --ip_worker $ip --port_worker $port --is_master $is_master --port_master $master_port --no_run $no_run --is_read $is_read  | tee -a '$log_file_160'.$port" &
            echo "$SRC_HOME_160/pagerank --no_node $no_node --no_array $no_array --ip_master $master_ip --ip_worker $ip --port_worker $port --is_master $is_master --port_master $master_port --no_run $no_run --is_read $is_read"
            
        fi
        sleep 1
        i=$((i+1))
        if [ "$i" = "$no_node" ]; then
            break
        fi
    done # for slave
    wait
    
    IFS="$old_IFS"
}

run_10(){
    # 对10次时间求和
    count=10
    total=0.0
    for ((i=1; i<=count; i++)); do
        echo "i=$i"
        runtime=$(run $1 $2 $3 $4 | grep "running time : " | awk '{print $4}')
        echo "run time of i=$i: $runtime"
        total=$(echo "$total + $runtime" | bc)
        echo "i=$i"
        
    done
    echo "total run time of $1 $2 $3 $4: $total"
}

run 256 4 8 2

# echo "" > output.txt

# choice_no_array=(50000)
# choice_node=(4)
# choice_worker=(3 2 1)
# choice_no_run=(1 2)
# choice_count=(1 2 3 4 5)
# for no_array in ${choice_no_array[@]}
# do
#     for no_worker in ${choice_worker[@]}
#     do
#         for no_node in ${choice_node[@]}
#         do
#             if [ $no_worker -lt $no_node ]; then
#                 no_node=$no_worker
#             else
#                 no_node=4
#             fi
#             for no_run in ${choice_no_run[@]}
#             do
#                 sleep 1
#                 file_prefix="no_array_"$no_array"_no_node_"$no_node"_no_worker_"$no_worker"_no_run_"$no_run
#                 echo "Running with no_array=$no_array, no_node=$no_node, no_worker=$no_worker, no_run=$no_run"
#                 echo "File Prefix: $file_prefix"
                
#                 mkdir -p data0924/$file_prefix
                
#                 for no_iterate in ${choice_count[@]}
#                 do
#                     echo "no_iterate=$no_iterate"
#                     # 运行命令，并将输出追加到文件
#                     echo "" > data0924/$file_prefix/$no_iterate.txt
#                     run $no_array $no_node $no_worker $no_run >> data0924/$file_prefix/$no_iterate.txt
#                 done
                
#             done
#         done
#     done
# done
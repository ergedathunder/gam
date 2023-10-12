#! /usr/bin/env bash
bin=`dirname "$0"`
bin=`cd "$bin"; pwd`
SRC_HOME=$bin
SRC_HOME_155="/home/wpq/gam/nbody_gam_parr"
SRC_HOME_158="/home/wpq/rdmatest/gam/nbody_gam_parr"
SRC_HOME_159="/home/wpq/gam/nbody_gam_parr"
SRC_HOME_160="/home/wpq/gam/nbody_gam_parr"

slaves=$bin/slaves
log_file=$bin/log
log_file_155=$SRC_HOME_155/log
log_file_158=$SRC_HOME_158/log
log_file_159=$SRC_HOME_159/log
log_file_160=$SRC_HOME_160/log


master_ip=10.77.110.155
master_port=90227

run() {
    no_particle=$1
    node=$2
    no_worker=$3
    no_run=$4
    
    node_id=0
    
    is_read=1
    no_steps=5
    
    old_IFS=$IFS
    IFS=$'\n'
    i=0
    for slave in `cat "$slaves"`
    do
        ip=`echo $slave | cut -d ' ' -f1`
        port=`echo $slave | cut -d ' ' -f2`
        node_id=$((i + 1))
        echo "node_id=$node_id"
        if [ $i = 0 ]; then
            is_master=1
            master_ip=$ip
        else
            is_master=0
        fi
        if [ $port == $ip ]; then
            port=1234
        fi
        
        if [ $i -eq 0 ]; then
            ssh  $ip "$SRC_HOME/nbody --no_node $node --no_worker $no_worker --node_id $node_id --ip_master $master_ip --ip_worker $ip --port_worker $port --is_master $is_master --port_master $master_port --no_run $no_run --is_read $is_read --is_sync $is_sync --see_time $see_time --no_particle $no_particle --no_steps  $no_steps | tee -a '$log_file'.$port" &
            elif [ $i -eq 1 ]; then
            
            ssh  wpq@$ip "$SRC_HOME_158/nbody --no_node $node --no_worker $no_worker --node_id $node_id --ip_master $master_ip --ip_worker $ip --port_worker $port --is_master $is_master --port_master $master_port --no_run $no_run --is_read $is_read --is_sync $is_sync --see_time $see_time --no_particle $no_particle --no_steps $no_steps | tee -a '$log_file_158'.$port" &
            elif [ $i -eq 2 ]; then
            
            ssh  wpq@$ip "$SRC_HOME_159/nbody --no_node $node --no_worker $no_worker --node_id $node_id --ip_master $master_ip --ip_worker $ip --port_worker $port --is_master $is_master --port_master $master_port --no_run $no_run --is_read $is_read --is_sync $is_sync --see_time $see_time --no_particle $no_particle --no_steps $no_steps | tee -a '$log_file_159'.$port" &
            
        else
            ssh  wpq@$ip "$SRC_HOME_160/nbody --no_node $node --no_worker $no_worker --node_id $node_id --ip_master $master_ip --ip_worker $ip --port_worker $port --is_master $is_master --port_master $master_port --no_run $no_run --is_read $is_read --is_sync $is_sync --see_time $see_time --no_particle $no_particle --no_steps $no_steps | tee -a '$log_file_160'.$port" &
            
        fi
        
        sleep 1
        i=$((i+1))
        if [ "$i" = "$node" ]; then
            break
        fi
    done # for slave
    wait
    
    IFS="$old_IFS"
}

clean() {
    node_clean=4
    old_IFS=$IFS
    IFS=$'\n'
    i=0
    for slave in `cat "$slaves"`
    do
        ip=`echo $slave | cut -d ' ' -f1`
        port=`echo $slave | cut -d ' ' -f2`
        
        if [ $i = 0 ]; then
            ssh $ip "pkill nbody" &
            elif [ $i = 1 ]; then
            
            ssh wpq@$ip "pkill nbody" &
            elif [ $i = 2 ]; then
            
            ssh wpq@$ip "pkill nbody" &
            
        else
            ssh wpq@$ip "pkill nbody" &
            
        fi
        sleep 1
        i=$((i+1))
        if [ "$i" = "$node_clean" ]; then
            break
        fi
    done # for slave
    wait
    
    IFS="$old_IFS"
}



run_10(){
    count=10
    total=0.0
    for ((i=1; i<=count; i++)); do
        runtime=$(run $1 $2 $3 $4 | grep "Run time: " | awk '{print $4}')
        # echo "run time of i=$i: $runtime"
        total=$(echo "$total + $runtime" | bc)
        
    done
    echo "total run time of $1 $2 $3 $4: $total"
}

clean
echo "" > output.txt

choice_nbody_num=(100)
choice_node=(4)
choice_worker=(1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16)
choice_no_run=(1 2)
# 1-100
choice_count=(1 2 3 4 5 6 7 8 9 10)


# for no_array in ${choice_nbody_num[@]}
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

#                 mkdir -p data0928/$file_prefix

#                 for no_iterate in ${choice_count[@]}
#                 do
#                     echo "no_iterate=$no_iterate"
#                     # 运行命令，并将输出追加到文件
#                     echo > data0928/$file_prefix/$no_iterate.txt
#                     run $no_array $no_node $no_worker $no_run >> data0928/$file_prefix/$no_iterate.txt
#                 done

#             done
#         done
#     done
# done

run 100 1 1 1 >> output.txt

# ./nbody --no_node 1 --no_worker 1 --node_id 1 --ip_master 10.77.110.155 --ip_worker 10.77.110.155 --port_worker 23456 --is_master 1 --no_run 1 --port_master 12345 --no_run 1 --is_read 1 --no_particle 100 --no_steps 5
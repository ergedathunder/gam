#! /usr/bin/env bash
bin=`dirname "$0"`
bin=`cd "$bin"; pwd`
SRC_HOME=$bin
SRC_HOME_155="/home/xeg/last/gam/test_alloc"
SRC_HOME_141="/home/xeg/last/gam/test_alloc"
SRC_HOME_158="/home/xeg/last/gam/test_alloc"
SRC_HOME_159="/home/xeg/last/gam/test_alloc"
SRC_HOME_160="/home/xeg/last/gam/test_alloc"

slaves=$bin/slaves
log_file=$bin/log
log_file_155=$SRC_HOME_155/log
log_file_141=$SRC_HOME_141/log
log_file_158=$SRC_HOME_158/log
log_file_159=$SRC_HOME_159/log
log_file_160=$SRC_HOME_160/log


master_ip=10.77.110.155
master_port=6666
son_file="test_alloc"


run() {
    xeg=1
    no_array=$1
    no_node=$2
    no_run=$3
    
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
            ssh $ip "$SRC_HOME/$son_file --no_node $no_node --no_array $no_array --ip_master $master_ip --ip_worker $ip --port_worker $port --is_master $is_master --port_master $master_port --no_run $no_run --is_read $is_read  | tee '$log_file'.$port" &
            echo "$SRC_HOME/$son_file --no_node $no_node --no_array $no_array --ip_master $master_ip --ip_worker $ip --port_worker $port --is_master $is_master --port_master $master_port --no_run $no_run --is_read $is_read"
                        
            elif [ $i = 1 ]; then
            ssh xeg@$ip "$SRC_HOME_158/$son_file --no_node $no_node --no_array $no_array --ip_master $master_ip --ip_worker $ip --port_worker $port --is_master $is_master --port_master $master_port --no_run $no_run --is_read $is_read  | tee '$log_file_158'.$port" &
            echo "$SRC_HOME_158/$son_file --no_node $no_node --no_array $no_array --ip_master $master_ip --ip_worker $ip --port_worker $port --is_master $is_master --port_master $master_port --no_run $no_run --is_read $is_read"
            
            elif [ $i = 2 ]; then
            ssh xeg@$ip "$SRC_HOME_159/$son_file --no_node $no_node --no_array $no_array --ip_master $master_ip --ip_worker $ip --port_worker $port --is_master $is_master --port_master $master_port --no_run $no_run --is_read $is_read  | tee '$log_file_159'.$port" &
            echo "$SRC_HOME_159/$son_file --no_node $no_node --no_array $no_array --ip_master $master_ip --ip_worker $ip --port_worker $port --is_master $is_master --port_master $master_port --no_run $no_run --is_read $is_read"
            
        else
            ssh xeg@$ip "$SRC_HOME_160/$son_file --no_node $no_node --no_array $no_array --ip_master $master_ip --ip_worker $ip --port_worker $port --is_master $is_master --port_master $master_port --no_run $no_run --is_read $is_read  | tee '$log_file_160'.$port" &
            echo "$SRC_HOME_160/$son_file --no_node $no_node --no_array $no_array --ip_master $master_ip --ip_worker $ip --port_worker $port --is_master $is_master --port_master $master_port --no_run $no_run --is_read $is_read"
            
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
    xeg=10
    echo "xeg = $xeg"
    count=1
    total_msi=0.0
    total_ws=0.0
    for ((i=1; i<=count; i++)); do
        echo "round $i start"
        run $1 $2 $3
        # runtime=$(run $1 $2 $3 | grep "running time" | awk '{print $4}')
        # echo "Msi run time of round i=$i: $runtime"
        # total_msi=$(echo "$total_msi + $runtime" | bc)
        # sleep 1

        # runtime=$(run $1 $2 $4 | grep "running time" | awk '{print $4}')
        # echo "Write_shared run time of round i=$i: $runtime"
        # total_ws=$(echo "$total_ws + $runtime" | bc)
        echo "round $i end"
        sleep 1
        
    done
    echo "total msi run time of $1 $2 $3: $total_msi"
    echo "total write_shared run time of $1 $2 $3: $total_ws"
    echo "xeg = $xeg"
}

run 256 2 12


# choice_no_array=(2048 4096 8192)
# choice_node=(1 2 3 4 5)
# choice_no_run=(1 2)

# ./make.sh

# echo "" > output.txt

# clean

# for no_array in ${choice_no_array[@]}
# do
#     for no_node in ${choice_node[@]}
#     do
#         for no_run in ${choice_no_run[@]}
#         do
#             sleep 1
#             file_prefix="no_array_"$no_array"_no_node_"$no_node"_no_run_"$no_run
#             echo $file_prefix
#             echo "" > data/$file_prefix.txt
#             run $no_array $no_node $no_run >> data/$file_prefix.txt
#         done
#     done
# done

# run 8192 5 2 >> output.txt

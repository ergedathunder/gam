#! /usr/bin/env bash
bin=`dirname "$0"`
bin=`cd "$bin"; pwd`
SRC_HOME=$bin
SRC_HOME_155="/home/wpq/gam/fft_gam_parr"
SRC_HOME_141="/home/wupuqing/gam/fft_gam_parr"
SRC_HOME_158="/home/wpq/rdmatest/gam/fft_gam_parr"
SRC_HOME_159="/home/wpq/gam/fft_gam_parr"
SRC_HOME_160="/home/wpq/gam/fft_gam_parr"

slaves=$bin/slaves
log_file=$bin/log
log_file_155=$SRC_HOME_155/log
log_file_141=$SRC_HOME_141/log
log_file_158=$SRC_HOME_158/log
log_file_159=$SRC_HOME_159/log
log_file_160=$SRC_HOME_160/log


master_ip=10.77.110.155
master_port=7237


run() {
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
            ssh $ip "$SRC_HOME/fft --no_node $no_node --no_array $no_array --ip_master $master_ip --ip_worker $ip --port_worker $port --is_master $is_master --port_master $master_port --no_run $no_run --is_read $is_read  | tee -a '$log_file'.$port" &
            
            elif [ $i = 1 ]; then
            ssh -p 5102 wupuqing@$ip "$SRC_HOME_141/fft --no_node $no_node --no_array $no_array --ip_master $master_ip --ip_worker $ip --port_worker $port --is_master $is_master --port_master $master_port --no_run $no_run --is_read $is_read  | tee -a '$log_file_141'.$port" &
            
            elif [ $i = 2 ]; then
            ssh wpq@$ip "$SRC_HOME_158/fft --no_node $no_node --no_array $no_array --ip_master $master_ip --ip_worker $ip --port_worker $port --is_master $is_master --port_master $master_port --no_run $no_run --is_read $is_read  | tee -a '$log_file_158'.$port" &
            
            elif [ $i = 3 ]; then
            ssh wpq@$ip "$SRC_HOME_159/fft --no_node $no_node --no_array $no_array --ip_master $master_ip --ip_worker $ip --port_worker $port --is_master $is_master --port_master $master_port --no_run $no_run --is_read $is_read  | tee -a '$log_file_159'.$port" &
            
        else
            ssh wpq@$ip "$SRC_HOME_160/fft --no_node $no_node --no_array $no_array --ip_master $master_ip --ip_worker $ip --port_worker $port --is_master $is_master --port_master $master_port --no_run $no_run --is_read $is_read  | tee -a '$log_file_160'.$port" &
            
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

clean() {
    old_IFS=$IFS
    IFS=$'\n'
    i=0
    for slave in `cat "$slaves"`
    do
        ip=`echo $slave | cut -d ' ' -f1`
        port=`echo $slave | cut -d ' ' -f2`
        
        if [ $i = 0 ]; then
            ssh $ip "pkill fft" &
            elif [ $i = 1 ]; then
            
            ssh -p 5102 wupuqing@$ip "pkill fft" &
            elif [ $i = 2 ]; then
            
            ssh wpq@$ip "pkill fft" &
            elif [ $i = 3 ]; then
            
            ssh wpq@$ip "pkill fft" &
            
        else
            ssh wpq@$ip "pkill fft" &
            
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
        runtime=$(run $1 $2 $3 | grep "Run time: " | awk '{print $4}')
        echo "run time of i=$i: $runtime"
        total=$(echo "$total + $runtime" | bc)
        
    done
    echo "total run time of $1 $2 $3: $total"
}


choice_no_array=(2048 4096 8192)
choice_node=(1 2 3 4 5)
choice_no_run=(1 2)

# ./make.sh

# echo "" > output.txt

clean

for no_array in ${choice_no_array[@]}
do
    for no_node in ${choice_node[@]}
    do
        for no_run in ${choice_no_run[@]}
        do
            sleep 1
            file_prefix="no_array_"$no_array"_no_node_"$no_node"_no_run_"$no_run
            echo $file_prefix
            echo "" > data/$file_prefix.txt
            run $no_array $no_node $no_run >> data/$file_prefix.txt
        done
    done
done

# run 8192 5 2 >> output.txt

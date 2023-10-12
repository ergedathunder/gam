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
master_port=80129

remotemake() {
    old_IFS=$IFS
    IFS=$'\n'
    i=0
    for slave in `cat "$slaves"`
    do
        ip=`echo $slave | cut -d ' ' -f1`
        port=`echo $slave | cut -d ' ' -f2`
        
        if [ $i = 0 ]; then
            ssh $ip "$SRC_HOME/make.sh" &
            elif [ $i = 1 ]; then
            
            ssh wpq@$ip "$SRC_HOME_158/make.sh" &
            elif [ $i = 2 ]; then
            
            ssh wpq@$ip "$SRC_HOME_159/make.sh" &
            
        else
            ssh wpq@$ip "$SRC_HOME_160/make.sh" &
            
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

remotemake >> makelog.txt 2>&1


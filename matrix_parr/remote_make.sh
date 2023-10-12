#! /usr/bin/env bash
bin=`dirname "$0"`
bin=`cd "$bin"; pwd`
SRC_HOME=$bin
SRC_HOME_155="/home/xeg/last/gam/matrix_parr"
SRC_HOME_141="/home/xeg/last/gam/matrix_parr"
SRC_HOME_158="/home/xeg/last/gam/matrix_parr"
SRC_HOME_159="/home/xeg/last/gam/matrix_parr"
SRC_HOME_160="/home/xeg/last/gam/matrix_parr"

slaves=$bin/slaves
log_file=$bin/log
log_file_155=$SRC_HOME_155/log
log_file_141=$SRC_HOME_141/log
log_file_158=$SRC_HOME_158/log
log_file_159=$SRC_HOME_159/log
log_file_160=$SRC_HOME_160/log

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
            echo "$ip $SRC_HOME/make.sh"
            elif [ $i = 1 ]; then
            
            ssh xeg@$ip "$SRC_HOME_158/make.sh" &
            echo "$ip $SRC_HOME/make.sh"
            elif [ $i = 2 ]; then
            
            ssh xeg@$ip "$SRC_HOME_159/make.sh" &
            echo "$ip $SRC_HOME/make.sh"
            
        else
            ssh xeg@$ip "$SRC_HOME_160/make.sh" &
            echo "$ip $SRC_HOME/make.sh"
            
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

echo > makelog.txt
remotemake >> makelog.txt 2>&1

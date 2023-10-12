bin=`dirname "$0"`
bin=`cd "$bin"; pwd`
slaves=$bin/slaves

clean() {
    old_IFS=$IFS
    IFS=$'\n'
    i=0
    for slave in `cat "$slaves"`
    do
        ip=`echo $slave | cut -d ' ' -f1`
        port=`echo $slave | cut -d ' ' -f2`
        
        if [ $i = 0 ]; then
            ssh $ip "pkill pagerank" &
            echo "$ip pkill pagerank"

            elif [ $i = 1 ]; then
            
            ssh xeg@$ip "pkill pagerank" &
            echo "$ip pkill pagerank"
            elif [ $i = 2 ]; then
            
            ssh xeg@$ip "pkill pagerank" &
            echo "$ip pkill pagerank"
            
        else
            ssh xeg@$ip "pkill pagerank" &
            echo "$ip pkill pagerank"
            
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

clean
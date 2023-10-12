cd ../src
make clean
make -j
cd ../fft
rm -rf fft
iteration=0
no_run=0
length_log2=0
parrallel=0
echo "" > output.txt
make fft

run(){
    # echo "Run time of $1 length_log2, $2 runs, $3 parallel: "
    ./fft $1 $2 $3 | grep "running time : " | awk '{print $4}'
}

run_10(){
    count=10
    total=0.0
    for ((i=1; i<=$count; i++)); do
        runtime=$(run $1 $2 $3)
        total=$(echo "$total + $runtime" | bc)
    done
    average=$(echo "scale=3; $total / $count" | bc)
    echo "Average run time of $1 length_log2, $2 runs, $3 parallel: $average"
    
}

./fft 6 1 1 >> output.txt


# choice_length_log2=(10 11 12 13 14 15 16 17 18 19 20)
# choice_no_run=(1 2)
# choice_parrallel=(1 2 3 4 5 6 7 8 9 10)

# for length_log2_index in "${choice_length_log2[@]}"
# do
#     for no_run_index in "${choice_no_run[@]}"
#     do
#         for parrallel_index in "${choice_parrallel[@]}"
#         do
#             # echo "length_log2_index = $length_log2_index, no_run_index = $no_run_index, parrallel_index = $parrallel_index"
#             run_10 $length_log2_index $no_run_index $parrallel_index
#         done
#     done
# done

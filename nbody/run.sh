# cd ../src
# make clean
# make -j
# cd ../nbody

rm -rf main
make main

echo > output.txt

# ./main 100 2 2 >> output.txt
# ./main 1000 4 1 | grep "time" >> output.txt
# ./main 1000 4 2 | grep "time" >> output.txt

run_10() {
    total=0.0
    for i in {1..10}
    do
        runtime=$(./main $1 $2 $3 | grep "time" | awk '{print $4}')
        
        total=$(echo "$total + $runtime" | bc)
    done
    echo "no: $1, parrallel_num: $2, method: $3, total 10 times: $total"
}

choice_no_array=(100 500 1000 2000 5000)
choice_parrallel_num=(1 2 4 8)
choice_method=(1 2)

# ./main 1000 4 1 >> output.txt
./main 1000 4 2 >> output.txt


# for no in ${choice_no_array[@]}
# do
#     for parrallel_num in ${choice_parrallel_num[@]}
#     do
#         for method in ${choice_method[@]}
#         do
#             echo "no: $no, parrallel_num: $parrallel_num, method: $method"
#             run_10 $no $parrallel_num $method >> output.txt
            
#         done
#     done
# done

# run_10 1000 4 1 >> output.txt
# run_10 1000 4 2 >> output.txt
# run_10 1000 4 1 >> output.txt
# run_10 1000 4 2 >> output.txt
# run_10 1000 4 1 >> output.txt
# run_10 1000 4 2 >> output.txt
# run_10 1000 4 1 >> output.txt
# run_10 1000 4 2 >> output.txt
# run_10 1000 4 1 >> output.txt
# run_10 1000 4 2 >> output.txt
# run_10 1000 4 1 >> output.txt
# run_10 1000 4 2 >> output.txt
# run_10 1000 4 1 >> output.txt
# run_10 1000 4 2 >> output.txt
# run_10 1000 4 1 >> output.txt
# run_10 1000 4 2 >> output.txt


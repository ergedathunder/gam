cd ../src
make clean
make -j
cd ../fft_gam_parr

rm -rf log.*
make clean
make fft
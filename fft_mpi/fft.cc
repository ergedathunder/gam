#include <math.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <thread>
#include <pthread.h>
#include <complex>
#include <cstring>
#include <iostream>
#include <thread>

using namespace std;
#define PI acos(-1)

typedef std::complex<float> Complex;

// 可以改
int length_log2 = 0;
int length = 1 << length_log2;
#define N length
float fs = 1000;   // 采样频率
float dt = 1 / fs; // 采样间隔（周期）
int iteration_times = 1;
int no_run = 1;
int parrallel_num = 2;

void sub_fft(WorkerHandle *Cur_wh, GAddr addr_value, unsigned int n, unsigned int l, unsigned int k, Complex T)
{
    for (unsigned int a = l; a < N; a += n)
    {
        // a表示每一组的元素的下标，n表示跨度,k表示组数,l表示每一组的首个元素的下标

        unsigned int b = a + k;
        Complex xa, xb;
        Read_val(Cur_wh, addr_value + a * sizeof(complex<float>), (int *)&xa, sizeof(complex<float>));
        Read_val(Cur_wh, addr_value + b * sizeof(complex<float>), (int *)&xb, sizeof(complex<float>));
        Complex t = xa - xb;
        xa = xa + xb;
        Write_val(Cur_wh, addr_value + a * sizeof(complex<float>), (int *)&xa, sizeof(complex<float>), 1);
        xb = t * T;
        Write_val(Cur_wh, addr_value + b * sizeof(complex<float>), (int *)&xb, sizeof(complex<float>), 1);
    }
}

void p_fft_MSI(GAddr addr_value)
{
    unsigned int k = N, n;
    float thetaT = PI / N;
    Complex phiT = Complex(cos(thetaT), -sin(thetaT));
    Complex T;
    thread threads[parrallel_num];

    while (k > 1)
    // while (k > N / 4)
    {
        // k=64,32,16,8,4,2,1 ，表示组数
        printf("k = %d\n", k);
        n = k;
        k >>= 1;
        if (k < parrallel_num)
            parrallel_num = k;
        phiT = phiT * phiT;
        T = 1.0L;

        // parallel

        for (unsigned int l = 0; l < k; l++)
        {
            // l表示每一组的首个元素的下标
            int id = l % parrallel_num;
            threads[id] = thread(sub_fft, wh[id], addr_value, n, l, k, T);
            // threads[id - 1] = thread(test);
            threads[id].join();
            // T *= phiT;
        }
    }

    // Decimate
    unsigned int m = (unsigned int)log2(N);

    for (unsigned int a = 0; a < N; a++)
    {
        unsigned int b = a;
        // Reverse bits
        b = (((b & 0xaaaaaaaa) >> 1) | ((b & 0x55555555) << 1));
        b = (((b & 0xcccccccc) >> 2) | ((b & 0x33333333) << 2));
        b = (((b & 0xf0f0f0f0) >> 4) | ((b & 0x0f0f0f0f) << 4));
        b = (((b & 0xff00ff00) >> 8) | ((b & 0x00ff00ff) << 8));
        b = ((b >> 16) | (b << 16)) >> (32 - m);
        if (b > a)
        {
            // Complex t = x[a];
            // x[a] = x[b];
            // x[b] = t;
            Complex xa, xb, t;
            Read_val(wh[0], addr_value + a * sizeof(complex<float>), (int *)&xa, sizeof(complex<float>));
            Read_val(wh[0], addr_value + b * sizeof(complex<float>), (int *)&xb, sizeof(complex<float>));
            t = xa;
            xa = xb;
            xb = t;
            Write_val(wh[0], addr_value + a * sizeof(complex<float>), (int *)&xa, sizeof(complex<float>), 1);
            Write_val(wh[0], addr_value + b * sizeof(complex<float>), (int *)&xb, sizeof(complex<float>), 1);
        }
    }
}

void Solve_MSI()
{
    malloc_wh = wh[0];

    complex<float> value[N];

    for (int i = 0; i < N; i++)
    {
        value[i].real(0.7 * sin(2 * PI * 50 * dt * i) + sin(2 * PI * 120 * dt * i));
        value[i].imag(0);
    }
    GAddr addr_value = Malloc_addr(malloc_wh, sizeof(complex<float>) * N, Msi, 1);

    Write_val(malloc_wh, addr_value, (int *)value, sizeof(complex<float>) * N, 1);

    p_fft_MSI(addr_value);

    complex<float> temp1;
    Read_val(wh[0], addr_value, (int *)&temp1, sizeof(complex<float>));
    long End = get_time();
    printf("End\n");
    printf("running time : %ld\n", End - Start);

    complex<float> readbuf[N];
    // Read_val(wh[0], addr_value, (int *)readbuf, sizeof(complex<float>) * N);

    for (int i = 0; i < length; i++)
    {
        printf("readbuf[%d]=%f+%fi\n", i, readbuf[i].real(), readbuf[i].imag());
    }
}

int main(int argc, char *argv[])
{
    iteration_times = 1;
    length_log2 = atoi(argv[1]);
    length = 1 << length_log2;

    Solve_MSI();

    return 0;
}
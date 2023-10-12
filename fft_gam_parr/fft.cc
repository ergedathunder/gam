// GAM
#include <math.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <thread>
#include <pthread.h>
#include <complex>
#include <cstring>
#include <thread>
#include "structure.h"
#include "worker.h"
#include "settings.h"
#include "worker_handle.h"
#include "master.h"
#include "gallocator.h"
#include "workrequest.h"
#include <string>
#include <cstring>
#include <iostream>
#include "gallocator.h"
#include "log.h"
#include <cstring>

#define DEBUG_LEVEL LOG_TEST
#define STEPS 204800
uint64 SYNC_KEY = 204800;

using namespace std;
#define PI acos(-1)

typedef std::complex<float> Complex;

int no_array = 0;
float fs = 1000;   // 采样频率
float dt = 1 / fs; // 采样间隔（周期）
int iteration_times = 1;

int node_id;
int id, alloc_id;
int no_node = 0;
int thread_num[4], begin_thread_num[4];
int local_thread_num, local_begin_thread_num;

int worker_num = 0;

int is_read = 0;
int is_sync = 0;
int no_run = 0;
int see_time = 0;
double sleep_time = 0.0;

int is_master = 1;
string ip_master = get_local_ip("eth0");
string ip_worker = get_local_ip("eth0");
int port_master = 12345;
int port_worker = 12346;
GAlloc *alloc;
WorkerHandle *wh[10];
ibv_device **curlist;
Worker *worker[10];
Master *master;

int num_worker = 0;

std::vector<GAlloc *> alloc_array;

void justsetconf(Conf *old_conf)
{
    Conf *conf = new Conf();
    (*conf) = (*old_conf);
    conf->loglevel = DEBUG_LEVEL;
    GAllocFactory::SetConf(conf);
    // no setting master;
}

void Synchro()
{
    // alloc_array[0]->Put(SYNC_KEY + node_id, &node_id, sizeof(int));
    for (int i = 0; i < local_thread_num; i++)
    {
        alloc_array[i]->Put(SYNC_KEY + node_id * no_node + i, &node_id, sizeof(int));
        printf("node %d thread %d put key %ld\n", node_id, i, SYNC_KEY + node_id * no_node + i);
        fflush(stdout);
    }

    for (int i = 1; i <= no_node; i++)
    {
        // alloc_array[0]->Get(SYNC_KEY + i, &id);
        int target_thread_num = begin_thread_num[i - 1];
        for (int j = 0; j < target_thread_num; j++)
        {
            alloc_array[0]->Get(SYNC_KEY + i * no_node + j, &id);
            printf("node %d get key %ld\n", node_id, SYNC_KEY + i * no_node + j);
            epicAssert(id == i);
        }
    }

    SYNC_KEY = SYNC_KEY + no_node * no_node + worker_num;
}

void Create_master(Conf *old_conf)
{
    Conf *conf = new Conf();
    (*conf) = (*old_conf);
    conf->loglevel = DEBUG_LEVEL;
    GAllocFactory::SetConf(conf);
    master = new Master(*conf);
}

void Create_worker(Conf *old_conf)
{
    RdmaResource *res = new RdmaResource(curlist[0], false);
    printf("end get rdma source\n");
    fflush(stdout);

    Conf *conf = new Conf();
    (*conf) = (*old_conf);
    conf->worker_port -= num_worker * 10;
    worker[num_worker] = new Worker(*conf, res);

    printf("end get new worker\n");
    fflush(stdout);

    wh[num_worker] = new WorkerHandle(worker[num_worker]);

    printf("end get new workerhandler\n");
    fflush(stdout);

    printf("cur worker_port : %d\n", conf->worker_port);
    printf("cur node_id : %d\n", wh[num_worker]->GetWorkerId());
    printf("\n");
    fflush(stdout);

    num_worker++;
}

void sub_fft_perthread2(GAddr addr_value, unsigned int n, unsigned int start_index, unsigned int k, Complex T, Complex phiT, int thread_id)
{
}

void sub_fft(GAddr addr_value, unsigned int n, unsigned int l, unsigned int k, Complex T, int thread_id)
{
    for (unsigned int a = l; a < no_array; a += n)
    {
        // a表示每一组的元素的下标，n表示跨度,k表示组数,l表示每一组的首个元素的下标
        unsigned int b = a + k;
        Complex xa, xb;
        // Read_val(Cur_wh, addr_value + a * sizeof(complex<float>), (int *)&xa, sizeof(complex<float>));
        alloc_array[thread_id]->Read(addr_value + a * sizeof(complex<float>), (int *)&xa, sizeof(complex<float>));
        // Read_val(Cur_wh, addr_value + b * sizeof(complex<float>), (int *)&xb, sizeof(complex<float>));
        alloc_array[thread_id]->Read(addr_value + b * sizeof(complex<float>), (int *)&xb, sizeof(complex<float>));
        Complex t = xa - xb;
        xa = xa + xb;
        // Write_val(Cur_wh, addr_value + a * sizeof(complex<float>), (int *)&xa, sizeof(complex<float>), 1);
        alloc_array[thread_id]->Write(addr_value + a * sizeof(complex<float>), (int *)&xa, sizeof(complex<float>), 1);
        xb = t * T;
        // Write_val(Cur_wh, addr_value + b * sizeof(complex<float>), (int *)&xb, sizeof(complex<float>), 1);
        alloc_array[thread_id]->Write(addr_value + b * sizeof(complex<float>), (int *)&xb, sizeof(complex<float>), 1);
    }
}

void sub_fft_perthread(GAddr addr_value, unsigned int n, unsigned int start_index, unsigned int k, Complex *T, Complex phiT, int thread_id)
{
    int interval_node = no_node;
    int interval_thread = local_thread_num;
    int interval = interval_node * interval_thread;

    for (int index = start_index + thread_id * interval_node; index < k; index += interval)
    {
        sub_fft(addr_value, n, index, k, T[index], thread_id);
    }
}

void sub_fft_perworker(GAddr addr_value, unsigned int n, unsigned int start_index, unsigned int k, Complex *T, Complex phiT)
{
    thread *threads = new thread[local_thread_num];
    for (int i = 0; i < local_thread_num; i++)
    {
        threads[i] = thread(sub_fft_perthread, addr_value, n, start_index, k, T, phiT, i);
    }
    for (int i = 0; i < local_thread_num; i++)
    {
        threads[i].join();
    }
}

void p_fft_RC(GAddr addr_value)
{
    unsigned int k = no_array, n;
    float thetaT = PI / no_array;
    Complex phiT = Complex(cos(thetaT), -sin(thetaT));
    Complex T;

    while (k > 1)
    {
        // k=64,32,16,8,4,2,1 ，表示组数
        std::cout << "k = " << k << endl;
        n = k;
        k >>= 1;
        Complex T[k];
        // if (k <= no_node * local_thread_num)
        // {
        //     no_node = 1;
        //     local_thread_num = 1;
        // }

        phiT = phiT * phiT;
        T[0] = 1.0L;
        for (int i = 1; i < k; i++)
        {
            T[i] = T[i - 1] * phiT;
        }

        for (int i = 0; i < local_thread_num; i++)
        {
            alloc_array[i]->acquireLock(1, addr_value, sizeof(complex<float>) * no_array, true, sizeof(complex<float>));
        }

        for (int i = 0; i < no_node; i++)
        {
            int id_parr = i + 1;
            if (id_parr == node_id)
            {
                sub_fft_perworker(addr_value, n, i, k, T, phiT);
            }
        }

#ifdef RC_VERSION2
#else
        for (int i = 0; i < local_thread_num; i++)
        {
            alloc_array[i]->releaseLock(1, addr_value);
        }
#endif

        Synchro();
    }

    if (is_master)
    {
        unsigned int m = (unsigned int)log2(no_array);

        for (unsigned int a = 0; a < no_array; a++)
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
                // Read_val(wh[0], addr_value + a * sizeof(complex<float>), (int *)&xa, sizeof(complex<float>));
                alloc_array[0]->Read(addr_value + a * sizeof(complex<float>), (int *)&xa, sizeof(complex<float>));
                // Read_val(wh[0], addr_value + b * sizeof(complex<float>), (int *)&xb, sizeof(complex<float>));
                alloc_array[0]->Read(addr_value + b * sizeof(complex<float>), (int *)&xb, sizeof(complex<float>));
                t = xa;
                xa = xb;
                xb = t;
                // Write_val(wh[0], addr_value + a * sizeof(complex<float>), (int *)&xa, sizeof(complex<float>), 1);
                alloc_array[0]->Write(addr_value + a * sizeof(complex<float>), (int *)&xa, sizeof(complex<float>), 1);
                // Write_val(wh[0], addr_value + b * sizeof(complex<float>), (int *)&xb, sizeof(complex<float>), 1);
                alloc_array[0]->Write(addr_value + b * sizeof(complex<float>), (int *)&xb, sizeof(complex<float>), 1);
            }
        }
    }
}

void p_fft_MSI(GAddr addr_value)
{
    unsigned int k = no_array, n;
    float thetaT = PI / no_array;
    Complex phiT = Complex(cos(thetaT), -sin(thetaT));

    while (k > 1)
    // while (k > 2)
    {
        // k=64,32,16,8,4,2,1 ，表示组数
        printf("k = %d\n", k);
        n = k;
        k >>= 1;
        Complex T[k];
        // if (k < worker_num)
        // {
        //     no_node = 1;
        //     local_thread_num = 1;
        // }

        phiT = phiT * phiT;
        T[0] = 1.0L;
        for (int i = 1; i < k; i++)
        {
            T[i] = T[i - 1] * phiT;
        }

        for (int i = 0; i < no_node; i++)
        {
            int id_parr = i + 1;
            if (id_parr == node_id)
            {
                sub_fft_perworker(addr_value, n, i, k, T, phiT);
            }
        }
        Synchro();
    }

    // Decimate
    if (is_master)
    {
        unsigned int m = (unsigned int)log2(no_array);

        for (unsigned int a = 0; a < no_array; a++)
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
                // Read_val(wh[0], addr_value + a * sizeof(complex<float>), (int *)&xa, sizeof(complex<float>));
                alloc_array[0]->Read(addr_value + a * sizeof(complex<float>), (int *)&xa, sizeof(complex<float>));
                // Read_val(wh[0], addr_value + b * sizeof(complex<float>), (int *)&xb, sizeof(complex<float>));
                alloc_array[0]->Read(addr_value + b * sizeof(complex<float>), (int *)&xb, sizeof(complex<float>));
                t = xa;
                xa = xb;
                xb = t;
                // Write_val(wh[0], addr_value + a * sizeof(complex<float>), (int *)&xa, sizeof(complex<float>), 1);
                alloc_array[0]->Write(addr_value + a * sizeof(complex<float>), (int *)&xa, sizeof(complex<float>));
                // Write_val(wh[0], addr_value + b * sizeof(complex<float>), (int *)&xb, sizeof(complex<float>), 1);
                alloc_array[0]->Write(addr_value + b * sizeof(complex<float>), (int *)&xb, sizeof(complex<float>));
            }
        }
    }
}

void Solve_RC()
{
    GAddr addr_value;
    long Start;
    long End;
    if (is_master)
    {
        std::vector<std::complex<float>> value(no_array);

        for (int i = 0; i < no_array; i++)
        {
            value[i].real(0.7 * sin(2 * PI * 50 * dt * i) + sin(2 * PI * 120 * dt * i));
            value[i].imag(0);
        }
        addr_value = alloc_array[0]->AlignedMalloc(sizeof(complex<float>) * no_array, RC_Write_shared, 1);
        printf("addr_value=%lx\n", addr_value);
        alloc_array[0]->Write(addr_value, (int *)value.data(), sizeof(complex<float>) * no_array);
        alloc_array[0]->Put(141, &addr_value, sizeof(long long));
    }
    else
    {
        printf("begin slave\n");
        alloc_array[0]->Get(141, &addr_value);
        printf("addr_value=%lx\n", addr_value);
    }

    Synchro();

    if (is_master)
    {
        Start = get_time();
    }

    p_fft_RC(addr_value);

    if (is_master)
    {
        End = get_time();
        printf("running time : %ld\n", End - Start);
        if (is_read)
        {
            std::vector<std::complex<float>> readbuf(no_array);
            alloc_array[0]->Read(addr_value, (int *)readbuf.data(), sizeof(complex<float>) * no_array);

            for (int i = 0; i < no_array; i++)
                printf("readbuf[%d]=%f+%fi\n", i, readbuf[i].real(), readbuf[i].imag());
        }
    }
    for (int alloc_id = 0; alloc_id < local_begin_thread_num; alloc_id++)
    {
        alloc_array[alloc_id]->ReportCacheStatistics_RC();
    }
}

void Solve_MSI()
{
    GAddr addr_value;
    long Start;
    long End;
    if (is_master)
    {
        std::vector<std::complex<float>> value(no_array);

        for (int i = 0; i < no_array; i++)
        {
            value[i].real(0.7 * sin(2 * PI * 50 * dt * i) + sin(2 * PI * 120 * dt * i));
            value[i].imag(0);
        }
        addr_value = alloc_array[0]->AlignedMalloc(sizeof(complex<float>) * no_array, Msi, 1);
        printf("begin master,addr_value=%lx\n", addr_value);
        alloc_array[0]->Write(addr_value, (int *)value.data(), sizeof(complex<float>) * no_array);
        alloc_array[0]->Put(141, &addr_value, sizeof(long long));
    }
    else
    {
        alloc_array[0]->Get(141, &addr_value);
        printf("begin slave,addr_value=%lx\n", addr_value);
    }

    Synchro();

    if (is_master)
    {
        Start = get_time();
    }

    p_fft_MSI(addr_value);

    if (is_master)
    {
        End = get_time();
        printf("running time : %ld\n", End - Start);
        if (is_read)
        {
            // complex<float> readbuf[no_array];
            std::vector<std::complex<float>> readbuf(no_array);
            alloc_array[0]->Read(addr_value, (int *)readbuf.data(), sizeof(complex<float>) * no_array);

            for (int i = 0; i < no_array; i++)
                printf("readbuf[%d]=%f+%fi\n", i, readbuf[i].real(), readbuf[i].imag());
        }
    }

    for (int alloc_id = 0; alloc_id < local_begin_thread_num; alloc_id++)
    {
        alloc_array[alloc_id]->ReportCacheStatistics();
    }
}

void func()
{
    printf("bad!,master first come in\n");
}

void Solve_test()
{
    return;
}
void initValue()
{
    int workernum = worker_num;
    for (int j = 0; workernum > 0; j++)
    {
        thread_num[j % no_node]++; // 使用取模运算在节点之间循环分配
        workernum--;               // 更新剩余的工作线程数
    }
    epicAssert(no_node <= worker_num);
    
    for (int j = 0; j < no_node; j++)
    {
        printf("node %d has %d threads\n", j, thread_num[j]);
        begin_thread_num[j] = thread_num[j];
        if (j + 1 == node_id)
        {
            local_thread_num = thread_num[j];
            local_begin_thread_num = begin_thread_num[j];
        }
    }

    curlist = ibv_get_device_list(NULL);

    alloc_array.resize(local_thread_num);

    int temp_sum = 0;
    for (int i = 0; i < no_node; i++)
    {
        temp_sum += begin_thread_num[i];
    }
    epicAssert(temp_sum == worker_num);
}

int main(int argc, char *argv[])
{

    for (int i = 1; i < argc; i++)
    {
        if (strcmp(argv[i], "--ip_master") == 0)
        {
            ip_master = string(argv[++i]);
        }
        else if (strcmp(argv[i], "--ip_worker") == 0)
        {
            ip_worker = string(argv[++i]);
        }
        else if (strcmp(argv[i], "--port_master") == 0)
        {
            port_master = atoi(argv[++i]);
        }
        else if (strcmp(argv[i], "--iface_master") == 0)
        {
            ip_master = get_local_ip(argv[++i]);
        }
        else if (strcmp(argv[i], "--port_worker") == 0)
        {
            port_worker = atoi(argv[++i]);
        }
        else if (strcmp(argv[i], "--iface_worker") == 0)
        {
            ip_worker = get_local_ip(argv[++i]);
        }
        else if (strcmp(argv[i], "--iface") == 0)
        {
            ip_worker = get_local_ip(argv[++i]);
            ip_master = get_local_ip(argv[i]);
        }
        else if (strcmp(argv[i], "--is_master") == 0)
        {
            is_master = atoi(argv[++i]);
        }
        else if (strcmp(argv[i], "--no_array") == 0)
        {
            no_array = atoi(argv[++i]);
        }
        else if (strcmp(argv[i], "--no_node") == 0)
        {
            no_node = atoi(argv[++i]);
        }
        else if (strcmp(argv[i], "--no_worker") == 0)
        {
            worker_num = atoi(argv[++i]);
        }
        else if (strcmp(argv[i], "--node_id") == 0)
        {
            node_id = atoi(argv[++i]);
        }
        else if (strcmp(argv[i], "--no_run") == 0)
        {
            no_run = atoi(argv[++i]);
        }
        else if (strcmp(argv[i], "--is_read") == 0)
        {
            is_read = atoi(argv[++i]);
        }
        else if (strcmp(argv[i], "--is_sync") == 0)
        {
            is_sync = atoi(argv[++i]);
        }
        else if (strcmp(argv[i], "--see_time") == 0)
        {
            see_time = atoi(argv[++i]);
        }
        else if (strcmp(argv[i], "--sleep_time") == 0)
        {
            sleep_time = atoi(argv[++i]);
        }
        else
        {
            fprintf(stderr, "Unrecognized option %s for benchmark\n", argv[i]);
        }
    }
    Conf conf;
    conf.loglevel = DEBUG_LEVEL;
    conf.is_master = is_master;
    conf.master_ip = ip_master;
    conf.master_port = port_master;
    conf.worker_ip = ip_worker;
    conf.worker_port = port_worker;

    initValue();

    if (is_master)
    {
        Create_master(&conf);
        for (int i = 0; i < local_thread_num; ++i)
        {
            Create_worker(&conf);
        }
        for (int i = 0; i < local_thread_num; ++i)
        {
            alloc_array[i] = new GAlloc(worker[i]);
        }
        for (int i = 0; i < local_thread_num; ++i)
        {
            printf("worker i de id = %d \n", alloc_array[i]->GetID());
            fflush(stdout);
        }
        // int alloc_complete_flag = 0;
        // for (int i = 2; i <= no_node; i++)
        // {
        //     alloc_array[0]->Get(2023 + i, &alloc_complete_flag);
        //     printf("alloc_complete_flag=%d\n", alloc_complete_flag);
        //     fflush(stdout);
        // }

        // GAddr alloc_complete_flag_addr = alloc_array[0]->AlignedMalloc(BLOCK_SIZE, Msi, 1);
        // printf("alloc_complete_flag_addr=%lx\n", alloc_complete_flag_addr);
        // fflush(stdout);
        // alloc_array[0]->Put(2023, &alloc_complete_flag_addr, sizeof(GAddr));
        // printf("put addr ok\n");
        // alloc_array[0]->Write(alloc_complete_flag_addr, &alloc_complete_flag, sizeof(int));
        // while (1)
        // {
        //     alloc_array[0]->Read(alloc_complete_flag_addr, &alloc_complete_flag, sizeof(int));
        //     // if (alloc_complete_flag == 923)
        //     // {
        //     //     break;
        //     // }
        //     printf("alloc_complete_flag=%d\n", alloc_complete_flag);
        //     fflush(stdout);
        // }
    }

    else
    {
        justsetconf(&conf);
        for (int i = 0; i < local_thread_num; ++i)
        {
            Create_worker(&conf);
        }

        for (int i = 0; i < local_thread_num; ++i)
        {
            alloc_array[i] = new GAlloc(worker[i]);
        }
        for (int i = 0; i < local_thread_num; ++i)
        {
            printf("worker i de id = %d \n", alloc_array[i]->GetID());
            fflush(stdout);
        }

        // int alloc_complete_flag;
        // for (int i = 1; i <= no_node; i++)
        // {
        //     if (i == node_id)
        //     {
        //         alloc_complete_flag = 923 + i;
        //         alloc_array[0]->Put(2023 + i, &alloc_complete_flag, sizeof(int));
        //     }
        // }

        // GAddr alloc_complete_flag_addr;
        // alloc_array[0]->Put(2023, &alloc_complete_flag_addr, sizeof(GAddr));
        // printf("get addr = %lx\n", alloc_complete_flag_addr);
        // GAddr test_addr = alloc_array[0]->AlignedMalloc(BLOCK_SIZE, Msi, 1);
        // printf("test_addr=%lx\n", test_addr);

        // alloc_array[0]->Write(alloc_complete_flag_addr, &alloc_complete_flag, sizeof(int));
    }

    printf("node_id = %d\n", node_id);

    Synchro();

    if (no_run == 1)
    {
        Solve_MSI();
    }
    else if (no_run == 2)
    {
        Solve_RC();
    }
    else if (no_run == 3)
    {
        Solve_test();
    }
    sleep(10);
    return 0;
}
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
WorkerHandle *wh[10];
ibv_device **curlist;
Worker *worker[10];
Master *master;
int worker_id[10];
int sync_key[10];

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

void Synchro_spe(int wh_id)
{
    int id = 0;
    alloc_array[wh_id]->Put(sync_key[wh_id] + worker_id[wh_id], &worker_id[wh_id], sizeof(int));
    for (int i = 1; i <= worker_num + 1; i++)
    {
        alloc_array[wh_id]->Get(sync_key[wh_id] + i, &id);
        epicAssert(id == i);
    }

    sync_key[wh_id] = sync_key[wh_id] + worker_num + 1 + 1;
}

void Synchro(int wh_id)
{
    int id = 0;
    alloc_array[wh_id]->Put(sync_key[wh_id] + worker_id[wh_id], &worker_id[wh_id], sizeof(int));
    for (int i = 1; i <= worker_num; i++)
    {
        alloc_array[wh_id]->Get(sync_key[wh_id] + i, &id);
        epicAssert(id == i);
    }

    sync_key[wh_id] = sync_key[wh_id] + worker_num + 1;
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

    // printf("end get new worker\n");
    fflush(stdout);

    wh[num_worker] = new WorkerHandle(worker[num_worker]);

    // printf("end get new workerhandler\n");
    fflush(stdout);

    printf("cur worker_port : %d\n", conf->worker_port);
    printf("cur node_id : %d\n", wh[num_worker]->GetWorkerId());
    printf("\n");
    fflush(stdout);
    worker_id[num_worker] = wh[num_worker]->GetWorkerId();

    num_worker++;
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
        begin_thread_num[j] = thread_num[j];
        if (j + 1 == node_id)
        {
            printf("node %d has %d threads\n", j, thread_num[j]);
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

int Max_node = 0;
const int Maxn = 1000010;
vector<int> Link[Maxn]; // 记录入边
map<int, int> Id_map;
map<int, int> Rev_Id_map;

int cd[Maxn];
int num_threads = 4;
const int Maxm = 4;
int total_node = 0;
int total_edge = 0;

uint64 read_miss[10], read_hit[10], write_miss[10], write_hit[10];
int read_miss_sum, read_hit_sum, write_miss_sum, write_hit_sum;

static void Calc_Matrix(GAlloc *alloc, GAddr a, GAddr b, GAddr c, int n, int Startx, int Endx, int Starty, int Endy)
{
    static int first_time = 0;
    if (first_time == 0)
    {
        first_time = 1;
        printf("(%d, %d) -> (%d, %d) \n", Startx, Starty, Endx, Endy);
    }

    int test_iterations = 1;
    for (int oo = 0; oo < test_iterations; ++oo)
    {
        for (int i = Startx; i < Endx; i++)
        {
            for (int j = Starty; j < Endy; j++)
            {
                float t = 0;
                for (int k = 0; k < n; k++)
                {
                    float val1 = 0, val2 = 0;
                    alloc->Read(a + (i * n + k) * sizeof(float), &val1, sizeof(float));
                    alloc->Read(b + (k * n + j) * sizeof(float), &val2, sizeof(float));
                    t += (float)val1 * val2;
                    alloc->Write(c + (i * n + j) * sizeof(float), &t, sizeof(float));
                }
            }
        }
    }
}

int test_val = 1;

void genMat(GAlloc *alloc, GAddr arr, int n)
{
    int i, j;

    for (i = 0; i < n; i++)
    {
        for (j = 0; j < n; j++)
        {
            float val = (float)rand() / RAND_MAX + (float)rand() / (RAND_MAX * RAND_MAX);
            val = (float)test_val;
            alloc->Write(arr + (i * n + j) * sizeof(float), &val, sizeof(float));
            test_val = test_val + 1;
        }
    }
}

void Answer_check(GAlloc *alloc, int n, GAddr c)
{
    int Round = 5;
    int Curx = 3, Cury = 15;
    for (int i = 0; i < Round; ++i)
    {
        int x = rand() % n;
        int y = rand() % n;
        x = Curx, y = Cury + i;
        printf("x : %d, y : %d\n", x, y);
        float val;
        alloc->Read(c + (x * n + y) * sizeof(float), &val, sizeof(float));
        printf(" c[%d][%d] = %.3f\n", x, y, val);
    }

    FILE *fp = fopen("/home/wpq/gam/matrix_gam_parr/DataSet/result_msi.txt", "w");
    for (int i = 0; i < n; ++i)
    {
        for (int j = 0; j < n; ++j)
        {
            float val;
            alloc->Read(c + (i * n + j) * sizeof(float), &val, sizeof(float));
            fprintf(fp, "%lf ", val);
        }
        fprintf(fp, "\n");
    }
    fclose(fp);
}

void Matrix(int wh_id)
{
    alloc_array[wh_id]->ResetCacheStatistics();
    alloc_array[wh_id]->ResetCacheStatistics_RC();
    GAddr a, b, c, d;
    int n = no_array;
    int Start_val = 141;
    if (is_master && wh_id == 0)
    {
        a = alloc_array[wh_id]->AlignedMalloc(sizeof(float) * n * n, 0, 0);
        b = alloc_array[wh_id]->AlignedMalloc(sizeof(float) * n * n, 0, 0);
        if (no_run & 1)
        {
            c = alloc_array[wh_id]->AlignedMalloc(sizeof(float) * n * n, Msi, 0);
        }
        else
        {
            c = alloc_array[wh_id]->AlignedMalloc(sizeof(float) * n * n, RC_Write_shared, 0);
        }

        alloc_array[wh_id]->Put(Start_val++, &a, sizeof(GAddr));
        alloc_array[wh_id]->Put(Start_val++, &b, sizeof(GAddr));
        alloc_array[wh_id]->Put(Start_val++, &c, sizeof(GAddr));

        genMat(alloc_array[wh_id], a, n);
        genMat(alloc_array[wh_id], b, n);
    }

    else
    {
        alloc_array[wh_id]->Get(Start_val++, &a);
        alloc_array[wh_id]->Get(Start_val++, &b);
        alloc_array[wh_id]->Get(Start_val++, &c);
    }

    int Max_iteration = 1;

    Synchro(wh_id);
    printf("worker %d finish first sync\n", worker_id[wh_id]);
    fflush(stdout);

    long Start_time = get_time();

    for (int Round = 0; Round < Max_iteration; ++Round)
    {

        long Cur_round_start = get_time();
        int apartx = 2;
        int aparty = worker_num / apartx;

        int i = (worker_id[wh_id] - 1) / aparty;
        int j = (worker_id[wh_id] - 1) - i * aparty;

        int Cur_Startx = i * (n / apartx);
        int Cur_Endx = Cur_Startx + (n / apartx);

        int Cur_Starty = j * (n / aparty);
        int Cur_Endy = Cur_Starty + (n / aparty);
        int Cur_id = i * aparty + j;

        alloc_array[wh_id]->acquireLock(1, c, sizeof(float) * n * n, false, sizeof(float));
        Calc_Matrix(alloc_array[wh_id], a, b, c, n, Cur_Startx, Cur_Endx, Cur_Starty, Cur_Endy);

        Synchro(wh_id);
        long Cur_round_end = get_time();
        if (is_master && wh_id == 0)
            printf("Round %d run time : %lld\n", Round + 1, Cur_round_end - Cur_round_start);
        fflush(stdout);
    }

    long End_time = get_time();

    tuple<uint64_t, uint64_t, uint64_t, uint64_t> temp = alloc_array[wh_id]->getReadWriteMiss();
    read_miss[wh_id] = get<0>(temp);
    read_hit[wh_id] = get<1>(temp);
    write_miss[wh_id] = get<2>(temp);
    write_hit[wh_id] = get<3>(temp);

    if (no_run == 2)
    {
        tuple<uint64_t, uint64_t, uint64_t, uint64_t> temp1 = alloc_array[wh_id]->getReadWriteMiss_RC();
        read_miss[wh_id] += get<0>(temp1);
        read_hit[wh_id] += get<1>(temp1);
        write_miss[wh_id] += get<2>(temp1);
        write_hit[wh_id] += get<3>(temp1);
    }

    if (is_master && wh_id == 0)
    {
        printf("running time : %lld\n", End_time - Start_time);
        fflush(stdout);

        // Answer_check(alloc_array[wh_id], n, c);
    }

    Synchro(wh_id);
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

    num_threads = worker_num;

    /* init Gam */
    initValue();

    /* init worker */
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
    }

    printf("node_id = %d\n", node_id);

    for (int i = 0; i < local_thread_num; ++i)
    {
        sync_key[i] = SYNC_KEY;
    }
    /* start mul matrix */
    thread threads[local_thread_num + 2];

    for (int i = 0; i < local_thread_num; ++i)
    {
        threads[i] = thread(Matrix, i);
    }
    for (int i = 0; i < local_thread_num; ++i)
    {
        threads[i].join();
    }

    for(int i = 0; i < local_thread_num; ++i)
    {
        printf("the %d th worker : read_miss = %lld, read_hit = %lld, write_miss = %lld, write_hit = %lld\n", i, read_miss[i], read_hit[i], write_miss[i], write_hit[i]);
    }

    sleep(2);
    return 0;
}
#include <omp.h> 
#include <cstdio> 
#include <cstdlib>
#include <cmath>
#include <ctime>
#include <thread>
#include <pthread.h>
#include "structure.h"
#include "worker.h"
#include "settings.h"
#include "worker_handle.h"
#include "master.h"
#include "gallocator.h"
#include "workrequest.h"
#include "log.h"

using namespace std;

WorkerHandle *wh[10];
ibv_device **curlist; 
Worker *worker[10];  
Master *master;
int num_worker = 0;
int num_threads = 4;
int iteration_times=50;   

void Create_master()
{
    Conf *conf = new Conf();
    // conf->loglevel = LOG_PQ;
    //conf->loglevel = LOG_TEST;
    conf->loglevel = LOG_WARNING;
    GAllocFactory::SetConf(conf);
    master = new Master(*conf);
}
 
void Create_worker()
{
    Conf *conf = new Conf();
    RdmaResource *res = new RdmaResource(curlist[0], false); 
    conf->worker_port += num_worker;
    worker[num_worker] = new Worker(*conf, res);
    wh[num_worker] = new WorkerHandle(worker[num_worker]);
    num_worker++;
}

void Read_val(WorkerHandle *Cur_wh, GAddr addr, void *val, int size)
{
    WorkRequest wr{}; 
    wr.op = READ;
    wr.flag = 0;
    wr.size = size;
    wr.addr = addr;
    wr.ptr = (void *)val;
    if (Cur_wh->SendRequest(&wr))
    {
        epicLog(LOG_WARNING, "send request failed");
    }
}

void Write_val(WorkerHandle *Cur_wh, GAddr addr, void *val, int size)
{
    WorkRequest wr{};
    for (int i = 0; i < 1; i++)
    {
        wr.Reset();
        wr.op = WRITE;
        //wr.flag = ASYNC; // 可以在这里调 
        wr.size = size;
        wr.addr = addr;
        wr.ptr = (void *)val;
        if (Cur_wh->SendRequest(&wr))
        {
            epicLog(LOG_WARNING, "send request failed");
        }
    }
}

GAddr Malloc_addr(WorkerHandle *Cur_wh, const Size size, Flag flag, int Owner)
{
#ifdef LOCAL_MEMORY_HOOK
    void *laddr = zmalloc(size);
    return (GAddr)laddr;
#else
    WorkRequest wr = {};
    wr.op = MALLOC;
    wr.flag = flag;
    wr.size = size;
    wr.arg = Owner;

    if (Cur_wh->SendRequest(&wr))
    {
        epicLog(LOG_WARNING, "malloc failed");
        return Gnullptr;
    }
    else
    {
        epicLog(LOG_DEBUG, "addr = %x:%lx", WID(wr.addr), OFF(wr.addr));
        return wr.addr;
    }
#endif
}

void Init_Gam() {
    curlist = ibv_get_device_list(NULL);
    Create_master();
    for (int i = 0; i < 4; ++i)
    {
        Create_worker();
    }

    sleep(1);
}

void genMat(GAddr arr, int n)
{
	int i, j;

	for (i = 0; i < n; i++)
	{
		for (j = 0; j < n; j++)
		{
            float val = (float)rand() / RAND_MAX + (float)rand() / (RAND_MAX * RAND_MAX);
            Write_val(wh[0], arr + (i * n + j) * sizeof(float), &val, sizeof(float));
		}
	}
}

void Gauss (WorkerHandle * Cur_wh, int row, int Start_row, int End_row, int Start_col, int End_col, int n, GAddr A) {
    for (int i = Start_row; i < End_row; ++i) {
        float x;
        Read_val(Cur_wh, A + sizeof(float) * (i * n + row), &x, sizeof(float) );
        for (int j = Start_col; j < End_col; ++j) {    
            float y, z;
            Read_val(Cur_wh, A + sizeof(float) * (i * n + j), &y, sizeof(float) );
            Read_val(Cur_wh, A + sizeof(float) * (row * n + j), &z, sizeof(float) );
            y = y - x * z;
            Write_val(Cur_wh, A + sizeof(float) * (i * n + j), &y, sizeof(float) );
        }
        x = 0;
        Write_val(Cur_wh, A + sizeof(float) * (i * n + row), &x, sizeof(float) );
    }
}

uint64 Transfer[100], Racetime[100], Requesttime[100];

int main() {

    srand(time(NULL));
    Init_Gam();

    int N = 512;
    GAddr A, B, X;
    A = Malloc_addr(wh[0], sizeof(float) * N * N, 0, 0);  
    B = Malloc_addr(wh[0], sizeof(float) * N, 0, 0); 
    X = Malloc_addr(wh[0], sizeof(float) * N, 0, 0);

    genMat(A, N);

    sleep(2);

    uint64 Total_transfer = 0;
    uint64 Total_racetime = 0;
    uint64 Total_requesttime = 0;

    clock_t start, stop;
	start = clock();
    long Start = get_time();
    num_threads = num_worker;

    for (int i = 0; i < num_worker; ++i) {
        Transfer[i] = wh[i]->getTransferredBytes();
        Racetime[i] = wh[i]->getracetime();
        Requesttime[i] = wh[i]->getrequesttime();
    }

    thread threads[num_threads];

    for (int i = 0; i < N; ++i) {

        //epicLog(LOG_WARNING, "got %d", i);
        
        float Cur;
        Read_val (wh[0], A + (i * N + i) * sizeof(float), &Cur, sizeof(float) );
        if (Cur == 0) {
            epicLog(LOG_WARNING, "got zero");
        }

        for (int j = i+1; j < N; ++j) {
            float x;
            Read_val(wh[0], A + (i * N + j) * sizeof(float), &x, sizeof(float) );
            x = x / Cur;
            Write_val(wh[0], A + (i * N + j) * sizeof(float), &x, sizeof(float) );
        }

        Cur = 1.0;
        Write_val(wh[0], A + (i * N + i) * sizeof(float), &Cur, sizeof(float) );

        if (i == N - 1) continue;

        //if (num_threads > (N - i - 1) ) num_threads = N - i - 1; 

        int apartx = 2; 
        int aparty = 2;
        int Intervalx = (N - i - 1) / apartx;
        int Intervaly = (N - i - 1) / aparty;

        //epicLog(LOG_WARNING, "got first");

/*
        int numx = 2, numy = 2;
        
        for (int j = 0; j < num_threads; ++j) {
            int Start_row = i + 1 + j * apartx;
            int End_row = i + 1 + (j + 1) * apartx;
            if (j == num_threads - 1) End_row = N;
            threads[j] = thread(Gauss, wh[j], i, Start_row, End_row, N, A); 
        }

*/
        
        for (int j = 0; j < apartx; ++j) {
            int Start_row = i + 1 + j * Intervalx;
            int End_row = i + 1 + (j + 1) * Intervalx;
            if (End_row > N) End_row = N;

            for (int k = 0; k < aparty; ++k) {
                int Start_col = i + 1 + k * Intervaly;
                int End_col = i + 1 + (k + 1) * Intervaly;
                if (End_col > N) End_col = N;
                threads[j * aparty + k] = thread(Gauss, wh[j * aparty + k], i, Start_row, End_row, Start_col, End_col, N, A);; 
            }
        }

        for (int j = 0; j < num_threads; ++j) {
            threads[j].join(); 
        }

        //epicLog(LOG_WARNING, "got second");
    }  

    stop = clock();
    long End = get_time();
    printf ("End\n");
    printf ("running time : %lld\n", End - Start); 
	printf("CPU_Serial time: %3f ms\n", ((double)stop - start) / CLOCKS_PER_SEC * 1000.0); 

    for (int i = 0; i < num_worker; ++i) {
        Transfer[i] = wh[i]->getTransferredBytes() - Transfer[i];
        Racetime[i] = wh[i]->getracetime() - Racetime[i]; 
        Requesttime[i] = wh[i]->getrequesttime() - Requesttime[i];
        printf ("Node %d transfer : %llu, racetime : %llu\n", i, Transfer[i], Racetime[i]);
        Total_transfer += Transfer[i];
        Total_racetime += Racetime[i];
        Total_requesttime += Requesttime[i];  
    }

    printf ("Total transfer : %llu\n", Total_transfer);
    printf ("Total racetime : %llu\n", Total_racetime);
    printf ("Total requesttime : %llu\n", Total_requesttime);

    return 0;
}
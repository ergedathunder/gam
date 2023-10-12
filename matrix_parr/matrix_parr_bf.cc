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

#define DEBUG_LEVEL LOG_TEST
uint64 SYNC_KEY = 204800;

int Start_val = 141;

int no_array = 0;
float fs = 1000;   // 采样频率
float dt = 1 / fs; // 采样间隔（周期）
int iteration_times = 1;

int node_id;
int id;
int no_node = 0;
int is_read = 0;
int is_sync = 0;
int no_run = 0;
int see_time = 0;
double sleep_time = 0.0;

int parrallel_num = 0;
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
int num_threads = 8;

typedef struct Error {
	float max;
	float average;
}Error; 

void Synchro() {
    static int cur_round = 0;
    cur_round ++;
    alloc->Put(SYNC_KEY + node_id, &node_id, sizeof(int) );
    for (int i = 1; i <= no_node; i++)
    {
        alloc->Get(SYNC_KEY + i, &id);
        epicAssert(id == i);
    }

    SYNC_KEY = SYNC_KEY + no_node + 1;
}

static void matMultCPU_serial(GAddr , GAddr, GAddr , int , int, int , int ,int );
void genMat(GAddr , int );
Error accuracyCheck(const float* , const float* , int );

void Answer_check(int n, GAddr c) {
    int Round = 5;
    int Curx = 3, Cury = 15;
    for (int i = 0; i < Round; ++i) {
        int x = rand() % n;
        int y = rand() % n;
        x = Curx, y = Cury + i;
        printf ("x : %d, y : %d\n", x, y);
        float val;
        alloc->Read(c + (x * n + y) * sizeof(float), &val, sizeof(float)); 
        printf (" c[%d][%d] = %.3f\n", x, y, val);  
    }

    FILE* fp = fopen ("/home/xeg/last/gam/matrix_parr/DataSet/result_msi.txt", "w");
    for (int i = 0; i < n; ++i) {
        for (int j = 0; j < n; ++j) {
            float val;
            alloc->Read(c + (i * n + j) * sizeof(float), &val, sizeof(float));
            fprintf (fp, "%lf ", val);
        } fprintf (fp, "\n");
    }
    fclose(fp);   
}

int main(int argc, char *argv[]) {

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

    alloc = GAllocFactory::CreateAllocator(&conf);
    printf ("node_number : %d, running_type : %d, array_number : %d\n", no_node, no_run, no_array);

    node_id = alloc->GetID();
    printf ("cur_node_id : %d, no_number : %d\n", node_id, no_node);
    Synchro();

    num_threads = no_node;

	printf ("finish Init node %d\n", node_id);
	fflush(stdout);
    srand(time(NULL));
	// Init matrix
    GAddr a, b, c, d;
	int n = no_array; 
    if (is_master) {
        a = alloc->AlignedMalloc(sizeof(float) * n * n, 0, 0);
        b = alloc->AlignedMalloc(sizeof(float) * n * n, 0, 0);
        if (no_run & 1) {
            c = alloc->AlignedMalloc(sizeof(float) * n * n, Msi, 0);
        }
        else {
            c = alloc->AlignedMalloc(sizeof(float) * n * n, Write_shared, 0);
        }

        alloc->Put(Start_val++, &a, sizeof(GAddr) );
        alloc->Put(Start_val++, &b, sizeof(GAddr) );
        alloc->Put(Start_val++, &c, sizeof(GAddr) );

        genMat(a, n);
        genMat(b, n);
    }
 
    else {
        alloc->Get(Start_val++, &a);
        alloc->Get(Start_val++, &b);
        alloc->Get(Start_val++, &c);
    }
 
    Synchro();

	clock_t start, stop;
	start = clock();
    long Start = get_time();
	////// calculation code here ///////
    num_threads = no_node;
    num_worker = no_node;

    uint64 Transfer, Racetime, Requesttime;
 
    Transfer = alloc->getTransferredBytes();
    Racetime = alloc->getracetime();
    Requesttime = alloc->getrequesttime();

    iteration_times = no_run + (no_run & 1);
    for (int Round = 0; Round < iteration_times; ++Round) {
        int apartx = 2; 
        int aparty = 2;

        int i = (node_id - 1) / aparty;
        int j = (node_id - 1) - i * aparty;

        int Cur_Startx = i * (n / apartx);
        int Cur_Endx = Cur_Startx + (n / apartx);

        int Cur_Starty = j * (n / aparty);
        int Cur_Endy = Cur_Starty + (n / aparty);
        int Cur_id = i * aparty + j;
        epicAssert(Cur_id == (node_id - 1) );
        matMultCPU_serial(a, b, c, n, Cur_Startx, Cur_Endx, Cur_Starty, Cur_Endy); 
        
        printf ("node %d Start synch Round %d\n", node_id, Round);
        fflush(stdout);
        
        Synchro();

        printf ("node %d end synch Round %d\n", node_id, Round);
        fflush(stdout);
    }

	////// end code  ///////
    printf ("node %d end calc\n", node_id);
    fflush(stdout);
	stop = clock();
    long End = get_time();
    if (is_master) {
        printf ("End\n");
        printf ("running time : %lld\n", End - Start);
        printf("CPU_Serial time: %3f ms\n", ((double)stop - start) / CLOCKS_PER_SEC * 1000.0); 
    }

    Transfer = alloc->getTransferredBytes() - Transfer;
    Racetime = alloc->getracetime() - Racetime; 
    Requesttime = alloc->getrequesttime() - Requesttime;
    printf ("Node %d transfer : %llu, racetime : %llu\n", node_id, Transfer, Racetime);

    printf ("node %d Start synch before_read\n", node_id);
    fflush(stdout);
    
    Synchro();

    printf ("node %d end synch before_read\n", node_id);
    fflush(stdout);

    float tmp;
    alloc->Read(c, &tmp, sizeof(float));
    //if (is_master) Answer_check (n, c);
    printf ("node %d Start synch\n", node_id);
    fflush(stdout);
    
    Synchro();

    printf ("node %d end synch\n", node_id);
    fflush(stdout);

	return 0;
}

int test_iterations = 1;

static void matMultCPU_serial(GAddr a, GAddr b, GAddr c, int n, int Startx, int Endx, int Starty, int Endy )
{ 
    static int first_time = 0;
    if (first_time == 0) {
        first_time = 1;
        printf ("(%d, %d) -> (%d, %d) \n", Startx, Starty, Endx, Endy);    
    }
    
    for (int oo = 0; oo < test_iterations; ++oo) {    
        for (int i = Startx; i < Endx; i++)
        {
            for (int j = Starty; j < Endy; j++) 
            {
                float t = 0; 
                for (int k = 0; k < n; k++)
                {
                    float val1 = 0, val2 = 0;
                    alloc->Read (a + (i * n + k) * sizeof(float), &val1, sizeof(float) ); 
                    alloc->Read (b + (k * n + j) * sizeof(float), &val2, sizeof(float) );
                    t += (float)val1 * val2;
                    //printf ("%.3f * %.3f = %.3f\n", val1, val2, t);
                    alloc->Write (c + (i * n + j) * sizeof(float), &t, sizeof(float) );  
                } 
                //Write_val(Cur_wh, c + (i * n + j) * sizeof(float), &t, sizeof(float) );
            }
        }
    }
}

int test_val = 1;

void genMat(GAddr arr, int n)
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
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

void justsetconf(Conf * old_conf) {
    Conf *conf = new Conf();
    (*conf) = (*old_conf);
    conf->loglevel = LOG_WARNING;
    GAllocFactory::SetConf(conf);
    // no setting master;
}

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

void Create_master(Conf * old_conf)
{
    Conf *conf = new Conf();
    (*conf) = (*old_conf);
    conf->loglevel = LOG_WARNING;
    GAllocFactory::SetConf(conf);
    master = new Master(*conf);
}

void Create_worker(Conf * old_conf)
{
    RdmaResource *res = new RdmaResource(curlist[0], false); 
    printf ("end get rdma source\n");
    fflush(stdout);

    Conf *conf = new Conf();
    (*conf) = (*old_conf);
    conf->worker_port -= num_worker;
    worker[num_worker] = new Worker(*conf, res);

    printf ("end get new worker\n");
    fflush(stdout);

    wh[num_worker] = new WorkerHandle(worker[num_worker]);

    printf ("end get new workerhandler\n");
    fflush(stdout);

    printf ("cur worker_port : %d\n", conf->worker_port);
    printf ("cur node_id : %d\n", wh[num_worker]->GetWorkerId() );
    printf ("\n");
    fflush(stdout);

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

void Put_val(WorkerHandle * Cur_wh, uint64_t key, const void *value, Size count)
{
  WorkRequest wr{};
  wr.op = PUT;
  wr.size = count;
  wr.key = key;
  wr.ptr = const_cast<void *>(value);

  if (Cur_wh->SendRequest(&wr))
  {
    epicLog(LOG_WARNING, "Put failed");
    return;
  }
}

void Get_val(WorkerHandle * Cur_wh, uint64_t key, void *value)
{
  WorkRequest wr{};
  wr.op = GET;
  wr.key = key;
  wr.ptr = value;

  if (Cur_wh->SendRequest(&wr))
  {
    epicLog(LOG_WARNING, "Get failed");
    return;
  }
}

int main(int argc, char *argv[]) {

    printf ("got here ???\n");
    fflush(stdout);

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

    int ok_link = 3;

    if (ok_link == 1) {
        alloc = GAllocFactory::CreateAllocator(&conf);
        printf ("node_number : %d, running_type : %d, array_number : %d\n", no_node, no_run, no_array);
        fflush(stdout);

        node_id = alloc->GetID();
        printf ("cur_node_id : %d, no_number : %d\n", node_id, no_node);
        fflush(stdout);
        Synchro();

        num_threads = no_node;

        printf ("test_alloc finish Init node %d\n", node_id);
        fflush(stdout);
        srand(time(NULL));
        // Init matrix
        GAddr a, b, c, d;
        int n = no_array; 
        if (is_master) {
            a = alloc->AlignedMalloc(sizeof(float) * n * n, 0, 0);
            printf ("node_id : %d, a_addr : %llu\n", node_id, a);
            fflush(stdout);

            alloc->Put(Start_val++, &a, sizeof(GAddr) );
        }
    
        else {
            alloc->Get(Start_val++, &a);
            printf ("node_id : %d, a_addr : %llu\n", node_id, a);
            fflush(stdout);
        }

        Synchro();
    
        printf ("end thread\n");
        fflush(stdout);
    }

    else if (ok_link == 2) {
        if (is_master) {
            alloc = GAllocFactory::CreateAllocator(&conf);
            node_id = alloc->GetID();
            printf ("cur_node_id : %d, no_number : %d\n", node_id, no_node);
            printf ("master node finish init\n");
            fflush(stdout);

            sleep(5);
        }

        else {
            curlist = ibv_get_device_list(NULL);
            for (int o = 0; o < 2; ++o) {
                Create_worker(&conf);
            }
            printf ("worker node finish init\n");
            fflush(stdout);

            sleep(5);
        }
    }

    else if (ok_link == 3) { //all using single machine style;
        curlist = ibv_get_device_list(NULL);
        if (is_master) {
            Create_master(&conf);
            for (int i = 0; i < 3; ++i) {
                Create_worker(&conf);
            }
            GAddr Cur_address = Malloc_addr(wh[2], BLOCK_SIZE, 0, 0);
            Put_val(wh[num_worker - 1], Start_val ++, &Cur_address, sizeof(GAddr));
            printf ("worker %d already put address %llu\n", wh[num_worker-1]->GetWorkerId(), Cur_address);
            fflush(stdout);

            int Tmp;
            Read_val(wh[num_worker - 1], Cur_address, &Tmp, sizeof(int) );
            printf ("cur_tmp : %d\n", Tmp);

            while (1) {
                Read_val(wh[num_worker - 1], Cur_address, &Tmp, sizeof(int) );
                printf ("cur_tmp : %d\n", Tmp);
                sleep(4);
            } // just wait
        }
        else {
            justsetconf(&conf);
            for (int i = 0; i < 2; ++i) {
                Create_worker(&conf);
            }
            GAddr Get_address;
            Get_val(wh[num_worker - 1], Start_val ++, &Get_address);
            printf ("worker %d already get address %llu\n", wh[num_worker-1]->GetWorkerId(), Get_address);
            int Tmp = 2;
            Write_val(wh[num_worker - 1], Get_address, &Tmp, sizeof(int) );

            while(1){
                
            }

            // alloc = GAllocFactory::CreateAllocator(&conf);
            // printf ("node_number : %d, running_type : %d, array_number : %d\n", no_node, no_run, no_array);
            // fflush(stdout);

            // node_id = alloc->GetID();
            // printf ("cur_node_id : %d, no_number : %d\n", node_id, no_node);
            // fflush(stdout);
        }
    }

    return 0;
}

// --no_node 2 --no_array 256 --ip_master 10.77.110.155 --ip_worker 10.77.110.155 --port_worker 6660 --is_master 1 --port_master 6666 --no_run 12 --is_read 1
// --no_node 2 --no_array 256 --ip_master 10.77.110.155 --ip_worker 10.77.110.158 --port_worker 6650 --is_master 0 --port_master 6666 --no_run 12 --is_read 1
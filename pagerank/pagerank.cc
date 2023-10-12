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

void Synchro_spe(int wh_id) {
    int id = 0;
    alloc_array[wh_id]->Put(sync_key[wh_id] + worker_id[wh_id], &worker_id[wh_id], sizeof(int) );
    for (int i = 1; i <= worker_num + 1; i++)
    {
        alloc_array[wh_id]->Get(sync_key[wh_id] + i, &id);
        epicAssert(id == i);
    }

    sync_key[wh_id] = sync_key[wh_id] + worker_num + ( (worker_num - 1) / no_node + 1) + 1;
}

void Synchro(int wh_id) {
    int id = 0;
    alloc_array[wh_id]->Put(sync_key[wh_id] + worker_id[wh_id], &worker_id[wh_id], sizeof(int) );
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

    //printf("end get new worker\n");
    fflush(stdout);

    wh[num_worker] = new WorkerHandle(worker[num_worker]);

    //printf("end get new workerhandler\n");
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
vector<int> Link[Maxn]; //记录入边
map<int , int> Id_map;
map<int , int> Rev_Id_map;

int cd[Maxn];
int num_threads = 4;
const int Maxm = 4;
int total_node = 0;
int total_edge = 0;

bool Read_file () {
	FILE * fp = fopen ("/home/xeg/last/gam/pagerank/DataSet/3.txt", "r");
	if (fp == NULL) {
        printf ("why got here ??\n");
		printf("File exception!\n");
		return false; 
	}

	printf ("start read_file\n");
	fflush(stdout);


	while (!feof(fp)) {
		int x, y;
		if (fscanf(fp, "%d\t%d\n", &x, &y)) {

			if (!Id_map[x]) {
				Id_map[x] = x;
				Rev_Id_map[x] = x;
				++ total_node;
			}
			if (!Id_map[y]) {
				Id_map[y] = y;
				Rev_Id_map[y] = y;
				++ total_node;
			}
			x = Id_map[x];
			y = Id_map[y];
			Link[y].push_back(x);
			cd[x] ++;

			total_edge ++;
		}
	}

	total_node = 50000;
	for (int i = 1; i <= total_node; ++i) {
		Id_map[i] = i;
		Rev_Id_map[i] = i;
	}

	//printf ("got here/ almost finish read!!!\n");
	//fflush(stdout);

	fclose(fp);
	return true;
}

struct cs {
	GAddr W1, W2, N1, E1;
}; //used to calc_pagerank 

struct node {
	double Value;
	int id;
}Final[Maxn]; // used to sort pagerank

bool cmp(node c, node d) {
	return c.Value > d.Value; 
}

GAddr Thread_End = 0;

Conf Big_conf;

uint64 read_miss[10], read_hit[10], write_miss[10], write_hit[10];

void Record_stats(int wh_id) {
    GAlloc * alloc = alloc_array[wh_id];
    uint64_t cur_readmiss = 0, cur_writemiss = 0, cur_readhit = 0, cur_writehit = 0;
    cur_readmiss = alloc->getreadmiss();
    cur_readhit = alloc->getreadhit();
    cur_writemiss = alloc->getwritemiss();
    cur_writehit = alloc->getwritehit();
    read_miss[wh_id] = cur_readmiss - read_miss[wh_id];
    read_hit[wh_id] = cur_readhit - read_hit[wh_id];
    write_miss[wh_id] = cur_writemiss - write_miss[wh_id];
    write_hit[wh_id] = cur_writehit - write_hit[wh_id];
}

void print_stats (int wh_id) {
	Record_stats(wh_id);
	printf ("worker[%d] : (read_miss: %lld), (read_hit: %lld), (write_miss: %lld), (write_hit: %lld)\n",
    worker_id[wh_id], read_miss[wh_id], read_hit[wh_id], write_miss[wh_id], write_hit[wh_id]);
    fflush(stdout);
}

void Behind_write(GAlloc * alloc, GAddr addr1, GAddr addr2,int n) {
	//后台线程不断修改
	while (1) {
        int Cur_val = 0;
        alloc->Read(Thread_End, &Cur_val, sizeof(int) );
        if (Cur_val == 1) {
            printf ("behind_thread ended\n");
            fflush(stdout);
            return;
        }
		for (int i = 0; i < n; i += (BLOCK_SIZE / sizeof(double) ) ) {
			double tmp = 1.0 / (1.0 * n);
			alloc->Write(addr1 + i * sizeof(double), &tmp, sizeof(double) );
			alloc->Write(addr2 + i * sizeof(double), &tmp, sizeof(double) );
		}
        //sleep(0.1);
	}
}

void Calc_pagerank(int wh_id, int Start, int End, int N, int rev, int thread_id, int n_thread, double damping_factor, cs T) {
    static int first_time = 0;
    if (first_time == 0) {
        first_time = 1;
        printf("Start : %d, End : %d\n", Start, End);
    }
	GAddr Weight[2];
	GAddr NoWhere;
	GAddr BUG;
	Weight[0] = T.W1;
	Weight[1] = T.W2;
	NoWhere = T.N1;
	BUG = T.E1;

	double no_where = 0.0;
	for (int i = 0; i < n_thread; ++i) {
		double tmp;
		alloc_array[wh_id]->Read(NoWhere + i * sizeof(double), &tmp, sizeof(double) );
		no_where += tmp;
	}

	double Cur_nowhere = 0.0;
	double Cur_error = 0.0;
	for (int i = Start; i <= End; ++i) {
		double x = 0.0;
		for (int j = 0; j < (int)Link[i].size(); ++j) {
			int From = Link[i][j];
			double tmp;
			alloc_array[wh_id]->Read(Weight[rev^1] + (From - 1) * sizeof(double), &tmp, sizeof(double) );
			x += tmp / cd[From];
		}
		x = (damping_factor * (x + (no_where / (1.0 * N) ) ) + (1.0 - damping_factor) / (1.0 * N) );
		double Last;
		alloc_array[wh_id]->Read(Weight[rev^1] + (i - 1) * sizeof(double), &Last, sizeof(double) );
		Cur_error += fabs(x - Last);
		alloc_array[wh_id]->Write(Weight[rev] + (i - 1) * sizeof(double), &x, sizeof(double) );

		if (!cd[i]) {
			Cur_nowhere += x;
		}
	}

	alloc_array[wh_id]->Write(NoWhere + thread_id * sizeof(double), &Cur_nowhere, sizeof(double) );
	alloc_array[wh_id]->Write(BUG + thread_id * sizeof(double), &Cur_error, sizeof(double) );
}

void PageRank(double damping_factor, int wh_id) {
	GAddr Weight[2];
	GAddr NoWhere;
	GAddr BUG;
    int Start_val = 141;

    if (is_master && wh_id == 0) {
        if (no_run == 2) { //bi
            Weight[0] = alloc_array[wh_id]->AlignedMalloc(sizeof(double) * (total_node + 1), b_i, 0); 
            Weight[1] = alloc_array[wh_id]->AlignedMalloc(sizeof(double) * (total_node + 1), b_i, 0); 
            NoWhere = alloc_array[wh_id]->AlignedMalloc(sizeof(double) * (num_threads + 1), b_i, 0); 
            BUG = alloc_array[wh_id]->AlignedMalloc(sizeof(double) * (num_threads + 1), b_i, 0);
        }

        else if (no_run == 1) {//msi
            Weight[0] = alloc_array[wh_id]->AlignedMalloc(sizeof(double) * (total_node + 1), 0, 0);
            Weight[1] = alloc_array[wh_id]->AlignedMalloc(sizeof(double) * (total_node + 1), 0, 0);
            NoWhere = alloc_array[wh_id]->AlignedMalloc(sizeof(double) * (num_threads + 1), 0, 0);
            BUG = alloc_array[wh_id]->AlignedMalloc(sizeof(double) * (num_threads + 1), 0, 0);
        }

        Thread_End = alloc_array[wh_id]->AlignedMalloc(BLOCK_SIZE, 0, 0);

        alloc_array[wh_id]->Put(Start_val++, &Weight[0], sizeof(GAddr) );
        alloc_array[wh_id]->Put(Start_val++, &Weight[1], sizeof(GAddr) );
        alloc_array[wh_id]->Put(Start_val++, &NoWhere, sizeof(GAddr) );
        alloc_array[wh_id]->Put(Start_val++, &BUG, sizeof(GAddr) );
        alloc_array[wh_id]->Put(Start_val++, &Thread_End, sizeof(GAddr) );
    }
    else {
        alloc_array[wh_id]->Get(Start_val++, &Weight[0]);
        alloc_array[wh_id]->Get(Start_val++, &Weight[1]);
        alloc_array[wh_id]->Get(Start_val++, &NoWhere);
        alloc_array[wh_id]->Get(Start_val++, &BUG);
        alloc_array[wh_id]->Get(Start_val++, &Thread_End);

        printf ("worker %d got weight[0] : %lld\n", worker_id[wh_id], Weight[0]);
        fflush(stdout);
    }

	int Max_iteration = 20;
	int apartx = total_node / num_threads;

	double no_where = 0.0;

    if (is_master && wh_id == 0) {
        for (int i = 1; i <= total_node; ++i) {
            double tmp = 1.0 / (1.0 * total_node);
            if (cd[i] == 0) no_where += tmp;
            alloc_array[wh_id]->Write(Weight[0] + (i - 1) * sizeof(double), &tmp, sizeof(double) );
        }

        for (int i = 0; i < num_threads; ++i) {
            double tmp = no_where / (1.0 * num_threads);
            alloc_array[wh_id]->Write(NoWhere + i * sizeof(double), &tmp, sizeof(double) );
        }
    }

    Synchro_spe(wh_id);
    printf ("node %d finish first sync\n", node_id);
    fflush(stdout);

    if (worker_id[wh_id] > worker_num){
        printf ("start behind_thread\n");
        fflush(stdout);
        Behind_write(alloc_array[wh_id], Weight[0], Weight[1], total_node);
        return;
    } // behind_thread 

    long Start_time = get_time();
	Record_stats(wh_id);
	for (int i = 1; i <= Max_iteration; ++i) {
        // printf ("node : %d, round Start\n", node_id);
        //fflush(stdout);

		double no_where = 0.0;

		int j = (worker_id[wh_id] - 1);
        int CurStart = j * apartx + 1;
        int CurEnd = (j+1) * apartx;
        if (j == num_threads - 1) CurEnd = total_node;
        cs T;
        T.W1 = Weight[0];
        T.W2 = Weight[1];
        T.N1 = NoWhere;
        T.E1 = BUG;

        Calc_pagerank(wh_id, CurStart, CurEnd, total_node, (i&1), j, num_threads, 0.85, T);

        Synchro (wh_id);

		double Cur_error = 0.0;
        if (is_master && wh_id == 0) {
            for (int j = 0; j < num_threads; ++j) {
                double tmp;
                alloc_array[wh_id]->Read(BUG + j * sizeof(double), &tmp, sizeof(double) );
                Cur_error += tmp;
            }
            printf ("round %d error : %.5f\n", i, Cur_error);
            fflush(stdout);
        }

		continue; 
	}

	long End_time = get_time();
	
	print_stats (wh_id);
    if (is_master && wh_id == 0) {
        printf ("running time : %lld\n", End_time - Start_time);
        fflush(stdout);

        for (int i = 1; i <= total_node; ++i) {
            Final[i].id = i;
            double tmp;
            alloc_array[wh_id]->Read(Weight[0] + (i-1) * sizeof(double), &tmp, sizeof(double) );
            Final[i].Value = tmp;
        }

        printf ("total_node : %d, total_edge : %d\n", total_node, total_edge);
        sort(Final + 1, Final + total_node + 1, cmp);
        for (int i = 1; i <= min(total_node, 10); ++i) printf ("node : %d, pagerank : %.10f\n", Rev_Id_map[Final[i].id], Final[i].Value);
        int cur_val = 1;
        printf ("end behind_thread\n");
        fflush(stdout);
        alloc_array[wh_id]->Write(Thread_End, &cur_val, sizeof(int) );
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
    Big_conf = conf;

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

    /* behind worker create */
    if (node_id == no_node) { // 在最后一个节点多建一个worker
        for (int i = 0; i < ( (worker_num - 1) / no_node + 1); ++i) {
            Create_worker(&conf);
            alloc_array.push_back(new GAlloc(worker[local_thread_num]) );
            local_thread_num ++;
        }
    }

    for (int i = 0; i < local_thread_num; ++i) {
        sync_key[i] = SYNC_KEY;
    }
    /* read graph data */
    bool read_ok = Read_file ();

    /* start pagerank */
    thread threads[local_thread_num + 2];

    for (int i = 0; i < local_thread_num; ++i) {
        threads[i] = thread (PageRank, 0.85, i);
    }
    for (int i = 0; i < local_thread_num; ++i) {
        threads[i].join();
    }

    sleep(2);
    return 0;
}
#include <cstdio>
#include <cstdlib>
#include <vector>
#include <cmath>
#include <map>
#include <thread>
#include <pthread.h>
#include <algorithm>
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

int Max_node = 0;
const int Maxn = 1000010;
vector<int> Link[Maxn]; //记录入边
map<int , int> Id_map;
map<int , int> Rev_Id_map;

int cd[Maxn];
const int num_threads = 4;
int node_id = 0;

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
        wr.flag = ASYNC; // 可以在这里调 
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
    for (int i = 0; i < num_threads; ++i)
    {
        Create_worker();
    }

    sleep(1);
}

bool Read_file () {
	FILE * fp = fopen ("DataSet/2.txt", "r");
	if (fp == NULL) {
		printf("File exception!\n");
		return false; 
	}

	printf ("start read_file\n");
	fflush(stdout);

	while (!feof(fp)) {
		int x, y;
		if (fscanf(fp, "%d\t%d\n", &x, &y)) {
			//printf ("x : %d, y : %d\n", x, y);
			//fflush(stdout);
			if (!Id_map[x]) {
				Id_map[x] = ++node_id;
				Rev_Id_map[node_id] = x;
			}
			if (!Id_map[y]) {
				Id_map[y] = ++node_id;
				Rev_Id_map[node_id] = y;
			}
			x = Id_map[x];
			y = Id_map[y];
			Link[y].push_back(x);
			cd[x] ++;
			//printf ("round done\n");
			//fflush(stdout);
		}
	}

	//printf ("got here/ almost finish read!!!\n");
	//fflush(stdout);

	fclose(fp);
	return true;
}

struct cs {
	GAddr W1, W2, N1, E1;
};

void Calc_pagerank(WorkerHandle * Cur_wh, int Start, int End, int N, int rev, int thread_id, int n_thread, double damping_factor, cs T) {
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
		Read_val(Cur_wh, NoWhere + i * sizeof(double), &tmp, sizeof(double) );
		no_where += tmp;
	}

	double Cur_nowhere = 0.0;
	double Cur_error = 0.0;
	for (int i = Start; i <= End; ++i) {
		double x = 0.0;
		for (int j = 0; j < (int)Link[i].size(); ++j) {
			int From = Link[i][j];
			double tmp;
			Read_val(Cur_wh, Weight[rev^1] + (From - 1) * sizeof(double), &tmp, sizeof(double) );
			x += tmp / cd[From];
		}
		x = (damping_factor * (x + (no_where / (1.0 * N) ) ) + (1.0 - damping_factor) / (1.0 * N) );
		double Last;
		Read_val(Cur_wh, Weight[rev^1] + (i - 1) * sizeof(double), &Last, sizeof(double) );
		Cur_error += fabs(x - Last);
		Write_val(Cur_wh, Weight[rev] + (i - 1) * sizeof(double), &x, sizeof(double) );

		if (!cd[i]) {
			Cur_nowhere += x;
		}
	}

	Write_val(Cur_wh, NoWhere + thread_id * sizeof(double), &Cur_nowhere, sizeof(double) );
	Write_val(Cur_wh, BUG + thread_id * sizeof(double), &Cur_error, sizeof(double) );
}

bool Check(double Cur_error) {
	if (Cur_error <= 1e-5) return true;
	return false;
}

void Print_debug() {
	for (int i = 1; i <= node_id; ++i) {
		//printf ("node_id : %d, pagerank : %.10f\n", Rev_Id_map[i], Val[0][i]);
	}
}

struct node {
	double Value;
	int id;
}Final[Maxn];

void PageRank(double damping_factor) {
	GAddr Weight[2];
	GAddr NoWhere;
	GAddr BUG;

	Weight[0] = Malloc_addr(wh[0], sizeof(double) * (node_id + 1), b_i, 0); 
	Weight[1] = Malloc_addr(wh[0], sizeof(double) * (node_id + 1), b_i, 0); 
	NoWhere = Malloc_addr(wh[0], sizeof(double) * (num_threads + 1), b_i, 0); 
	BUG = Malloc_addr(wh[0], sizeof(double) * (num_threads + 1), b_i, 0);  

	// Weight[0] = Malloc_addr(wh[0], sizeof(double) * (node_id + 1), 0, 0);
	// Weight[1] = Malloc_addr(wh[0], sizeof(double) * (node_id + 1), 0, 0);
	// NoWhere = Malloc_addr(wh[0], sizeof(double) * (num_threads + 1), 0, 0);
	// BUG = Malloc_addr(wh[0], sizeof(double) * (num_threads + 1), 0, 0);

	int Max_iteration = 50;
	int apartx = node_id / num_threads;
	thread threads[num_threads]; 

	double no_where = 0.0;
	long Start_time = get_time();

	for (int i = 1; i <= node_id; ++i) {
		double tmp = 1.0 / (1.0 * node_id);
		if (cd[i] == 0) no_where += tmp;
		Write_val(wh[0], Weight[0] + (i - 1) * sizeof(double), &tmp, sizeof(double) );
	}

	for (int i = 0; i < num_threads; ++i) {
		double tmp = no_where / (1.0 * num_threads);
		Write_val(wh[0], NoWhere + i * sizeof(double), &tmp, sizeof(double) );
	}

	for (int i = 1; i <= Max_iteration; ++i) {
		double no_where = 0.0;
		for (int j = 0; j < num_threads; ++j) {
			int CurStart = j * apartx + 1;
			int CurEnd = (j+1) * apartx;
			if (j == num_threads - 1) CurEnd = node_id;
			cs T;
			T.W1 = Weight[0];
			T.W2 = Weight[1];
			T.N1 = NoWhere;
			T.E1 = BUG;
			threads[j] = thread(Calc_pagerank, wh[j], CurStart, CurEnd, node_id, (i&1), j, num_threads, 0.85, T);
		}
		for (int j = 0; j < num_threads; ++j) {
            threads[j].join();
        }
		double Cur_error = 0.0;
		for (int j = 0; j < num_threads; ++j) {
			double tmp;
			Read_val(wh[0], BUG + j * sizeof(double), &tmp, sizeof(double) );
			Cur_error += tmp;
		}

		printf ("round %d error : %.5f\n", i, Cur_error);
		//Print_debug();
		//Cur_error = Cur_error / (1.0 * node_id);
		if (Check(Cur_error)) {
			printf ("end at round %d\n", i);
			break;
		}
	}

	long End_time = get_time();
	printf ("running time : %lld\n", End_time - Start_time);

	for (int i = 1; i <= node_id; ++i) {
		Final[i].id = i;
		double tmp;
		Read_val(wh[0], Weight[0] + (i-1) * sizeof(double), &tmp, sizeof(double) );
		Final[i].Value = tmp;
	}
}

bool cmp(node c, node d) {
	return c.Value > d.Value; 
}

int main() {
	Init_Gam();
	printf ("finish Init\n");
	fflush(stdout);
	bool read_ok = Read_file ();
	printf ("read_file done\n");
	fflush (stdout);
	PageRank(0.85);
	printf ("%d\n", node_id);
	sort(Final + 1, Final + node_id + 1, cmp);
	for (int i = 1; i <= min(node_id, 10); ++i) printf ("node_id : %d, pagerank : %.10f\n", Rev_Id_map[Final[i].id], Final[i].Value);

	return 0;
}
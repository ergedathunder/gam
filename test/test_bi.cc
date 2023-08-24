#include <iostream>
#include <cstdio>
#include <cstdlib>
#include <vector>
#include <cmath>
#include <map>
#include <thread>
#include <pthread.h>
#include <algorithm>

int Max_node = 0;
const int Maxn = 1000010;
std::vector<int> Link[Maxn]; //记录入边
std::map<int , int> Id_map;
std::map<int , int> Rev_Id_map;

double Val[2][Maxn];
int cd[Maxn];
const int num_threads = 4;
int node_id = 0;
double nowhere[num_threads];
double Error[num_threads];

bool Read_file (char * filename) {
	FILE * fp = fopen (filename, "r");
	if (fp == NULL) {
		printf("File exception!\n");
		return false;
	}

	while (!feof(fp)) {
		int x, y;
		if (fscanf(fp, "%d\t%d\n", &x, &y)) {
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
		}
	}
}

void Calc_pagerank(int Start, int End, int N, int rev, int thread_id, int n_thread, double damping_factor) {
	double no_where = 0.0;
	for (int i = 0; i < n_thread; ++i) {
		no_where += nowhere[i];
	}

	double Cur_nowhere = 0.0;
	double Cur_error = 0.0;
	for (int i = Start; i <= End; ++i) {
		double x = 0.0;
		for (int j = 0; j < (int)Link[i].size(); ++j) {
			int From = Link[i][j];
			x += Val[rev^1][From] / cd[From];
		}
		x = (damping_factor * (x + (no_where / (1.0 * N) ) ) + (1.0 - damping_factor) / (1.0 * N) );
		Val[rev][i] = x;

		if (!cd[i]) {
			Cur_nowhere += x;
		}
		Cur_error += fabs(Val[rev][i] - Val[rev^1][i]);
	}

	nowhere[thread_id] = Cur_nowhere;
	Error[thread_id] = Cur_error;
}

bool Check(double Cur_error) {
	if (Cur_error <= 1e-5) return true;
	return false;
}

void Print_debug() {
	for (int i = 1; i <= node_id; ++i) {
		printf ("node_id : %d, pagerank : %.10f\n", Rev_Id_map[i], Val[0][i]);
	}
}

void PageRank(double damping_factor) {
	int Max_iteration = 100;
	int apartx = node_id / num_threads;
	std::thread threads[num_threads]; 

	double no_where = 0.0;

	for (int i = 1; i <= node_id; ++i) {
		Val[0][i] = 1.0 / (1.0 * node_id);
		if (cd[i] == 0) no_where += Val[0][i];
	}

	for (int i = 0; i < num_threads; ++i) nowhere[i] = (no_where / (1.0 * num_threads) );

	for (int i = 1; i <= Max_iteration; ++i) {
		double no_where = 0.0;
		for (int j = 0; j < num_threads; ++j) {
			int CurStart = j * apartx + 1;
			int CurEnd = (j+1) * apartx;
			if (j == num_threads - 1) CurEnd = node_id;
			threads[j] = std::thread(Calc_pagerank, CurStart, CurEnd, node_id, (i&1), j, num_threads, damping_factor);
		}
		for (int j = 0; j < num_threads; ++j) {
            threads[j].join();
        }
		double Cur_error = 0.0;
		for (int j = 0; j < num_threads; ++j) {
			Cur_error += Error[j];
		}
		printf ("round %d error : %.5f\n", i, Cur_error);
		//Print_debug();
		//Cur_error = Cur_error / (1.0 * node_id);
		if (Check(Cur_error)) {
			printf ("end at round %d\n", i);
			break;
		}
	}
}

struct node {
	double Value;
	int id;
}Final[Maxn];

bool cmp(node c, node d) {
	return c.Value > d.Value;
}

int main() {
	Read_file ("DataSet/2.txt");
	PageRank(0.85);
	for (int i = 1; i <= node_id; ++i) {
		Final[i].id = i;
		Final[i].Value = Val[0][i];
	}
	printf ("%d\n", node_id);
	std::sort(Final + 1, Final + node_id + 1, cmp);
	for (int i = 1; i <= std::min(node_id, 10); ++i) printf ("node_id : %d, pagerank : %.10f\n", Rev_Id_map[Final[i].id], Final[i].Value);

	return 0;
}
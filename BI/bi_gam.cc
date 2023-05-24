#include <cstring>
#include <iostream>
#include <thread>
#include "structure.h"
#include "worker.h"
#include "settings.h"
#include "worker_handle.h"
#include "master.h"
#include "gallocator.h"
#include "workrequest.h"

WorkerHandle *wh1_1, *wh1_2, *wh2_1, *wh2_2, *wh3_1, *wh3_2;
Size size = BLOCK_SIZE;  //eight blocks

WorkerHandle * wh[1000];
ibv_device **curlist;
Worker * worker[100];
Master * master;
int num_worker = 0;

void Create_master() {
  Conf* conf = new Conf();
  conf->loglevel = LOG_WARNING;
  //conf->loglevel = LOG_DEBUG;
  GAllocFactory::SetConf(conf);
  master = new Master(*conf);
}

void Create_worker() {
  Conf* conf = new Conf();
  RdmaResource* res = new RdmaResource(curlist[0], false);
  conf->worker_port += num_worker;
  worker[num_worker] = new Worker(*conf, res);
  wh[num_worker] = new WorkerHandle(worker[num_worker]);
  num_worker ++;
}

GAddr Malloc_addr(WorkerHandle * Cur_wh, const Size size, Flag flag, int Owner) {
#ifdef LOCAL_MEMORY_HOOK
  void* laddr = zmalloc(size);
  return (GAddr)laddr;
#else
  WorkRequest wr = { };
  wr.op = MALLOC;
  wr.flag = flag;
  wr.size = size;
  /* add ergeda add */
  wr.arg = (uint64_t) Owner; //arg没啥用，刚好用来存储owner
  /* add ergeda add */

  if (Cur_wh->SendRequest(&wr)) {
    epicLog(LOG_WARNING, "malloc failed");
    return Gnullptr;
  } else {
    epicLog(LOG_DEBUG, "addr = %x:%lx", WID(wr.addr), OFF(wr.addr));
    return wr.addr;
  }
#endif
}

void Read_val (WorkerHandle * Cur_wh, GAddr addr, int * val) {
  WorkRequest wr { };
  wr.op = READ;
  wr.flag = 0;
  wr.size = sizeof(int);
  wr.addr = addr;
  wr.ptr = (void*)val;
  if (Cur_wh->SendRequest(&wr)) {
    epicLog(LOG_WARNING, "send request failed");
  }
}

void Write_val (WorkerHandle * Cur_wh, GAddr addr, int * val) {
  WorkRequest wr { };
  wr.op = WRITE;
  wr.flag = ASYNC; //可以在这里调
  wr.size = sizeof(int);
  wr.addr = addr;
  wr.ptr = (void*)val;
  if (Cur_wh->SendRequest(&wr)) {
    epicLog(LOG_WARNING, "send request failed");
  }
}

typedef struct
{
	double p_t0;
	double p_t1;
	double e;
	int *From_id;
	int con_size;
	int from_size;
}Node;

#define maxThreads 64

/******************** Defines ****************/
// Number of nodes
int N, num_threads;

// Convergence threashold and algorithm's parameter d  
double threshold, d;

//Table of threads
pthread_t *Threads;

// Table with thread's data
Thread *Threads_data;

// Table of node's data
Node *Nodes;
pthread_mutex_t lockP = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t locksum = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t lockmax = PTHREAD_MUTEX_INITIALIZER;

// Number of threads
int num_threads;

// Number of iterations
int iterations = 0;

double max_error = 1;
double sum = 0;

/***** Memory allocation - Initializations for Threads *****/

void Threads_Allocation()
{

	int i;
	double N_split =  (double) N / num_threads;
	
	// Allocate memory for threads
	Threads = (pthread_t *)malloc(num_threads * sizeof(pthread_t));

	// Stores thread's data		
	Threads_data = (Thread*)malloc(num_threads * sizeof(Thread));	
	
	// Split dataset into subsets, given to each thread
	Threads_data[0].tid = 0;
	Threads_data[0].start = 0;
	Threads_data[0].end = floor(N_split);

	for (i = 1; i < num_threads; i++)
	{
		Threads_data[i].tid = i;
		Threads_data[i].start = Threads_data[i - 1].end;
		if (i < (num_threads - 1))
		{
			Threads_data[i].end = Threads_data[i].start + floor(N_split);
		}
		else
		{
			Threads_data[i].end = N;
		}
	}
	
	printf("\n");

	for (i = 0; i < num_threads; i++)
	{
		printf("Thread %d, start = %d, end = %d\n", Threads_data[i].tid, Threads_data[i].start, Threads_data[i].end);
	}

	printf("\n");

}

/***** Memory allocation - Initializations for Nodes *****/
void Nodes_Allocation()
{

	int i;
	Nodes = (Node*)malloc(N*sizeof(Node));
    
    for (i = 0; i < N; i++)
	{
		Nodes[i].con_size = 0;
		Nodes[i].from_size = 0;
        Nodes[i].From_id = (int*) malloc(sizeof(int));
    }	

}

/***** Read graph connections from txt file *****/	

void Read_from_txt_file(char* filename)
{
    
    FILE *fid;

    int from_idx, to_idx;
	int temp_size;
	char line[1000];

    fid = fopen("web-Google.txt", "r");
   	if (fid == NULL){printf("Error opening the file\n");}

	while (!feof(fid))
	{
		fgets(line, sizeof(line), fid);
		// ignore sentences starting from #
		if (sscanf(line,"%d\t%d\n", &from_idx, &to_idx))
		{
			Nodes[from_idx].con_size++;
			Nodes[to_idx].from_size++;
			temp_size = Nodes[to_idx].from_size;
			Nodes[to_idx].From_id = (int*) realloc(Nodes[to_idx].From_id, temp_size * sizeof(int));
			Nodes[to_idx].From_id[temp_size - 1] = from_idx; 
		}
	}

	printf("End of connections insertion!\n");

	fclose(fid);

}

/***** Read P vector from txt file*****/	

void Read_P_from_txt_file()
{

	FILE *fid;
	double temp_P;
	int index = 0;

    fid = fopen("P.txt", "r");
   	if (fid == NULL){printf("Error opening the Probabilities file\n");}

	while (!feof(fid))
	{
		// P's values are double!
		if (fscanf(fid,"%lf\n", &temp_P))
		{
			Nodes[index].p_t1 = temp_P;
			index++;	   
		}
	}
	printf("End of P insertion!");

	fclose(fid);	

}


/***** Read E vector from txt file*****/	

void Read_E_from_txt_file()
{

	FILE *fid;
	double temp_E;
	int index = 0;
	
    fid = fopen("E.txt", "r");
   	if (fid == NULL){printf("Error opening the E file\n");}

	while (!feof(fid))
	{
		// E's values are double!
		if (fscanf(fid,"%lf\n", &temp_E))
		{
			Nodes[index].e = temp_E;
			index++;   
		}
	}
	printf("End of E insertion!");

	fclose(fid);	

}

/***** Create P and E with equal probability *****/

void Random_P_E()
{

   	int i;
    // Sum of P (it must be =1)
    double sum_P_1 = 0;
    // Sum of E (it must be =1)
    double sum_E_1 = 0; 
    
    
    // Arrays initialization
    for (i = 0; i < N; i++)
    {
        Nodes[i].p_t0 = 0;
        Nodes[i].p_t1 = 1;
        Nodes[i].p_t1 = (double) Nodes[i].p_t1 / N;

        sum_P_1 = sum_P_1 + Nodes[i].p_t1;
        
		Nodes[i].e = 1;
        Nodes[i].e = (double) Nodes[i].e / N;
        sum_E_1 = sum_E_1 + Nodes[i].e;
    }

    // Assert sum of probabilities is =1
    
    // Print sum of P (it must be =1)
    //printf("Sum of P = %f\n",sum_P_1);
    
    // Exit if sum of P is !=1
    assert(sum_P_1 = 1);
    
    //printf("\n");
    
    // Print sum of E (it must be =1)
    //printf("Sum of E = %f\n",sum_E_1);
    
    // Exit if sum of Pt0 is !=1
    assert(sum_E_1 = 1);

}

/***** Re-initialize P(t) and P(t + 1) values *****/

void* P_reinit(void* arg)
{

	Thread *thread_data = (Thread *)arg;
	int i;

	for (i = thread_data->start; i < thread_data->end; i++)
	{
			Nodes[i].p_t0 = Nodes[i].p_t1;	
			Nodes[i].p_t1 = 0;
	}
	return 0;
}

/***** Main parallel algorithm *****/

void* Pagerank_Parallel(void* arg)
{

	Thread *thread_data = (Thread *) arg;
	int i, j, index;

	// Every thread will compute a local sum and add it
	// to the global one
	double temp_sum = 0;

	for (i = thread_data->start; i < thread_data->end; i++)
	{
		if (Nodes[i].con_size == 0)
		{
			 temp_sum = temp_sum + (double) Nodes[i].p_t0 / N;
		}

		if (Nodes[i].from_size != 0)
        {
            // Compute the total probability, contributed by node's neighbors
            for (j = 0; j < Nodes[i].from_size; j++)
            {
				index = Nodes[i].From_id[j];	
				Nodes[i].p_t1 = Nodes[i].p_t1 + (double) Nodes[index].p_t0 / Nodes[index].con_size;
			}
        }		
	}
	
	// This is an atomic operation
	pthread_mutex_lock(&locksum);
	sum = sum + temp_sum; 
	pthread_mutex_unlock(&locksum);
	return 0;
}

/***** Compute local max (thread's data max) *****/
void* Local_Max(void* arg)
{

	Thread *thread_data = (Thread *) arg;
	int i, j;
	
	// Every thread will find a local max and then check
	// if this is a global one
	double temp_max = -1;

	for (i = thread_data->start; i < thread_data->end; i++)
	{
		Nodes[i].p_t1 = d * (Nodes[i].p_t1 + sum) + (1 - d) * Nodes[i].e;
 
        if (fabs(Nodes[i].p_t1 - Nodes[i].p_t0) > temp_max)
        {
            temp_max  = fabs(Nodes[i].p_t1 - Nodes[i].p_t0);
        }		
	}

	// Check if we have a new global max
	// This is an atomic operaiton
	pthread_mutex_lock(&lockmax);
	
	if (max_error  < temp_max)
	{			
		max_error = temp_max;		
	}	
	pthread_mutex_unlock(&lockmax);	
	return 0;
}

/***** Pagerank main algortihm *****/
void Pagerank()
{

 	/***** Start of algorithm *****/
	
    int i, j, index;
	
	// Continue if we don't have convergence yet
    while (max_error > threshold)
    {
    	max_error = -1;
		sum = 0;

		// P array re-Initialization
        for (i = 0; i < num_threads; i++)
        {
			pthread_create(&Threads[i], NULL, &P_reinit,(void*) &Threads_data[i]);
		}

		// Wait for all threads to "catch" this point
		for (i = 0; i < num_threads; i++)
		{
			pthread_join(Threads[i], NULL);
        }


        // Find P for each webpage
        for (i = 0; i < num_threads; i++)
        {
            pthread_create(&Threads[i], NULL, &Pagerank_Parallel, (void*) &Threads_data[i]);   
        }

		for (i = 0; i < num_threads; i++)
		{
			pthread_join(Threads[i], NULL);
		}


		// Find local and global max
		for (i = 0; i < num_threads; i++)
        {
            pthread_create(&Threads[i], NULL, &Local_Max, (void*) &Threads_data[i]);   
        }

		for (i = 0; i < num_threads; i++)
		{
			pthread_join(Threads[i], NULL);
		}
        
        printf("Max Error in iteration %d = %f\n", iterations+1, max_error);
        iterations++;
    }

}


/***** main function *****/   

int main(int argc, char** argv)
{
    Create_master();
    for (int i = 0; i < 3; ++i) {
        Create_worker();
    }

    struct timeval start, end;
    
    int i,j,k;
	double totaltime;
	
	// Check input arguments
	if (argc < 5)
	{
		printf("Error in arguments! Three arguments required: graph filename, N, threshold and d\n");
		return 0;
	}

	// get arguments 
	char filename[256];
	strcpy(filename, argv[1]);
	N = atoi(argv[2]);
	threshold = atof(argv[3]);
	d = atof(argv[4]); 
	num_threads = atoi(argv[5]);

	// Check input arguments
	if ((num_threads < 1) || (num_threads > maxThreads)) 
	{
		printf("Threads number must be >= 1 and  <= %d!\n", maxThreads);
		exit(1);
	}

	Threads_Allocation();
	Nodes_Allocation();
	
	// OR read probabilities from files
    Read_from_txt_file(filename);
	//Read_P_from_txt_file();
	//Read_E_from_txt_file();

    Random_P_E();

    printf("\n");

    printf("Parallel version of Pagerank\n");

    gettimeofday(&start, NULL);
    Pagerank();
	gettimeofday(&end, NULL);  

    /*for (i = 0; i < N; i++)
    {
        printf("P_t1[%d] = %f\n",i, Nodes[i].p_t1);
    }
    
    printf("\n");*/
    
	// Print no of iterations
    printf("Total iterations: %d\n", iterations);


    totaltime = (((end.tv_usec - start.tv_usec) / 1.0e6+ end.tv_sec - start.tv_sec) * 1000) / 1000;

	printf("\nTotaltime = %f seconds\n", totaltime);
    
    printf("End of program!\n");
    double Max_pagerank = 0.0;
    int Max_id = 0;
    for (int i = 0; i < 100; ++i) {
        if (Nodes[i].p_t1 > 0.00)
        printf ("i : %d, pagerank : %f\n", i, Nodes[i].p_t1);
    }
    for (int i = 0; i < N; ++i) {
        if (Nodes[i].p_t1 > Max_pagerank) {
            Max_pagerank = Nodes[i].p_t1;
            Max_id = i;
        }
    }
    
    printf ("Max_id : %d, Max_pagerank : %f\n", Max_id, Max_pagerank);

    return (EXIT_SUCCESS);
}
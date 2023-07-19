// Basic C code for Gaussian elimination without pivoting    
//    for the POSIX thread standard.                        
//    Compile with "gcc -o gauss gauss_col.c -lpthread"  
//    No error checking                                   
#include <stdio.h>
#include <stdlib.h>
#include <sys/times.h>
#include <limits.h>
#include <math.h>
#include <string.h>
#include <sys/types.h>
#include <sys/times.h>
#include <sys/time.h>
#include <pthread.h>
#include <unistd.h>

#define _REENTRANT
#define min(a, b)            ((a) < (b)) ? (a) : (b)


// given A * X = B; to solve for X.            
int N;  	// matrix size
int M;  	// number of threads, must >=1
int vFlag, *idx;
double *matA, *X, *B, **colA,**rowA, *colZ;
int NumThreads;
#define MAXN 20000     // keep ECE's server running
#define MAXN4print 20  // safty, not to overload ECE server
// thread information
pthread_t idThreads[_POSIX_THREAD_THREADS_MAX];
pthread_mutex_t Mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t CountLock = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t NextIter = PTHREAD_COND_INITIALIZER;

//copy from gaussPT.c
int Norm, CurrentRow, Count;
void create_threads(void);
void *gaussPT(void *);
int find_next_row_chunk(int *, int );
void barrier(int * );
void wait_for_threads(void);
unsigned int time_seed(void);
void parameters(int , char **);
void initialise_inputs(void);
void print_inputs(void);
void PrintX(void);
void printAB(void);
void pivoting(int );
//
//
void GetParam(int argc, char **argv) {
    int opt, seed, i, j; extern char *optarg; extern int optopt;
    srand(88); // default seed
    // get command line input
    while((opt=getopt(argc,argv,"hf:N:M:S:V")) != -1) {
        switch(opt) {
            case 'h': // leave help information here
                break;
            case 'V':
                vFlag = 1; //verification: X=1,2,...
                break;
            case 'f': // needed to run testfile
                printf("Take input file: %s\n", *optarg);
                break;
            case 'N':
                N=atoi(optarg);
                if (N < 1 || N > MAXN) {
                    printf("N=%d is out of range.\n",N); exit(0);}
                printf("\nMatrix size: N=%d; ", N);
                break;
            case 'M':
                M = atoi(optarg);
                if (M < 1) {
                    printf("Invalid # of threads=%d. M=1.\n",M); M=1;}
                else if (M>_POSIX_THREAD_THREADS_MAX) {
                    printf("%d threads requested; only %d available.\n", 
                            M, _POSIX_THREAD_THREADS_MAX);
                    M = _POSIX_THREAD_THREADS_MAX; }
                printf("# of Threads: M=%d \n", M);
                break;
            case 'S':
                seed = atoi(optarg); srand(seed);
                break;
            case ':':
                fprintf(stderr, "Err:option`%c' needs a value\n", 
                        optopt);
                break;
            default: 
                fprintf(stderr, "Err: no such option:`%c'\n", 
                        optopt);
        }
    }


    NumThreads = M;
    printf("NumThreads = %d\n",NumThreads);
    // initilize matrix
    printf("\nInitializing ...\n");
    matA = (double*) malloc(sizeof(double)*N*(N)); 
    rowA = (double**) malloc(sizeof(double*)*(N));
    X = (double*) malloc(sizeof(double)*N); 
    B = (double*) malloc(sizeof(double)*N); 
    idx = (int*) malloc(sizeof(int)*N); 
    colZ = (double*) malloc(sizeof(double)*N); 

////colA
//    for (j=0; j<N; j++) { // generate coeff matrix A
//        colA[j] = matA + j*N;
//        for (i=0; i<N; i++)
//            colA[j][i]=(double)rand()/RAND_MAX+1;
//        X[j]= (vFlag) ? (j+1.0) : 0.0;
//        idx[j]=j;
//    }

//rowA
    for (i=0; i<N; i++) { // generate coeff matrjx A
        rowA[i] = matA + i*N;
        for (j=0; j<N; j++)
        {
            rowA[i][j]=(double)rand()/RAND_MAX+1;
            //printf("row[%d][%d] = %f\n",i,j,row[i][j]);
        }
        X[i]= (vFlag) ? (i+1.0) : 0.0;
        idx[i]=i;
    }

//colA
//    if (vFlag) {
//        for (i=0; i<N; i++) {
//            B[i]=(double)0.0;
//            for (j=0; j<N; j++)
//                B[i]+=colA[j][i]*X[j];
//        }
//    }
//    else {
//        for (i=0; i<N; i++) {
//            B[i]=(double)rand()/RAND_MAX+1;
//        }
//    }
//    
//    printAB();
//    printf("rowA is following:\n");
//rowA
    if (vFlag) {
        for (i=0; i<N; i++) {
            B[i]=(double)0.0;
            for (j=0; j<N; j++)
                B[i]+=rowA[i][j]*X[j];
        }
    }

    else {
        for (i=0; i<N; i++) {
            B[i]=(double)rand()/RAND_MAX+1;
        }
    }
    //printAB();
}//end of GetPara()


void printAB(void){
// print matrix if not too large
    int i,j;
    if (N<MAXN4print) {
        printf("\nPrinting ... (if matrix is not too large)\n");
        for (i=0; i<N; i++) {
            printf("A[%d]=",i);
            for (j=0; j<N; j++)
                printf("%6.3f%s",rowA[i][j],(j%5!=4)?",":",\n   ");
            printf("\n");
        }
        printf("B = ");
        for (j=0; j<N; j++)
            printf("%6.3f%s",B[j],(j%5!=4)?"; ":";\n   ");
        printf("\n");
    }
}

void PrintX(void) {
    int i;
    printf("\nX = ");
    for (i=0; i<N; i++)
        printf("%6.3f %s",X[idx[i]],(i%5!=4)?"; ":";\n    ");
    printf("\n");
}

// MAIN routine
int main(int argc, char **argv) {
    clock_t clkStart,clkStop; //Elapsed times using <times()>
    struct tms tStart,tStop;  //CPU times for the threads
    int k, i, j, temp;
    double max;

    vFlag = 0;
    GetParam(argc, argv);




    int row ;int col;
    pivoting(0);
    CurrentRow = Norm +1;
    Count = NumThreads -1;

    printf("Starting timing ... computing ...\n");
    clkStart = times(&tStart);

    create_threads();
    wait_for_threads();



    //printf("before back substitution:\n");
    //printAB();
    //PrintX();
    // Back substitution in sequential                         
    for (i=N-1; i>=0; i--) {
        X[idx[i]] = B[idx[i]];
        for (j=N-1; j>i; j--)
            //X[idx[i]]-=colA[j][idx[i]]*X[idx[j]];
            X[idx[i]]-=rowA[idx[i]][j]*X[idx[j]];
        X[idx[i]] /= rowA[idx[i]][i];
    }

    clkStop = times(&tStop);
    printf("Stopped timing.\n");

    if (N<MAXN4print) PrintX();
    if (vFlag) for (i=0;i<N;i++) if (fabs(X[idx[i]]-(double)(i+1))>0.01) 
        printf("Incorrect results, i=%d, X[i]=%f\n",i,X[idx[i]]);

    printf("Elapsed time = %g ms.\n", 
            (float)(clkStop-clkStart)
            /(float)sysconf(_SC_CLK_TCK)*1000);

    printf("The total CPU time comsumed = %g ms.\n", 
            (float)((tStop.tms_utime-tStart.tms_utime) 
                + (tStop.tms_stime-tStart.tms_stime))
            / (float)sysconf(_SC_CLK_TCK)*1000);
    return 0;
}

void pivoting(int k)
{

 //   printf("\n pivoting %d\n",k);
    int i,j, temp;
    double max;
    //print out the index before
//    for( i = k; i < N; i++)
//    {
//        printf("before pivoting: idx[%d] = %d\n",i,idx[i]);
//    }
    //max = fabs(colA[k][idx[k]]);
    max = fabs(rowA[idx[k]][k]);
//    printf("max is %f\n",max);
    j=k; //search for max pivot
    for (i=k+1; i<N; i++)
        if (max<fabs(rowA[idx[i]][k]))
        {
            max=fabs(rowA[idx[i]][k]);
            j=i;
            //printf("max is %f, j is :%d\n",max,j);
        } 
    if (j!=k)
    {
        temp=idx[k];idx[k]=idx[j];
        idx[j]=temp;
    }
    for (i=k+1; i<N; i++) //compute colZ as multipliers
    {
        colZ[idx[i]] = rowA[idx[i]][k]/rowA[idx[k]][k];
//        printf("cloz[%d] =%f\n",idx[i],colZ[idx[i]]);
    }
    //print out the index afterwards
//    for( i = k; i < N; i++)
//    {
//        printf("idx[%d] = %d\n",i,idx[i]);
//    }
//    PrintX();
}

//void *gaussPT(void *arg) {
//    int row, col, *pK=(int*)arg, k=*pK;
//    double multiplier;
//    // Actual Gaussian elimination begins here. 
//    for (row=k+1; row<N; row++) {
//        //multiplier = colA[k][row]/colA[k][k];
//        for (col=k+1; col<N; col++)
//            colA[col][idx[row]] -= colA[col][idx[k]]*colZ[idx[row]];
//        B[idx[row]] -= B[idx[k]]*colZ[idx[row]];
//    }
//}

// copy from Gauss_seq.c

void *gaussPT(void *dummy)
{
    /*  <myRow> denotes the first row of the chunk assigned to a thread.  */
    int myRow = 0, row, col;

    /*                     Normalisation row.                             */
    int myNorm = 0;

    float multiplier;
    int chunkSize;

    /*                Actual Gaussian elimination begins here.            */
    while (myNorm < N-1)
    {
        /*        Ascertain the row chunk to be assigned to this thread.      */
        while (chunkSize = find_next_row_chunk(&myRow, myNorm))
        {  
            /*      We perform the eliminations across these rows concurrently.   */
            for (row = myRow; row < (min(N, myRow+chunkSize)); row++)
            {
                multiplier = rowA[row][myNorm]/rowA[myNorm][myNorm];
                for (col = myNorm; col < N; col++)
                    rowA[idx[row]][col] -= rowA[idx[myNorm]][col]*colZ[idx[row]];
                B[idx[row]] -= B[idx[myNorm]]*colZ[idx[row]];
//                colA[row][col] -= colA[myNorm][col]*multiplier;
//                B[row] -= B[myNorm]*multiplier;
            }
        }

        /*           We wait until all threads are done with this stage.      */
        /*          We then proceed to handle the next normalisation row.     */
        barrier(&myNorm);
    }
}


void barrier(int *myNorm)
{

    /*         We implement synchronisation using condition variables.    */
    pthread_mutex_lock(&CountLock);

    if (Count == 0)
    {
        /*  Only the last thread for each value of <Norm> reaches this point. */
        printAB();
        Norm++;
        Count = NumThreads-1;
        if(Norm < N)
            pivoting(Norm);
        CurrentRow = Norm+1;
        pthread_cond_broadcast(&NextIter);
    }
    else
    {
        Count--;
        pthread_cond_wait(&NextIter, &CountLock);
    }

    /*    <*myNorm> is each thread's view of the global variable <Norm>.  */
    *myNorm = Norm;

    pthread_mutex_unlock(&CountLock);
}


int find_next_row_chunk(int *myRow, int myNorm)
{
    int chunkSize;


    pthread_mutex_lock(&Mutex);

    *myRow = CurrentRow;

    /*    For guided-self scheduling, we determine the chunk size here.   */
    chunkSize = (*myRow < N) ? (N-myNorm-1)/(2*NumThreads)+1 : 0;
    CurrentRow += chunkSize;

    pthread_mutex_unlock(&Mutex);

    return chunkSize;
}


void create_threads(void)
{
    int i;


    for (i = 0; i < NumThreads; i++)
        pthread_create(&idThreads[i], NULL, gaussPT, NULL);
}



void wait_for_threads(void)
{
    int i;

    for (i = 0; i < NumThreads; i++)
        pthread_join(idThreads[i], NULL);
}
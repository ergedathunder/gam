#include <omp.h>
#include <cstdio>
#include <cmath>
#include <thread>
#include <math.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <iostream>
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
#include <cstdlib>
#include <cstdio>
#include <cstring>
#include <sys/time.h>

using namespace std;

/*
 * Types
 */
struct Particle
{
    double position_x; /* m   */
    double position_y; /* m   */
    double position_z; /* m   */
    double velocity_x; /* m/s */
    double velocity_y; /* m/s */
    double velocity_z; /* m/s */
    double total_force_x;
    double total_force_y;
    double total_force_z;
    double mass; /* kg  */
};

ibv_device **curlist;
Worker *worker[10];
Master *master;
int num_worker = 0;
WorkerHandle *malloc_wh;
WorkerHandle *wh[10];

int iteration_times = 1;

int no_array = 0;
int no_run = 0;
int parrallel_num = 0;
int number_of_timesteps = 5;

Particle *first_particles_array;

const double GRAVITATIONAL_CONSTANT = 6.6726e-11; /* N(m/kg)2 */

const double DEFAULT_DOMAIN_SIZE_X = 1.0e+3; /* m  */
const double DEFAULT_DOMAIN_SIZE_Y = 1.0e+3; /* m  */
const double DEFAULT_DOMAIN_SIZE_Z = 1.0e+3; /* m  */
const double DEFAULT_MASS_MAXIMUM = 1.0e+15; /* kg */
const double DEFAULT_TIME_INTERVAL = 1.0e+1; /* s  */

double domain_size_x;
double domain_size_y;
double domain_size_z;
double mass_maximum;
double time_interval;

const int DEFAULT_NUMBER_OF_PARTICLES = 1000;
const int DEFAULT_NUMBER_OF_TIMESTEPS = 100;
const int DEFAULT_TIMESTEPS_BETWEEN_OUTPUTS = 1000;
const bool DEFAULT_EXECUTE_SERIAL = false;
const int DEFAULT_RANDOM_SEED = 12345;
const int DEFAULT_STRING_LENGTH = 1023;
const int PROGRAM_SUCCESS_CODE = 0;

char base_filename[DEFAULT_STRING_LENGTH + 1];
int block_size;
int timesteps_between_outputs;
bool execute_serial;
unsigned random_seed;

// #define CHECK
// #define CHECK_VAL

/*
 * Compute variables
 */
GAddr particle_array_msiInput;
GAddr particle_array_rcInput;
GAddr particle_array_msiOutput;
GAddr particle_array_rcOutput;

/*
 * Function Prototypes
 */
void Particle_input_arguments(FILE *input);

// Particle
void Particle_clear(WorkerHandle *Cur_wh, GAddr this_particle, int index);
void Particle_construct(WorkerHandle *Cur_wh, GAddr this_particle, int index);
void Particle_destruct(WorkerHandle *Cur_wh, GAddr this_particle, int index);

// Particle array
GAddr Particle_array_allocate(WorkerHandle *Cur_wh, int no_array);
GAddr Particle_array_construct(WorkerHandle *Cur_wh, int no_array);
GAddr Particle_array_deallocate(WorkerHandle *Cur_wh, GAddr this_particle_array, int no_array);
GAddr Particle_array_destruct(WorkerHandle *Cur_wh, GAddr this_particle_array, int no_array);

void Particle_set_position_randomly(WorkerHandle *Cur_wh, GAddr this_particle, int index);
void Particle_initialize_randomly(WorkerHandle *Cur_wh, GAddr this_particle, int index);
void Particle_array_initialize_randomly(WorkerHandle *Cur_wh, GAddr this_particle_array, int no_array);
void Particle_array_initialize(WorkerHandle *Cur_wh, GAddr this_particle_array, int no_array);

void Particle_array_output(WorkerHandle *Cur_wh, GAddr this_particle_array, int no_array);
void Particle_array_output_xyz(WorkerHandle *Cur_wh, GAddr this_particle_array, int no_array);
void Particle_output(WorkerHandle *Cur_wh, GAddr this_particle, int index);
void Particle_output_xyz(WorkerHandle *Cur_wh, GAddr this_particle, int index);

// Check
#ifdef CHECK
void Particle_check(GAddr this_particle, char *action, char *routine);
void Particle_array_check(GAddr this_particle_array, int no_array,
                          char *action, char *routine);
#endif

void Create_master()
{
    Conf *conf = new Conf();
    // conf->loglevel = LOG_DEBUG;
    conf->loglevel = LOG_TEST;
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
    wr.wid = Cur_wh->GetWorkerId();
    wr.flag = 0;
    wr.size = size;
    wr.addr = addr;
    wr.ptr = (void *)val;
    if (Cur_wh->SendRequest(&wr))
    {
        epicLog(LOG_WARNING, "send request failed");
    }
}

void Write_val(WorkerHandle *Cur_wh, GAddr addr, void *val, int size, int flush_id)
{
    WorkRequest wr{};
    if (Cur_wh->GetWorkerId() == 0)
    {
        flush_id = -1;
    }

    wr.Reset();
    wr.op = WRITE;
    wr.wid = Cur_wh->GetWorkerId();
    // wr.flag = ASYNC; // 可以在这里调
    wr.flush_id = flush_id;
    wr.size = size;
    wr.addr = addr;
    wr.ptr = (void *)val;
    if (Cur_wh->SendRequest(&wr))
    {
        epicLog(LOG_WARNING, "send request failed");
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

void Free_addr(WorkerHandle *Cur_wh, GAddr addr)
{
    WorkRequest wr = {};
    wr.Reset();
    wr.addr = addr;
    wr.op = FREE;
    if (Cur_wh->SendRequest(&wr))
    {
        epicLog(LOG_WARNING, "send request failed");
    }
}

void calculate_force_addr(WorkerHandle *Cur_wh, GAddr this_particle1, GAddr this_particle2, GAddr this_particle3,
                          double *force_x, double *force_y, double *force_z)
{
    /* Particle calculate force */
    double difference_x, difference_y, difference_z;
    double distance_squared, distance;
    double force_magnitude;

    Particle p1, p2, p3;
    Read_val(Cur_wh, this_particle1, &p1, sizeof(Particle));
    Read_val(Cur_wh, this_particle2, &p2, sizeof(Particle));

    difference_x = p2.position_x - p1.position_x;
    difference_y = p2.position_y - p1.position_y;
    difference_z = p2.position_z - p1.position_z;

    distance_squared = difference_x * difference_x +
                       difference_y * difference_y +
                       difference_z * difference_z;

    distance = std::sqrt(distance_squared); // sqrtf(distance_squared);

    force_magnitude = GRAVITATIONAL_CONSTANT * (p1.mass) * (p2.mass) / distance_squared;

    *force_x = (force_magnitude / distance) * difference_x;
    *force_y = (force_magnitude / distance) * difference_y;
    *force_z = (force_magnitude / distance) * difference_z;

    Write_val(Cur_wh, this_particle3, &p3, sizeof(Particle), 1);
}
void calculate_force(WorkerHandle *Cur_wh, int this_particle1_id, int this_particle2_id,
                     double *force_x, double *force_y, double *force_z)
{
    /* Particle calculate force */
    double difference_x, difference_y, difference_z;
    double distance_squared, distance;
    double force_magnitude;

    Particle p1 = first_particles_array[this_particle1_id];
    Particle p2 = first_particles_array[this_particle2_id];

    difference_x = p2.position_x - p1.position_x;
    difference_y = p2.position_y - p1.position_y;
    difference_z = p2.position_z - p1.position_z;

    distance_squared = difference_x * difference_x +
                       difference_y * difference_y +
                       difference_z * difference_z;

    distance = std::sqrt(distance_squared); // sqrtf(distance_squared);

    force_magnitude = GRAVITATIONAL_CONSTANT * (p1.mass) * (p2.mass) / distance_squared;

    *force_x = (force_magnitude / distance) * difference_x;
    *force_y = (force_magnitude / distance) * difference_y;
    *force_z = (force_magnitude / distance) * difference_z;
}

void calculate_force2(WorkerHandle *Cur_wh, int this_particle1_id, int this_particle2_id, GAddr second_particles)
{
    /* Particle calculate force */
    double difference_x, difference_y, difference_z;
    double distance_squared, distance;
    double force_magnitude;

    Particle p1 = first_particles_array[this_particle1_id];
    Particle p2 = first_particles_array[this_particle2_id];

    difference_x = p2.position_x - p1.position_x;
    difference_y = p2.position_y - p1.position_y;
    difference_z = p2.position_z - p1.position_z;

    distance_squared = difference_x * difference_x +
                       difference_y * difference_y +
                       difference_z * difference_z;

    distance = std::sqrt(distance_squared); // sqrtf(distance_squared);

    force_magnitude = GRAVITATIONAL_CONSTANT * (p1.mass) * (p2.mass) / distance_squared;

    double force_x = (force_magnitude / distance) * difference_x;
    double force_y = (force_magnitude / distance) * difference_y;
    double force_z = (force_magnitude / distance) * difference_z;
    Particle p3;
    Read_val(Cur_wh, second_particles + this_particle1_id * sizeof(Particle), &p3, sizeof(Particle));
    p3.total_force_x += force_x;
    p3.total_force_y += force_y;
    p3.total_force_z += force_z;
    Write_val(Cur_wh, second_particles + this_particle1_id * sizeof(Particle), &p3, sizeof(Particle), 1);
}

void sub_nbody(WorkerHandle *Cur_wh, GAddr first_particles, GAddr second_particles, int index)
{
    GAddr first_particles_index = first_particles + index * sizeof(Particle);
    GAddr second_particles_index = second_particles + index * sizeof(Particle);

    int i;

    for (i = 0; i < no_array; i++)
    {
        if (i != index)
        {
            // calculate_force(Cur_wh, index, i, &force_x, &force_y, &force_z);
            calculate_force2(Cur_wh, index, i, second_particles);
        }
    }

    double velocity_change_x, velocity_change_y, velocity_change_z;
    double position_change_x, position_change_y, position_change_z;

    Particle first_particles_buf = first_particles_array[index];
    Particle second_particles_buf;
    Read_val(Cur_wh, second_particles_index, &second_particles_buf, sizeof(Particle));

    second_particles_buf.mass = first_particles_buf.mass;

    // velocity_change_x = total_force_x * (time_interval / first_particles_buf.mass);
    // velocity_change_y = total_force_y * (time_interval / first_particles_buf.mass);
    // velocity_change_z = total_force_z * (time_interval / first_particles_buf.mass);

    velocity_change_x = second_particles_buf.total_force_x * (time_interval / first_particles_buf.mass);
    velocity_change_y = second_particles_buf.total_force_y * (time_interval / first_particles_buf.mass);
    velocity_change_z = second_particles_buf.total_force_z * (time_interval / first_particles_buf.mass);

    position_change_x = first_particles_buf.velocity_x + velocity_change_x * (0.5 * time_interval);
    position_change_y = first_particles_buf.velocity_y + velocity_change_y * (0.5 * time_interval);
    position_change_z = first_particles_buf.velocity_z + velocity_change_z * (0.5 * time_interval);

    second_particles_buf.velocity_x = first_particles_buf.velocity_x + velocity_change_x;
    second_particles_buf.velocity_y = first_particles_buf.velocity_y + velocity_change_y;
    second_particles_buf.velocity_z = first_particles_buf.velocity_z + velocity_change_z;

    second_particles_buf.position_x = first_particles_buf.position_x + position_change_x;
    second_particles_buf.position_y = first_particles_buf.position_y + position_change_y;
    second_particles_buf.position_z = first_particles_buf.position_z + position_change_z;

    Write_val(Cur_wh, second_particles_index, &second_particles_buf, sizeof(Particle), 1);
}

void nbody_index(GAddr first_particles, GAddr second_particles, int start_index)
{
    int interval = parrallel_num;
    for (int index = start_index; index < no_array; index += interval)
    {
        sub_nbody(wh[start_index], first_particles, second_particles, index);
    }
}
void nbody(GAddr first_particles, GAddr second_particles)
{
    Read_val(wh[0], first_particles, first_particles_array, sizeof(Particle) * no_array);

    std::thread threads[parrallel_num];
    for (int i = 0; i < parrallel_num; i++)
    {
        threads[i] = std::thread(nbody_index, first_particles, second_particles, i);
    }

    for (int i = 0; i < parrallel_num; i++)
    {
        threads[i].join();
    }
}

void nbody_serial(GAddr first_particles, GAddr second_particles)
{
    for (int id = 0; id < no_array; id++)
    {
        sub_nbody(wh[0], first_particles, second_particles, id);
    }
}

void nbody_rc(GAddr first_particles, GAddr second_particles)
{

    for (int i = 0; i < parrallel_num; i++)
    {
        // wh[i]->acquireLock(2, first_particles, sizeof(Particle) * no_array, false, sizeof(Particle));
        wh[i]->acquireLock(1, second_particles, sizeof(Particle) * no_array, true, sizeof(Particle));
    }

    Read_val(wh[0], first_particles, first_particles_array, sizeof(Particle) * no_array);

    std::thread threads[parrallel_num];

    // for (int id = 0; id < no_array; id++)
    // {
    //     int thread_id = id % parrallel_num;

    //     threads[thread_id] = std::thread(sub_nbody, wh[thread_id], first_particles, second_particles, id);
    //     threads[thread_id].join();
    // }

    for (int i = 0; i < parrallel_num; i++)
    {
        threads[i] = std::thread(nbody_index, first_particles, second_particles, i);
    }
    for (int i = 0; i < parrallel_num; i++)
    {
        threads[i].join();
    }

#ifdef RC_VERSION2
#else
    for (int i = 0; i < parrallel_num; i++)
    {
        wh[i]->releaseLock(1, second_particles);
    }
#endif
}

void particle_print(WorkerHandle *Cur_wh, GAddr particle, int no_array)
{
    printf("\n\nPrinting %d particles:\n", no_array);
    for (int i = 0; i < no_array; i++)
    {
        Particle particle_buf;
        Read_val(Cur_wh, particle + i * sizeof(Particle), &particle_buf, sizeof(Particle));
        printf("Particle %d:\n", i);
        printf("Mass: %lf\n", particle_buf.mass);
        printf("Position: (%lf, %lf, %lf)\n", particle_buf.position_x, particle_buf.position_y, particle_buf.position_z);
        printf("Velocity: (%lf, %lf, %lf)\n", particle_buf.velocity_x, particle_buf.velocity_y, particle_buf.velocity_z);
    }
}

void Solve_MSI()
{
    printf("Begin N-body simulation MSI\n");

    long start = get_time();

    for (int timestep = 1; timestep <= number_of_timesteps; timestep++)
    {
        nbody(particle_array_msiInput, particle_array_msiOutput);

        particle_print(wh[0], particle_array_msiOutput, 3);
        printf("Iteration %d complete\n", timestep);

        if (timestep == number_of_timesteps)
        {
            break;
        }

        Particle tmp2[no_array];
        Read_val(wh[0], particle_array_msiOutput, tmp2, sizeof(Particle) * no_array);
        Write_val(wh[0], particle_array_msiInput, tmp2, sizeof(Particle) * no_array, 1);
    }

    for (int i = 0; i < parrallel_num; i++)
    {
        wh[i]->ReportCacheStatistics();
    }

    long end = get_time();
    long time = (end - start);

    printf("NBody simulation complete\n");
    printf("Number of Particles: %d\n", no_array);
    printf("Number of Iterations: %d\n", number_of_timesteps);
    printf("MSI Run time: %ld ns\n", time);
}

void Solve_RC()
{
    printf("Begin N-body simulation RC\n");

    long start = get_time();

    for (int timestep = 1; timestep <= number_of_timesteps; timestep++)
    {
        nbody_rc(particle_array_rcInput, particle_array_rcOutput);

        particle_print(wh[0], particle_array_rcOutput, 3);
        printf("Iteration %d complete\n", timestep);

        if (timestep == number_of_timesteps)
        {
            break;
        }
        Particle tmp2[no_array];
        Read_val(wh[0], particle_array_rcOutput, tmp2, sizeof(Particle) * no_array);
        Write_val(wh[0], particle_array_rcInput, tmp2, sizeof(Particle) * no_array, 1);
    }

    long end = get_time();
    long time = (end - start);

    printf("NBody simulation complete\n");
    printf("Number of Particles: %d\n", no_array);
    printf("Number of Iterations: %d\n", number_of_timesteps);
    printf("RC Run time: %ld ns\n", time);
}

void Solve_TEST()
{
    GAddr particle_input = Malloc_addr(wh[0], sizeof(Particle) * no_array, Msi, 1);

    GAddr particle_output = Malloc_addr(wh[0], sizeof(Particle) * no_array, RC_Write_shared, 1);

    Particle tmp1[no_array];
    for (int i = 0; i < no_array; i++)
    {
        tmp1[i].mass = i;
        tmp1[i].position_x = i;
        tmp1[i].position_y = i;
        tmp1[i].position_z = i;
        tmp1[i].velocity_x = i;
        tmp1[i].velocity_y = i;
        tmp1[i].velocity_z = i;
    }
    Write_val(wh[0], particle_input, tmp1, sizeof(Particle) * no_array, 1);
    Particle tmp1_READ[no_array];
    Read_val(wh[0], particle_input, tmp1_READ, sizeof(Particle) * no_array);

    Write_val(wh[0], particle_output, tmp1_READ, sizeof(Particle) * no_array, 1);
    Particle tmp2[no_array];
    Read_val(wh[0], particle_output, tmp2, sizeof(Particle) * no_array);
    printf("\n\n");
    for (int i = 0; i < no_array; i++)
    {
        printf("Particle %d:\n", i);
        printf("Mass: %lf\n", tmp2[i].mass);
        printf("Position: (%lf, %lf, %lf)\n", tmp2[i].position_x, tmp2[i].position_y, tmp2[i].position_z);
        printf("Velocity: (%lf, %lf, %lf)\n", tmp2[i].velocity_x, tmp2[i].velocity_y, tmp2[i].velocity_z);
    }
}

/*
 * Get command line arguments.
 */
void Particle_input_arguments(FILE *input)
{

    if (fscanf(input, "%d", &no_array) != 1)
    {
        fprintf(stderr, "ERROR: cannot read number of particles from standard input!\n");
        std::abort();
    }

    if (no_array < 1)
    {
        fprintf(stderr, "ERROR: cannot have %d particles!\n", no_array);
        std::abort();
    }

    if (no_array == 1)
    {
        fprintf(stderr, "There is only one particle, therefore no forces.\n");
        std::abort();
    }
    //
    if (fscanf(input, "%d", &block_size) != 1)
    {
        fprintf(stderr, "ERROR: cannot read block size from standard input!\n");
        std::abort();
    }

    if (block_size <= 0)
    {
        fprintf(stderr, "ERROR: cannot have %d as block size!\n", block_size);
        std::abort();
    }

    if (no_array % block_size != 0)
    {
        fprintf(stderr, "ERROR: block size must be divisable by number of particles!\n");
        std::abort();
    }

    if (fscanf(input, "%lf", &domain_size_x) != 1)
    {
        fprintf(stderr, "ERROR: cannot read domain size X from standard input!\n");
        std::abort();
    }

    if (domain_size_x <= 0.0)
    {
        fprintf(stderr, "ERROR: cannot have a domain whose X dimension has length %lf!\n", domain_size_x);
        std::abort();
    }

    if (fscanf(input, "%lf", &domain_size_y) != 1)
    {
        fprintf(stderr, "ERROR: cannot read domain size Y from standard input!\n");
        std::abort();
    }

    if (domain_size_y <= 0.0)
    {
        fprintf(stderr, "ERROR: cannot have a domain whose Y dimension has length %lf!\n", domain_size_y);
        std::abort();
    }

    if (fscanf(input, "%lf", &domain_size_z) != 1)
    {
        fprintf(stderr, "ERROR: cannot read domain size Z from standard input!\n");
        std::abort();
    }

    if (domain_size_z <= 0.0)
    {
        fprintf(stderr, "ERROR: cannot have a domain whose Z dimension has length %lf!\n", domain_size_z);
        std::abort();
    }

    if (fscanf(input, "%lf", &time_interval) != 1)
    {
        fprintf(stderr, "ERROR: cannot read time interval from standard input!\n");
        std::abort();
    }

    if (time_interval <= 0.0)
    {
        fprintf(stderr, "ERROR: cannot have a time interval of %lf!\n", time_interval);
        std::abort();
    }

    if (fscanf(input, "%d", &number_of_timesteps) != 1)
    {
        fprintf(stderr, "ERROR: cannot read number of timesteps from standard input!\n");
        std::abort();
    }

    if (number_of_timesteps <= 0)
    {
        fprintf(stderr, "ERROR: cannot have %d timesteps!\n", number_of_timesteps);
        std::abort();
    }

    if (fscanf(input, "%d", &timesteps_between_outputs) != 1)
    {
        fprintf(stderr, "ERROR: cannot read timesteps between outputs from standard input!\n");
        std::abort();
    }

    if (timesteps_between_outputs <= 0)
    {
        fprintf(stderr, "ERROR: cannot have %d timesteps between outputs!\n", timesteps_between_outputs);
        std::abort();
    }

    int aux_serial;
    if (fscanf(input, "%d", &aux_serial) != 1)
    {
        fprintf(stderr, "ERROR: cannot read serial from standard input!\n");
        std::abort();
    }

    if (aux_serial != 0 && aux_serial != 1)
    {
        fprintf(stderr, "ERROR: serial must be 0 (false) or 1 (true)!\n");
        std::abort();
    }

    if (fscanf(input, "%d", &random_seed) != 1)
    {
        fprintf(stderr, "ERROR: cannot read random seed from standard input!\n");
        std::abort();
    }

    if (fscanf(input, "%lf", &mass_maximum) != 1)
    {
        fprintf(stderr, "ERROR: cannot read mass maximum from standard input!\n");
        std::abort();
    }

    if (mass_maximum <= 0.0)
    {
        fprintf(stderr, "ERROR: cannot have a maximum mass of %lf!\n", mass_maximum);
        std::abort();
    }

    fgetc(input);
    fgets(base_filename, DEFAULT_STRING_LENGTH, input);
    if (base_filename[strlen(base_filename) - 1] == '\n')
    {
        base_filename[strlen(base_filename) - 1] = '\0';
    }
}

/*
 * Clear the particle's data.
 */
void Particle_clear(WorkerHandle *Cur_wh, GAddr this_particle, int index)
{
#ifdef CHECK
    Particle_check(this_particle, "clear", "Particle clear");
#endif
    Particle particle_init;

    particle_init.position_x = 0.0;
    particle_init.position_y = 0.0;
    particle_init.position_z = 0.0;
    particle_init.velocity_x = 0.0;
    particle_init.velocity_y = 0.0;
    particle_init.velocity_z = 0.0;
    particle_init.mass = 0.0;
    Write_val(Cur_wh, this_particle + index * sizeof(Particle), &particle_init, sizeof(Particle), 1);
}

/*
 * Construct the particle.
 */
void Particle_construct(WorkerHandle *Cur_wh, GAddr this_particle, int index)
{
#ifdef CHECK
    Particle_check(this_particle, "construct", "Particle construct");
#endif

    Particle_clear(Cur_wh, this_particle, index);
}

/*
 * Destroy the particle.
 */
void Particle_destruct(WorkerHandle *Cur_wh, GAddr this_particle, int index)
{
#ifdef CHECK
    Particle_check(this_particle, "destruct", "Particle_destruct");
#endif

    Particle_clear(Cur_wh, this_particle, index);
}

/*
 * Initialize the particle by setting its data randomly.
 */
void Particle_set_position_randomly(WorkerHandle *Cur_wh, GAddr this_particle, int index)
{
#ifdef CHECK
    Particle_check(this_particle, "randomly set the position", "Particle_set_randomly");
#endif

    Particle particle_init;
    particle_init.position_x = domain_size_x * (static_cast<double>(random()) / (static_cast<double>(RAND_MAX) + 1.0));
    particle_init.position_y = domain_size_y * (static_cast<double>(random()) / (static_cast<double>(RAND_MAX) + 1.0));
    particle_init.position_z = domain_size_z * (static_cast<double>(random()) / (static_cast<double>(RAND_MAX) + 1.0));
    particle_init.velocity_x = 0.0;
    particle_init.velocity_y = 0.0;
    particle_init.velocity_z = 0.0;
    particle_init.mass = mass_maximum * (static_cast<double>(random()) / (static_cast<double>(RAND_MAX) + 1.0));
    Write_val(Cur_wh, this_particle + index * sizeof(Particle), &particle_init, sizeof(Particle), 1);
}

/*
 * Initialize the particle by setting its data randomly.
 */
void Particle_initialize_randomly(WorkerHandle *Cur_wh, GAddr this_particle, int index)
{
#ifdef CHECK
    Particle_check(this_particle, "randomly initialize", "Particle initialize randomly");
#endif

    // Particle_clear(Cur_wh, this_particle, index);
    Particle_set_position_randomly(Cur_wh, this_particle, index);

#ifdef CHECK_VAL
    Particle particle_buf;
    Read_val(Cur_wh, this_particle + index * sizeof(Particle), &particle_buf, sizeof(Particle));
    printf("mass %g\n", particle_buf.mass);
#endif
}

void Particle_output(WorkerHandle *Cur_wh, GAddr this_particle, int index)
{
    Particle particle;
    Read_val(Cur_wh, this_particle + index * sizeof(Particle), &particle, sizeof(Particle));

    printf("%g %g %g %g %g %g %g\n",
           particle.position_x,
           particle.position_y,
           particle.position_z,
           particle.velocity_x,
           particle.velocity_y,
           particle.velocity_z,
           particle.mass);
}

void Particle_output_xyz(WorkerHandle *Cur_wh, GAddr this_particle, int index)
{
    Particle particle;
    Read_val(Cur_wh, this_particle + index * sizeof(Particle), &particle, sizeof(Particle));
    printf("C %g %g %g\n",
           particle.position_x, particle.position_y, particle.position_z);
}

/*
 * Allocate and return an array of particles.
 */
GAddr Particle_array_allocate(WorkerHandle *Cur_wh, int no_array)
{
    GAddr this_particle_array;

#ifdef CHECK
    if (no_array < 0)
    {
        fprintf(stderr, "ERROR: illegal number of particles %d to allocate\n", no_array);
        fprintf(stderr, "  in Particle array construct\n");
        std::abort();
    }
#endif

    if (no_array == 0)
        return -1;

    this_particle_array = Malloc_addr(Cur_wh, no_array * sizeof(Particle), Msi, 1);
    if (this_particle_array == -1)
    {
        fprintf(stderr, "ERROR: can't allocate a particle array of %d particles\n", no_array);
        fprintf(stderr, "  in Particle array construct\n");
        std::abort();
    }

    return this_particle_array;
}

/*
 * Construct and return an array of particles, cleared.
 */
GAddr Particle_array_construct(WorkerHandle *Cur_wh, int no_array)
{
    GAddr this_particle_array;

    this_particle_array = Particle_array_allocate(Cur_wh, no_array);

    for (int index = 0; index < no_array; index++)
    {
        Particle_construct(Cur_wh, this_particle_array, index);
    }
    return this_particle_array;
}

/*
 * Deallocate the array of particles, and return NULL.
 */
GAddr Particle_array_deallocate(WorkerHandle *Cur_wh, GAddr this_particle_array, int no_array)
{
#ifdef CHECK
    Particle_array_check(this_particle_array, no_array, "deallocate", "Particle_array_deallocate");
#endif

    Free_addr(Cur_wh, this_particle_array);

    return 0;
}

/*
 * Destroy the array of particles, and return NULL.
 */
GAddr Particle_array_destruct(WorkerHandle *Cur_wh, GAddr this_particle_array, int no_array)
{
#ifdef CHECK
    Particle_array_check(this_particle_array, no_array, "destroy", "Particle array destruct");
#endif

    for (int index = no_array - 1; index >= 0; index--)
    {
        Particle_destruct(Cur_wh, this_particle_array, index);
    }

    return Particle_array_deallocate(Cur_wh, this_particle_array, no_array);
}

/*
 * Initialize the array of particles by setting its data randomly.
 */
void Particle_array_initialize_randomly(WorkerHandle *Cur_wh, GAddr this_particle_array, int no_array)
{
#ifdef CHECK
    Particle_array_check(this_particle_array, no_array,
                         "initialize randomly", "Particle_array_initialize_randomly");
#endif

    for (int index = 0; index < no_array; index++)
    {
        Particle_initialize_randomly(Cur_wh, this_particle_array, index);
    }
}

void Particle_array_initialize_not_random(WorkerHandle *Cur_wh, GAddr this_particle_array, int no_array)
{
    for (int index = 0; index < no_array; index++)
    {
    }
}

/*
 * Initialize the array of particles.
 */
void Particle_array_initialize(WorkerHandle *Cur_wh, GAddr this_particle_array, int no_array)
{
    Particle_array_initialize_randomly(Cur_wh, this_particle_array, no_array);
    // Particle_array_initialize_not_random(Cur_wh, this_particle_array, no_array);
}

/*
 * Particle_array_output
 */
void Particle_array_output(WorkerHandle *Cur_wh, GAddr this_particle_array, int no_array)
{
    printf("%d\nNBody\n", no_array);

    for (int index = 0; index < no_array; index++)
    {
        Particle_output(Cur_wh, this_particle_array, index);
    }
} /* Particle_array_output */

/* Outputs particle positions in a format that VMD can easily visualize. */
void Particle_array_output_xyz(WorkerHandle *Cur_wh, GAddr this_particle_array, int no_array)
{
    printf("%d\nNBody\n", no_array);

    for (int index = 0; index < no_array; index++)
    {
        Particle_output_xyz(Cur_wh, this_particle_array, index);
    }
}

#ifdef CHECK
/*
 * Check that the particle exists.
 */
void Particle_check(GAddr this_particle, char *action, char *routine)
{
    if (this_particle != 0)
        return;

    printf("ERROR: can't %s a nonexistent particle\n",
           ((action == (char *)NULL) || (strlen(action) == 0)) ? "perform an unknown action on" : action);
    printf("  in %s\n",
           ((routine == (char *)NULL) || (strlen(routine) == 0)) ? "an unknown routine" : routine);

    std::abort();
}

void Particle_array_check(GAddr this_particle_array, int no_array,
                          char *action, char *routine)
{
    if (no_array < 0)
    {
        printf("ERROR: illegal number of particles %d\n", no_array);
        printf("  to %s\n",
               ((action == (char *)NULL) || (strlen(action) == 0)) ? "perform an unknown action on" : action);
        printf("  in %s\n",
               ((routine == (char *)NULL) || (strlen(routine) == 0)) ? "an unknown routine" : routine);

        std::abort();
    }

    if (no_array == 0)
    {
        printf("ERROR: can't %s\n",
               ((action == (char *)NULL) || (strlen(action) == 0)) ? "perform an unknown action on" : action);
        printf("  an existing particle array of length 0\n");
        printf("  in %s\n",
               ((routine == (char *)NULL) || (strlen(routine) == 0)) ? "an unknown routine" : routine);

        std::abort();
    }

    if (this_particle_array == 0)
    {
        printf("ERROR: can't %s\n",
               ((action == (char *)NULL) || (strlen(action) == 0)) ? "perform an unknown action on" : action);
        printf("  a nonexistent array of %d particles\n", no_array);
        printf("  in %s\n",
               ((routine == (char *)NULL) || (strlen(routine) == 0)) ? "an unknown routine" : routine);

        std::abort();
    }
    return;
}
#endif /* #ifdef CHECK */

/* main */
int main(int argc, char **argv)
{
    domain_size_x = DEFAULT_DOMAIN_SIZE_X;
    domain_size_y = DEFAULT_DOMAIN_SIZE_Y;
    domain_size_z = DEFAULT_DOMAIN_SIZE_Z;
    time_interval = DEFAULT_TIME_INTERVAL;
    mass_maximum = DEFAULT_MASS_MAXIMUM;
    no_array = atoi(argv[1]);
    parrallel_num = atoi(argv[2]);
    no_run = atoi(argv[3]);
    cout << "no_array: " << no_array << " parrallel_num: " << parrallel_num << " no_run: " << no_run << endl;

    first_particles_array = new Particle[no_array];

    srand(time(NULL));
    curlist = ibv_get_device_list(NULL);
    Create_master();
    for (int i = 0; i < parrallel_num; ++i)
    {
        Create_worker();
    }

    if (no_run == 1)
    {
        particle_array_msiInput = Malloc_addr(wh[0], sizeof(Particle) * no_array, Msi, 1);
        Particle_array_initialize(wh[0], particle_array_msiInput, no_array);
        particle_array_msiOutput = Malloc_addr(wh[0], sizeof(Particle) * no_array, Msi, 1);
        Solve_MSI();
        // particle_print(wh[0], particle_array_msiInput, 4);
    }
    else if (no_run == 2)
    {
        particle_array_rcInput = Malloc_addr(wh[0], sizeof(Particle) * no_array, Msi, 1);
        Particle_array_initialize(wh[0], particle_array_rcInput, no_array);

        particle_array_rcOutput = Malloc_addr(wh[0], sizeof(Particle) * no_array, RC_Write_shared, 1);

        Solve_RC();
        // Solve_TEST();
    }
    else if (no_run == 3)
    {
        particle_array_msiInput = Malloc_addr(wh[0], sizeof(Particle) * no_array, Msi, 1);
        particle_array_rcInput = Malloc_addr(wh[0], sizeof(Particle) * no_array, Msi, 1);

        Particle_array_initialize(wh[0], particle_array_msiInput, no_array);

        Particle tmp[no_array];
        Read_val(wh[0], particle_array_msiInput, tmp, sizeof(Particle) * no_array);
        Write_val(wh[0], particle_array_rcInput, tmp, sizeof(Particle) * no_array, 1);

        particle_array_msiOutput = Malloc_addr(wh[0], sizeof(Particle) * no_array, Msi, 1);
        particle_array_rcOutput = Malloc_addr(wh[0], sizeof(Particle) * no_array, RC_Write_shared, 1);

        Solve_MSI();
        Solve_RC();
    }

    return PROGRAM_SUCCESS_CODE;
}
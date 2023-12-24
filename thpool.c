/*
Structure per thread:

next free thread index
function pointer
data pointer
semaphore: job available

Structure per pool:

semaphore: next free thread
lock: next free thread
first free thread index (or -1)
free thread count

Initialisation:

Set up thread storage linked list
set first free thread
set free thread count to the number of threads
initialise next free thread semaphore to the number of threads
initialise job available semaphores to 0 for each thread
start new threads 
in each thread: wait on the job available semaphore

Destruction: (supposed to be not called concurrently with job submission)

Wait for all jobs to complete
Update all threads function pointer to null
post all threads job available semaphore
join all threads

Submit new job:

Loop: Wait on next free thread semaphore (decrement)
Lock next free thread
If no first free thread: report an error
pop from the linked list
decrement free thread count
unlock next free thread
populate the thread structure with the job
post the thread's job available semaphore

Worker thread loop:

Wait on job available semaphore
If function pointer is null then exit thread
call function pointer with data
Lock next free thread
increment free thread count
push to linked list
Unlock next free thread
Post next free thread semaphore
Go back to top (loop)

Wait for jobs:  (supposed to be not called concurrently with job submission)

Loop: Wait on next free thread semaphore (decrement)
Lock next free thread
check free thread count is all threads?
if not then unlock and go back to loop
unlock next free thread

*/

#include <stdlib.h>
#include <pthread.h>
#include <errno.h>
#include <semaphore.h>
#include <string.h>
#include <stdio.h>
#include <signal.h>

#undef DEBUG

#define CHECK_WAIT(x) check_fn(x, __LINE__, "semaphore wait call failed at line %i: %s\n", "%lu.%lu semaphore wait call succeeded at line %i: %s\n")
#define CHECK_POST(x) check_fn(x, __LINE__, "semaphore post call failed at line %i: %s\n", "%lu.%lu semaphore post call succeeded at line %i: %s\n")

#ifdef DEBUG
#include <sys/time.h>
static __thread char logbuffer[2000000];
static __thread int logbuffer_len = 0;

#endif

static int check_fn(int r, int line, char* msg, char* msg_success) {
  if (r != 0 && r != -1) {
    fprintf(stderr, msg, line, strerror(r));
  }
#ifdef DEBUG
  else {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    if (logbuffer_len < sizeof(logbuffer) - 1000) {
      logbuffer_len += sprintf(logbuffer + logbuffer_len, msg_success, tv.tv_sec, tv.tv_usec, line, strerror(r));
    }
  }
#endif
  return r;
}

struct thread {
  pthread_t thread;
  int next_free;
  void (*work)(void* data);
  void *data;
  sem_t job_available;
  struct thpool_* pool;
};

struct thpool_ {
  int num_threads;
  sem_t next_free_thread;
  sem_t thread_list_lock;
  sem_t hold; /* for pausing threads */
  int first_free_thread;
  int free_thread_count;
  struct thread threads[0];
};

static __thread struct thread* my_thread;

static void thread_hold(int sig) {
  if (my_thread != NULL) {
    printf("Thread %i put on hold\n", my_thread - my_thread->pool->threads);
    while(CHECK_WAIT(sem_wait(&my_thread->pool->hold)));
  }
}

static void* thread_function(void* arg) {
  struct thread* thread = (struct thread*)arg;
  int me = thread - thread->pool->threads;
  my_thread = thread;
  /* Register signal handler for pause */
  struct sigaction act;
  sigemptyset(&act.sa_mask);
  act.sa_flags = SA_ONSTACK;
  act.sa_handler = thread_hold;
  if (sigaction(SIGUSR1, &act, NULL) == -1) {
    perror("Adding SIGUSR1 signal handler failed");
    return NULL;
  }
  while(1) {
    while(CHECK_WAIT(sem_wait(&thread->job_available)));
    if (!thread->work) break;
    (*thread->work)(thread->data);
    while(CHECK_WAIT(sem_wait(&thread->pool->thread_list_lock)));
    thread->pool->free_thread_count++;
#ifdef DEBUG
    if (me == thread->pool->first_free_thread) {
      fprintf(stderr, "Error, completing work in a thread indicated as free: %d\n", me);
    }
#endif
#ifdef DEBUG
    if (logbuffer_len < sizeof(logbuffer) - 1000) {
      logbuffer_len += sprintf(logbuffer + logbuffer_len, "Me: %d, old next free: %d\n", me, thread->next_free);
    }
#endif
    thread->next_free = thread->pool->first_free_thread;
    thread->pool->first_free_thread = me;
#ifdef DEBUG
    if (logbuffer_len < sizeof(logbuffer) - 1000) {
      logbuffer_len += sprintf(logbuffer + logbuffer_len, "Me: %d, new next free: %d\n", me, thread->pool->first_free_thread);
    }
#endif
    CHECK_POST(sem_post(&thread->pool->thread_list_lock));
    CHECK_POST(sem_post(&thread->pool->next_free_thread));
  }
  my_thread = NULL;
}

/* returns NULL on error */
struct thpool_* thpool_init(int num_threads) {
  struct thpool_* pool = (struct thpool_*)malloc(sizeof(struct thpool_)+num_threads*sizeof(struct thread));
  if (pool != NULL) {
    pool->num_threads = num_threads;
    sem_init(&pool->next_free_thread, 0, num_threads);
    sem_init(&pool->thread_list_lock, 0, 1);
    sem_init(&pool->hold, 0, 0);
    pool->first_free_thread = 0;
    pool->free_thread_count = num_threads;
    for (int i = 0;i < num_threads;i++) {
      pool->threads[i].next_free = (i == num_threads-1) ? -1 : i + 1;
      sem_init(&pool->threads[i].job_available, 0, 0);
      pool->threads[i].pool = pool;
      pthread_create(&pool->threads[i].thread, NULL, thread_function, &pool->threads[i]);
    }
  }
  return pool;
}

/* acts like a barrier. Additional work could be added
   concurrently while it restores the free thread semaphore counter */
void thpool_wait(struct thpool_* pool) {
  for (int i = 0;i < pool->num_threads;i++) {
    while(CHECK_WAIT(sem_wait(&pool->next_free_thread)));
  }
  for (int i = 0;i < pool->num_threads;i++) {
    CHECK_POST(sem_post(&pool->next_free_thread));
  }
}

int thpool_add_work(struct thpool_* pool, void (*work)(void*), void* data) {
  int thread;
  while(CHECK_WAIT(sem_wait(&pool->next_free_thread)));
  while(CHECK_WAIT(sem_wait(&pool->thread_list_lock)));
  thread = pool->first_free_thread;
#ifdef DEBUG
  if (thread < 0) {
    fprintf(stderr, "Error, first free thread is negative: %d\n", thread);
  }
#endif
  pool->first_free_thread = pool->threads[thread].next_free;
#ifdef DEBUG
  pool->threads[thread].next_free = -1;
#endif
#ifdef DEBUG
  if (logbuffer_len < sizeof(logbuffer) - 1000) {
    logbuffer_len += sprintf(logbuffer + logbuffer_len, "New first free: %d, thread: %d\n", pool->first_free_thread, thread);
  }
#endif
  pool->free_thread_count--;
  CHECK_POST(sem_post(&pool->thread_list_lock));
  pool->threads[thread].work = work;
  pool->threads[thread].data = data;
  CHECK_POST(sem_post(&pool->threads[thread].job_available));
  return 0;
}

void thpool_destroy(struct thpool_* pool) {
  thpool_wait(pool);
  for (int i = 0;i < pool->num_threads;i++) {
    pool->threads[i].work = NULL;
    CHECK_POST(sem_post(&pool->threads[i].job_available));
  }
  for (int i = 0;i < pool->num_threads;i++) {
    pthread_join(pool->threads[i].thread, NULL);
  }
  for (int i = 0;i < pool->num_threads;i++) {
    sem_destroy(&pool->threads[i].job_available);
  }
  sem_destroy(&pool->thread_list_lock);
  sem_destroy(&pool->next_free_thread);
  free(pool);
}

int thpool_num_threads_working(struct thpool_* pool) {
  int num;
  while(CHECK_WAIT(sem_wait(&pool->thread_list_lock)));
  num = pool->num_threads - pool->free_thread_count;
  CHECK_POST(sem_post(&pool->thread_list_lock));
  return num;
}

void thpool_pause(struct thpool_* pool) {
  for (int i = 0;i < pool->num_threads;i++) {
    pthread_kill(pool->threads[i].thread, SIGUSR1);
  }
}

void thpool_resume(struct thpool_* pool) {
  for (int i = 0;i < pool->num_threads;i++) {
    CHECK_POST(sem_post(&pool->hold));
  }
}

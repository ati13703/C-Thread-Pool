struct thpool_;
typedef struct thpool_* threadpool;

struct thpool_* thpool_init(int num_threads);
void thpool_wait(struct thpool_* pool);
int thpool_add_work(struct thpool_* pool, void (*work)(void*), void* data);
void thpool_destroy(struct thpool_* pool);
int thpool_num_threads_working(struct thpool_* pool);
void thpool_pause(struct thpool_* pool);
void thpool_resume(struct thpool_* pool);

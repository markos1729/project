#ifndef PROJECT_JOBSCHEDULER_H
#define PROJECT_JOBSCHEDULER_H


#define NUMBER_OF_THREADS 16


#include <queue>
#include <pthread.h>
#include <stdlib.h>


using namespace std;


class Job {                        // extend this to whatever job you want scheduled (in a different .cpp/.h)
public:
    virtual ~Job() {}
    virtual bool run() = 0;        // must be thread_safe
};


struct thread_args {
    queue <Job *> *jobQueue;
    pthread_mutex_t *queueLock;
    pthread_cond_t *queueCond;
    volatile unsigned int *jobsRunning;
    volatile bool *threads_must_exit;
    pthread_cond_t *jobs_finished_cond;
    thread_args(queue<Job *> *_jobQueue, pthread_mutex_t *_queueLock, pthread_cond_t *_queueCond,
                volatile unsigned int *_jobsRunning, volatile bool *_threads_must_exit, pthread_cond_t *_jobs_finished_cond)
            : jobQueue(_jobQueue), queueLock(_queueLock), queueCond(_queueCond), jobsRunning(_jobsRunning), threads_must_exit(_threads_must_exit), jobs_finished_cond(_jobs_finished_cond) {}
};


void *thread_code(void *args);


class JobScheduler {
    queue<Job *> job_queue;
    pthread_t threads[NUMBER_OF_THREADS];
    pthread_mutex_t queue_lock;
    pthread_cond_t queue_cond;               // true -> not empty
    volatile unsigned int jobs_running;      // protected by queue_lock
    volatile bool threads_must_exit;
    pthread_cond_t jobs_finished_cond;
    thread_args *t_args;
public:
    JobScheduler();
    ~JobScheduler();                         // Waits until all jobs have finished!
    void schedule(Job *job);
    bool allJobsHaveFinished();
    void waitUntilAllJobsHaveFinished();
};


#endif

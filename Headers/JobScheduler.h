#ifndef PROJECT_JOBSCHEDULER_H
#define PROJECT_JOBSCHEDULER_H

#define NUMBER_OF_THREADS 16
#define SCHEDULER_WAITS_FOR_JOBS_TO_FINISH   // define this if JobScheduler should wait for all jobs in queue to finish before dying


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
    thread_args(queue<Job *> *_jobQueue, pthread_mutex_t *_queueLock, pthread_cond_t *_queueCond, volatile unsigned int *_jobsRunning) : jobQueue(_jobQueue), queueLock(_queueLock), queueCond(_queueCond), jobsRunning(_jobsRunning) {}
};


void *thread_code(void *args);


class JobScheduler {
    queue<Job *> job_queue;
    pthread_t threads[NUMBER_OF_THREADS];
    pthread_mutex_t queue_lock;
    pthread_cond_t queue_cond;     // true -> not empty
    volatile unsigned int jobs_running;     // protected by queue_lock
public:
    JobScheduler();
    ~JobScheduler();
    void schedule(Job *job);
    bool allJobsHaveFinished();
    void waitUntilAllJobsHaveFinished();
};


#endif

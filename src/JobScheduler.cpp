#include <iostream>
#include "../Headers/JobScheduler.h"


#define CHECK_PERROR(call, msg, actions) { if ( (call) < 0 ) { perror(msg); actions } }


using namespace std;

/* Globals for control */
volatile bool threads_must_exit;
#ifdef SCHEDULER_WAITS_FOR_JOBS_TO_FINISH
pthread_cond_t exit_cond;
#endif


/* JobScheduler Implementation */
JobScheduler::JobScheduler() {
    threads_must_exit = false;
    CHECK_PERROR(pthread_mutex_init(&queue_lock, NULL), "pthread_mutext_init failed",)
    CHECK_PERROR(pthread_cond_init(&queue_cond, NULL), "pthread_cond_init failed",)
#ifdef SCHEDULER_WAITS_FOR_JOBS_TO_FINISH
    CHECK_PERROR(pthread_cond_init(&exit_cond, NULL), "pthread_cond_init failed",)
#endif
    static struct thread_args args(&job_queue, &queue_lock, &queue_cond);   // (!) static so that they exist throughout the program
    for (int i = 0; i < NUMBER_OF_THREADS; i++) {
        CHECK_PERROR(pthread_create(&threads[i], NULL, thread_code, (void *) &args), "pthread_create failed", threads[i] = 0;)
    }
}

JobScheduler::~JobScheduler() {
#ifdef SCHEDULER_WAITS_FOR_JOBS_TO_FINISH
    CHECK_PERROR(pthread_mutex_lock(&queue_lock), "pthread_mutex_lock failed", )
    while (!job_queue.empty()){
        CHECK_PERROR(pthread_cond_wait(&exit_cond, &queue_lock) , "pthread_cond_wait failed", )
    }
    CHECK_PERROR(pthread_mutex_unlock(&queue_lock), "pthread_mutex_unlock failed", )
#endif
    threads_must_exit = true;
    CHECK_PERROR(pthread_cond_broadcast(&queue_cond), "pthread_broadcast failed", )
    for (int i = 0; i < NUMBER_OF_THREADS; i++) {
        CHECK_PERROR(pthread_join(threads[i], NULL), "pthread_join failed", )
    }
    CHECK_PERROR(pthread_mutex_destroy(&queue_lock), "pthread_mutex_destroy failed", )
    CHECK_PERROR(pthread_cond_destroy(&queue_cond), "pthread_cond_destroy failed", )
#ifdef SCHEDULER_WAITS_FOR_JOBS_TO_FINISH
    CHECK_PERROR(pthread_cond_destroy(&exit_cond), "pthread_cond_destroy failed", )
#endif
}

void JobScheduler::schedule(Job *job) {
    CHECK_PERROR(pthread_mutex_lock(&queue_lock), "pthread_mutex_lock failed", )
    bool wasEmpty = job_queue.empty();
    job_queue.push(job);
    if (wasEmpty) {   // wake up one blocked thread to pick up this job
        CHECK_PERROR(pthread_cond_signal(&queue_cond), "pthread_cond_signal failed", )
    }
    CHECK_PERROR(pthread_mutex_unlock(&queue_lock), "pthread_mutex_unlock failed", )
}

bool JobScheduler::hasNoMoreJobs() {
    bool result;
    CHECK_PERROR(pthread_mutex_lock(&queue_lock), "pthread_mutex_lock failed", )
    result = job_queue.empty();
    CHECK_PERROR(pthread_mutex_unlock(&queue_lock), "pthread_mutex_unlock failed", )
    return result;
}


/* Thread code: */
void *thread_code(void *args) {
    struct thread_args *argptr = ((struct thread_args *) args);
    queue<Job *> *job_queue = argptr->jobQueue;
    pthread_mutex_t *queue_lock = argptr->queueLock;
    pthread_cond_t *queue_cond = argptr->queueCond;
    while (!threads_must_exit) {
        Job *job;
        CHECK_PERROR(pthread_mutex_lock(queue_lock), "pthread_mutex_lock failed", continue; )
        while (job_queue->empty() && !threads_must_exit){
            CHECK_PERROR(pthread_cond_wait(queue_cond, queue_lock) , "pthread_cond_wait failed", )
        }
        if (threads_must_exit){
            CHECK_PERROR(pthread_mutex_unlock(queue_lock), "pthread_mutex_unlock failed", )
            break;
        }
        job = job_queue->front();     // get pointer to next job
        job_queue->pop();             // remove it from queue
        CHECK_PERROR(pthread_mutex_unlock(queue_lock), "pthread_mutex_unlock failed", )
        job->run();
#ifdef SCHEDULER_WAITS_FOR_JOBS_TO_FINISH
        CHECK_PERROR(pthread_cond_signal(&exit_cond), "pthread_cond_signal failed", )
#endif
    }
    pthread_exit((void *) 0);
}

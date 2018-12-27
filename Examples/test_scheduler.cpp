#include <iostream>
#include "../Headers/JobScheduler.h"

#define NUM_OF_JOBS 20

using namespace std;


class SayHi : public Job {
    int num;
public:
    void set_num(int _num) { num = _num; }
    void run(){
        cout << "Hi, I am thread with number " << num << endl;
    }
};


int main(){
    SayHi jobs[NUM_OF_JOBS];
    for (int i = 0 ; i < NUM_OF_JOBS ; i++){
        jobs[i].set_num(i);
    }
    JobScheduler scheduler;
    for (int i = 0 ; i < NUM_OF_JOBS ; i++) {
        scheduler.schedule(&jobs[i]);
    }
    // destructor called here
    return 0;
}

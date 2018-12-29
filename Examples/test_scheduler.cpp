#include <iostream>
#include "../Headers/JobScheduler.h"

#define NUM_OF_JOBS 20

using namespace std;


class SayHi : public Job {
    int num;
public:
    SayHi(int _num) : num(_num) { }
    bool run(){
        for (int i = 0 ; i < 9999999 ; i++) { /* waste time */ }
	cout << "Hi, I am thread with number " << num << endl;
        return true;
    }
};


int main() {
    JobScheduler scheduler;
    for (int i = 0 ; i < NUM_OF_JOBS ; i++) {
        scheduler.schedule(new SayHi(i));
    }
    if (scheduler.allJobsHaveFinished()){
	cout << "All jobs finished before check!" << endl;
    } else {
        scheduler.waitUntilAllJobsHaveFinished();
	cout << "All jobs finished and had to wait for them" << endl;
    }
    return 0;
}

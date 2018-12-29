#include <iostream>
#include "Headers/Relation.h"
#include "Headers/JoinResults.h"
#include "Headers/RadixHashJoin.h"
#include "Headers/JobScheduler.h"

using namespace std;


JobScheduler *scheduler = NULL;


/* Local Functions */
void printResults(Result *RxS, const Relation &R, const Relation &S);


int main(int argc, char *argv[]) {
    if (argc != 5) {
        cout << "Invalid arguments. Input must be:" << endl << "./project <Relation1> <JoinField1> <Relation2> <JoinField2>\n(join field numbers start from 1)" << endl;
        return -1;
    }
    int joinField1 = atoi(argv[2]), 
        joinField2 = atoi(argv[4]);
    if ( joinField1 <= 0 || joinField2 <= 0 ){
        cout << "Invalid JoinField parameters. Input must be:" << endl << "./project <Relation1> <JoinField1> <Relation2> <JoinField2>\n(join field numbers start from 1)" << endl;
        return -2;
    }
    try {
        Relation R(argv[1]);
        if ((unsigned int) joinField1 > R.getNumOfColumns()){
            cout << "Invalid JoinField1 parameter: no such column Relation1" << endl;
            return -3;
        }

        Relation S(argv[3]);
        if ((unsigned int) joinField2 > S.getNumOfColumns()){
            cout << "Invalid JoinField2 parameter: no such column in Relation2" << endl;
            return -3;
        }

        scheduler = new JobScheduler();

        JoinRelation *JRptr = R.extractJoinRelation(joinField1 - 1);
        JoinRelation *JSptr = S.extractJoinRelation(joinField2 - 1);
        JoinRelation &JR = *JRptr;
        JoinRelation &JS = *JSptr;

        Result *RxS = radixHashJoin(JR, JS);

        delete scheduler;

        if (RxS != NULL){
            printResults(RxS, R, S);      // print results to stdout. User can redirect this to a file
            delete RxS;
        }

        delete JRptr;
        delete JSptr;
    } catch (...) {
        printf("Could not load relations\n");
        return -4;
    }
    return 0;
}


/* Local Functions Implementation */
void printResults(Result *RxS, const Relation &R, const Relation &S) {
    // Note: This function assumes that column numbers can fit in 2 precision digits and intField values can fit in 12 precision digits in order to be pretty
    printf(" Radix Hash Join results:\n");
    for (unsigned int i = 0 ; i < R.getNumOfColumns() ; i++){
        printf(" Relation1.%2d |", i+1);
    }
    for (unsigned int i = 0 ; i < S.getNumOfColumns() ; i++){
        printf("| Relation2.%2d ", i+1);
    }
    printf("\n");
    Iterator p(RxS);
    unsigned int Rid, Sid;
    while (p.getNext(Rid, Sid)) {
        for (unsigned int i = 0 ; i < R.getNumOfColumns() ; i++){
            printf(" %12lu |", R.getValueAt(i, Rid - 1) );
        }
        for (unsigned int i = 0 ; i < S.getNumOfColumns() ; i++){
            printf("| %12lu ", S.getValueAt(i, Sid - 1));
        }
        printf("\n");
    }
}

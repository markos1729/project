#include <iostream>
#include "Headers/Relation.h"
#include "Headers/JoinResults.h"
#include "Headers/RadixHashJoin.h"

using namespace std;

int main(int argc, char *argv[]) {
    if (argc != 3) {
        cout << "Invalid arguments: Expected 2 input binary files." << endl;
        return 1;
    }
    try {
        Relation R(argv[1]);
        Relation S(argv[2]);

        JoinRelation *JRptr = R.extractJoinRelation(1);
        JoinRelation *JSptr = S.extractJoinRelation(1);
        JoinRelation &JR = *JRptr;
        JoinRelation &JS = *JSptr;

        Result *J = radixHashJoin(JR, JS);

        // TODO: print result?

        delete JRptr;
        delete JSptr;
    } catch (...) {
        printf("Could not load relations\n");
        return 2;
    }
    cout << "RHJ completed successfully!" << endl;
    return 0;
}
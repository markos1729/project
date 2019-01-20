#include <iostream>
#include <cstring>
#include <unistd.h>
#include <vector>
#include "../Headers/SQLParser.h"
#include "../Headers/JobScheduler.h"
#include "../Headers/Relation.h"
#include "../Headers/Optimizer.h"
#include "../Headers/macros.h"


#define STARTING_VECTOR_SIZE 16


JobScheduler *scheduler = NULL;
unsigned int QueryRelation::NumberOfRelationsInQuery = 0;


using namespace std;


int main(){
    // first read line-by-line for relations' file names until read "Done"
    vector<string> *fileList = new vector<string>();
    fileList->reserve(STARTING_VECTOR_SIZE);
    string currName;
    while ( getline(cin, currName) && currName != "Done" ) {
        fileList->push_back(currName);
    }
    CHECK( !cin.eof() && !cin.fail() , "Error: reading file names from cin failed", delete fileList; return -1; )
    CHECK( !fileList->empty() , "Warning: No file names were given", delete fileList; return -1; )
    // and load all files into memory
    const int Rlen = (int) fileList->size();
    Relation **R = new Relation *[Rlen]();
    {
        unsigned int i = 0;
        try {
            for (auto iter = fileList->begin(); iter != fileList->end(); iter++, i++) {
                R[i] = new Relation((*iter).c_str());
            }
        } catch (...) {
            cerr << "Error: At least one of the files wasn't found or was inaccessible." << endl;
            for (auto iter = fileList->begin(); iter != fileList->end(); iter++, i++) delete R[i];
            delete fileList;
            return -1;
        }
    }
    delete fileList;
    // Calculate statistics for all tables (once in the beginning!)
    RelationStats **Rstats = new RelationStats *[Rlen];
    for (int i = 0 ; i < Rlen ; i++){
        Rstats[i] = new RelationStats(R[i]);
        Rstats[i]->calculateStats();
    }
    // then start parsing 'sql' statements
    string currStatement;
    do {
        while (getline(cin, currStatement) && !cin.eof() && !cin.fail() && currStatement != "F") {

            cout << "Query is: " << currStatement << endl;

            // Parse line as an SQL-like statement
            SQLParser *p = new SQLParser(currStatement.c_str());
            CHECK( p->relations != NULL && p->projections != NULL , "Error: invalid query: \"" + currStatement + "\"", continue; )

            bool abort = false;

            // execute FROM: load original Relations to an array of pointers to such
            QueryRelation::set_nrelations(p->nrelations);          // (!) VERY-VERY important (must change each query)
            QueryRelation **QueryRelations = new QueryRelation*[p->nrelations]();
            int *seen_at = new int[Rlen]();
            for (int i = 0 ; i < Rlen ; i++) seen_at[i] = -1;      // init to -1
            for (unsigned int i = 0; i < p->nrelations ; i++){
                CHECK( p->relations[i] < Rlen, "SQL Error: SQL query contains non-existent Relation in \'FROM\'. Aborting query...", delete[] QueryRelations; delete p; abort = true; break; )
                if ( seen_at[p->relations[i]] == -1 ) {
                    QueryRelations[i] = R[p->relations[i]];
                    R[p->relations[i]]->setId	(i);                  // id of relations are in ascending order in 'FROM'
                    seen_at[p->relations[i]] = p->relations[i];
                } else {                                           // the second+ time we meet the same relation we create an IntermediateRelation for it
                    unsigned int tempsize = R[seen_at[p->relations[i]]]->getSize();
                    unsigned int *temprowids = new unsigned int[tempsize];
                    for (unsigned int j = 0 ; j < tempsize ; j++){ temprowids[j] = j + 1; }
                    QueryRelations[i] = new IntermediateRelation(i, temprowids, tempsize, R[seen_at[p->relations[i]]]);
                    delete[] temprowids;
                }
            }
            delete[] seen_at;
            if (abort) continue;


            //////////////////////////////
            /// Join Optimization here ///
            //////////////////////////////
            // Initialize optimizer with pointers to the correct stats for each rel_id in the query
            Optimizer *optimizer = new Optimizer(*p);
            for (unsigned int i = 0; i < p->nrelations; i++) {   // all QueryRelations here are still only consisted by 1 original relation
                optimizer->initializeRelation(i, Rstats[p->relations[i]]);   // (!) must map rel_id i to index of Rstats
            }
            // estimate filters
            optimizer->estimate_filters();
            // estimate equal columns
            optimizer->estimate_eqColumns();
            // estimate joins and come up with an optimal join-order plan according to assumptions
            int *bestJoinOrder = optimizer->best_plan();
            delete optimizer;

            // print best plan:
            cout << "Best plan is: ";
            for (int i = 0 ; i < p->npredicates ; i++){
                cout << bestJoinOrder[i] << " ";
            }
            cout << endl << endl;

            delete[] bestJoinOrder;

            // cleanup
            for (int i = 1 ; i < p->nrelations; i++){   // (!) DO NOT delete QueryRelations[0] as this will be cleaned up after the IOJob scheduled has finished
                if ( QueryRelations[i] != NULL && QueryRelations[i]->isIntermediate ) delete QueryRelations[i];
            }
            delete[] QueryRelations;                    // this is ok to delete, QueryRelations[0] pointer has been saved
            delete p;                                   // this is also ok to delete since we pass a copy of p->projections
        }
    } while ( !cin.eof() && !cin.fail() );
    // cleanup
    for (unsigned int i = 0 ; i < Rlen ; i++ ) {
        delete Rstats[i];
        delete R[i];
    }
    delete[] Rstats;
    delete[] R;
    return 0;
}

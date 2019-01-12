#include <iostream>
#include <cstring>
#include <unistd.h>
#include <vector>
#include "Headers/SQLParser.h"
#include "Headers/Relation.h"
#include "Headers/JobScheduler.h"
#include "Headers/Optimizer.h"
#include "Headers/macros.h"


#define STARTING_VECTOR_SIZE 16


#define PRINT_SUM                  // define this to print SUM of projected columns instead of their values
//#define PRINT_FEEDBACK_MESSAGES  // define this for extra feedback messages to be printed by joiner. Do not define this if the only stdout output you want is the results


using namespace std;


/* Global variables */
JobScheduler *scheduler = NULL;


/* Local Functions */
unsigned int find_rel_pos(QueryRelation **QueryRelations, unsigned int size, unsigned int rel_id);
unsigned int count_not_null(void **ptrarray, unsigned int size);


/* Local Structs */
class IOJob : public Job {
    QueryRelation *R;
    projection *projections;
    unsigned int nprojections;
public:
    IOJob(QueryRelation *_R, projection *_projections, unsigned int _nprojections) : Job(), R(_R), projections(_projections), nprojections(_nprojections) {}
    ~IOJob() {    // must cleanup QueryRelation and projections when done
        delete[] projections;
        delete R;
    }
    bool run();
};


int main(){
    #ifdef PRINT_FEEDBACK_MESSAGES
    cout << "Loading relations..." << endl;
    #endif
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
    #ifdef PRINT_FEEDBACK_MESSAGES
    cout << "Loading done! Ready to accept queries." << endl << endl;
    #endif
    // initing JobScheduler
    scheduler = new JobScheduler();
    JobScheduler IOScheduler(1);             // make a new Scheduler only for IO with 1 thread in order not to waste CPU cycles
    // then start parsing 'sql' statements
    #ifdef PRINT_FEEDBACK_MESSAGES
    unsigned int count = 0;
    #endif
    string currStatement;
    do {
        #ifdef PRINT_FEEDBACK_MESSAGES
        bool first = true;
        #endif
        while (getline(cin, currStatement) && !cin.eof() && !cin.fail() && currStatement != "F") {
            #ifdef PRINT_FEEDBACK_MESSAGES
            if (first){
                cout << "Running Batch #" << ++count << ":" << endl;
                first = false;
            }
            #endif

            // Parse line as an SQL-like statement
            SQLParser *p = new SQLParser(currStatement.c_str());
            CHECK( p->relations != NULL && p->projections != NULL , "Error: invalid query: \"" + currStatement + "\"", continue; )

            bool abort = false;

            // execute FROM: load original Relations to an array of pointers to such
            QueryRelation **QueryRelations = new QueryRelation*[p->nrelations]();
            int *seen_at = new int[Rlen]();
            for (int i = 0 ; i < Rlen ; i++) seen_at[i] = -1;      // init to -1
            for (unsigned int i = 0; i < p->nrelations ; i++){
                CHECK( p->relations[i] < Rlen, "SQL Error: SQL query contains non-existent Relation in \'FROM\'. Aborting query...", delete[] QueryRelations; delete p; abort = true; break; )
                if ( seen_at[p->relations[i]] == -1 ) {
                    QueryRelations[i] = R[p->relations[i]];
                    R[p->relations[i]]->setId(i);                  // id of relations are in ascending order in 'FROM'
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


            // execute WHERE: filters > equal columns > joins (+ equal columns if they end up to be)
            // First, do filters
            for (int i = 0 ; i < p->nfilters ; i++){
                //FILTER
                const filter &filter = p->filters[i];
                CHECK( (filter.rel_id < p->nrelations), "SQL Error: SQL filter contains a relation that does not exist in \'FROM\'. Aborting query...",
                       for (int ii = 0 ; ii < p->nrelations; ii++) { if ( QueryRelations[ii] != NULL && QueryRelations[ii]->isIntermediate ) delete QueryRelations[ii]; } delete[] QueryRelations; delete p; abort = true; break; )
                QueryRelation *prev = QueryRelations[filter.rel_id];
                QueryRelations[filter.rel_id] = QueryRelations[filter.rel_id]->performFilter(filter.rel_id, filter.col_id, filter.value, filter.cmp);
                CHECK(QueryRelations[filter.rel_id] != NULL, "Error: Could not execute filter: " + to_string(filter.rel_id) + filter.cmp + to_string(filter.col_id),
                      QueryRelations[filter.rel_id] = prev; )  // restore prev
            }
            if (abort) continue;

            // Then equal columns operations
            for (int i = 0 ; i < p->npredicates ; i++){
                if ( p->predicates[i].rela_id == p->predicates[i].relb_id && p->predicates[i].cola_id != p->predicates[i].colb_id){
                    // EQUAL COLUMNS UNARY OPERATION
                    const predicate &predicate = p->predicates[i];
                    CHECK( (predicate.rela_id < p->nrelations), "SQL Error: SQL equal columns predicate on one relation that does not exist in \'FROM\'. Aborting query...",
                           for (int ii = 0 ; ii < p->nrelations; ii++) { if ( QueryRelations[ii] != NULL && QueryRelations[ii]->isIntermediate ) delete QueryRelations[ii]; } delete[] QueryRelations; delete p; abort = true; break;)
                    QueryRelation *prev = QueryRelations[predicate.rela_id];
                    QueryRelations[predicate.rela_id] = QueryRelations[predicate.rela_id]->performEqColumns(predicate.rela_id, predicate.relb_id, predicate.cola_id, predicate.colb_id);
                    CHECK(QueryRelations[predicate.rela_id] != NULL, "Error: Could not execute equal columns: " + to_string(predicate.rela_id) + "." + to_string(predicate.cola_id) + "=" + to_string(predicate.relb_id) + "." + to_string(predicate.colb_id),
                          QueryRelations[predicate.rela_id] = prev; )  // restore prev
                }
                // else if p->predicates[i].rela_id == p->predicates[i].relb_id -> ignore predicate
            }
            if (abort) continue;


            //////////////////////////////
            /// Join Optimization here ///
            //////////////////////////////
            // Initialize optimizer with pointers to the correct stats for each rel_id in the query
            Optimizer *optimizer = new Optimizer(*p);
            for (unsigned int i = 0; i < p->nrelations; i++) {   // all QueryRelations here are still only consisted by 1 original relation
                CHECK( QueryRelations[i]->getNumOfColumns() > 0 , "Query Optimization Input Error: QueryRelation with more than one original relations?!",
                       for (int ii = 0 ; ii < p->nrelations; ii++) { if ( QueryRelations[ii] != NULL && QueryRelations[ii]->isIntermediate ) delete QueryRelations[ii]; } delete[] QueryRelations; delete p; abort = true; break; )
                optimizer->initializeRelation(i, Rstats[QueryRelations[i]->getOnlyRelId()]);
            }
            // estimate filters
            optimizer->estimate_filters();
            // estimate equal columns
            optimizer->estimate_eqColumns();
            // estimate joins and come up with an optimal join-order plan according to assumptions
            int *bestJoinOrder = optimizer->best_plan();
            delete optimizer;
            if (abort) continue;

            // perform all joins in the order found best
            for (int i = 0 ; i < p->npredicates ; i++){
                const predicate &predicate = p->predicates[bestJoinOrder[i]];   // in the order got from optimizer!
                CHECK( (predicate.rela_id < p->nrelations && predicate.relb_id < p->nrelations), "SQL Error: SQL join predicate contains a relation that does not exist in \'FROM\'. Aborting query...",
                       for (int ii = 0 ; ii < p->nrelations; ii++) { if ( QueryRelations[ii] != NULL && QueryRelations[ii]->isIntermediate ) delete QueryRelations[ii]; } delete[] QueryRelations; delete p; abort = true; break; )
                unsigned int rela_pos = find_rel_pos(QueryRelations, p->nrelations, predicate.rela_id);
                unsigned int relb_pos = find_rel_pos(QueryRelations, p->nrelations, predicate.relb_id);
                CHECK( rela_pos != -1 && relb_pos != -1, "Warning: searched for a rel_id that was not found in QueryRelations! rela_id = " + ( (rela_pos == -1) ? to_string(predicate.rela_id) : "OK") + " relb_id = " + ( (relb_pos == -1) ? to_string(predicate.relb_id) : "OK"), continue; )
                if ( rela_pos != relb_pos ){
                    // JOIN
                    bool failed = false;
                    if (rela_pos < relb_pos){
                        QueryRelation *prev = QueryRelations[rela_pos];
                        QueryRelations[rela_pos] = QueryRelations[rela_pos]->performJoinWith(*QueryRelations[relb_pos], predicate.rela_id, predicate.cola_id, predicate.relb_id, predicate.colb_id);
                        CHECK( QueryRelations[rela_pos] != NULL, "Error: Could not execute join: " + to_string(predicate.rela_id) + "." + to_string(predicate.cola_id) + "=" + to_string(predicate.relb_id) + "." + to_string(predicate.colb_id),
                               QueryRelations[rela_pos] = prev; failed = true; )
                        if (!failed) {
                            if (prev->isIntermediate && QueryRelations[relb_pos]->isIntermediate) delete QueryRelations[relb_pos];   // if only QueryRelations[relb_pos] is Intermediate then we do not want to delete it (!)
                            QueryRelations[relb_pos] = NULL;
                        }
                    } else {
                        QueryRelation *prev = QueryRelations[relb_pos];
                        QueryRelations[relb_pos] = QueryRelations[relb_pos]->performJoinWith(*QueryRelations[rela_pos], predicate.relb_id, predicate.colb_id, predicate.rela_id, predicate.cola_id);
                        CHECK( QueryRelations[relb_pos] != NULL, "Error: Could not execute join: " + to_string(predicate.rela_id) + "." + to_string(predicate.cola_id) + "=" + to_string(predicate.relb_id) + "." + to_string(predicate.colb_id),
                               QueryRelations[relb_pos] = prev; failed = true; )
                        if (!failed) {
                            if (prev->isIntermediate && QueryRelations[rela_pos]->isIntermediate) delete QueryRelations[rela_pos];
                            QueryRelations[rela_pos] = NULL;
                        }
                    }
                } else {   // (!) if two tables are joined two times then the first it will be join whilst the second time it will be an equal columns operation!
                    // EQUAL COLUMNS UNARY OPERATION (self join)
                    QueryRelation *prev = QueryRelations[rela_pos];   // (!) rela_id == relb_id
                    QueryRelations[rela_pos] = QueryRelations[rela_pos]->performEqColumns(predicate.rela_id, predicate.relb_id, predicate.cola_id, predicate.colb_id);
                    CHECK( QueryRelations[rela_pos] != NULL, "Error: Could not execute equal columns (self join): " + to_string(predicate.rela_id) + "." + to_string(predicate.cola_id) + "=" + to_string(predicate.relb_id) + "." + to_string(predicate.colb_id),
                           QueryRelations[rela_pos] = prev; )
                }
            }
            delete[] bestJoinOrder;
            if (abort) continue;
            CHECK( QueryRelations[0] != NULL, "Fatal error: Could not keep results to leftmost Intermediate QueryRelation (Should not happen). Please debug...",
                   for (int i = 0 ; i < p->nrelations; i++) { if ( QueryRelations[i] != NULL && QueryRelations[i]->isIntermediate ) delete QueryRelations[i]; } delete[] QueryRelations; delete p;
                   for (int i = 0 ; i < Rlen ; i++ ) { delete Rstats[i]; delete R[i]; } delete[] Rstats; delete[] R; delete scheduler; return -2; )

            // Last but not least any cross-products left to do
            int lastpos = 0;
            while ( count_not_null((void **) QueryRelations, p->nrelations) > 1 ){
                // CROSS PRODUCT: perform cross product between remaining Relations uniting them into one in the leftest position until only one QueryRelation remains
                int i = lastpos + 1;
                while ( i < p->nrelations && QueryRelations[i] == NULL ) i++;
                CHECK( i < p->nrelations,  "Warning: count_not_null does not work properly? Please debug...", break; )
                QueryRelation *prev = QueryRelations[0];
                QueryRelations[0] = QueryRelations[0]->performCrossProductWith(*QueryRelations[i]);
                CHECK( QueryRelations[0] != NULL, "Error: could not execute cross product", QueryRelations[0] = prev; continue; )
                if (QueryRelations[0]->isIntermediate && QueryRelations[i]->isIntermediate) delete QueryRelations[i];
                QueryRelations[i] = NULL;
                lastpos = i;
            }

            // schedules results to be printed to std::out
            projection *projections_copy = new projection[p->nprojections];
            for (unsigned int i = 0 ; i < p->nprojections ; i++) { projections_copy[i] = p->projections[i]; }
            IOScheduler.schedule(new IOJob(QueryRelations[0], projections_copy, p->nprojections));

            // cleanup
            for (int i = 1 ; i < p->nrelations; i++){   // (!) DO NOT delete QueryRelations[0] as this will be cleaned up after the IOJob scheduled has finished
                if ( QueryRelations[i] != NULL && QueryRelations[i]->isIntermediate ) delete QueryRelations[i];
            }
            delete[] QueryRelations;                    // this is ok to delete, QueryRelations[0] pointer has been saved
            delete p;                                   // this is also ok to delete since we pass a copy of p->projections
        }
        #ifdef PRINT_FEEDBACK_MESSAGES
        cout << endl;
        #endif
    } while ( !cin.eof() && !cin.fail() );
    IOScheduler.waitUntilAllJobsHaveFinished();
    // cleanup
    for (unsigned int i = 0 ; i < Rlen ; i++ ) {
        delete Rstats[i];
        delete R[i];
    }
    delete[] Rstats;
    delete[] R;
    delete scheduler;
    return 0;
}


/* Local Function Implementation */
unsigned int find_rel_pos(QueryRelation **QueryRelations, unsigned int size, unsigned int rel_id){
    for (unsigned int i = 0 ; i < size ; i++){
        if (QueryRelations[i] != NULL && QueryRelations[i]->containsRelation(rel_id)){
            return i;
        }
    }
    return -1;  // unsigned -> 111..1 -> should be caught by a CHECK(..)
}

unsigned int count_not_null(void **ptrarray, unsigned int size){
    unsigned int count = 0;
    for (unsigned int i = 0 ; i < size ; i++){
        if ( ptrarray[i] != NULL ) count++;
    }
    return count;
}

bool IOJob::run() {
    if (R == NULL || projections == NULL) return false;
    // Choose one (sum or select):
    #ifdef PRINT_SUM
    R->performSum(projections,nprojections);
    #else
    R->performSelect(projections, nprojections);
    #endif
    cout.flush();    // (!) important for harness to work
    return true;
}

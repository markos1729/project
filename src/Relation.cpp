#include <iostream>
#include <cmath>
#include <cstring>
#include <unordered_map>
#include <cstdio>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include "../Headers/Relation.h"
#include "../Headers/RadixHashJoin.h"
#include "../Headers/HashFunctions.h"
#include "../Headers/macros.h"
#include "../Headers/ConfigureParameters.h"


using namespace std;


extern JobScheduler *scheduler;


/* Join Relation Implementation */
JoinRelation::JoinRelation(unsigned int _size, const intField *_joinField, const unsigned int *_rowids) : size(_size), joinField(NULL), rowids(NULL), Psum(NULL), numberOfBuckets(0) {
    joinField = new intField[size];
    rowids = new unsigned int[size];
    for (unsigned int i = 0; i < size; i++) {
        joinField[i] = _joinField[i];
        rowids[i] = _rowids[i];
    }
}

JoinRelation::~JoinRelation() {
    delete[] Psum;
    delete[] joinField;
    delete[] rowids;
}

// (multi-threaded version) phase 1: partition in place JoinRelation R into buckets and fill Psum to distinguish them (|Psum| = num_of_buckets = 2^H1_N)
bool JoinRelation::partitionRelation(unsigned int H1_N) {
    if (this->getSize() == 0 || rowids == NULL || joinField == NULL ) return true;     // nothing to partition
    const unsigned int num_of_buckets = (unsigned int) 0x01 << H1_N;  // = 2^H1_N
    // 1) calculate Hist using available threads
    const unsigned int threads_to_use = MIN(CPU_CORES, scheduler->get_number_of_threads());
    unsigned int *Hist = new unsigned int[num_of_buckets]();          // all Hist[i] are initialized to 0
    unsigned int *bucket_nums = new unsigned int[size];
    unsigned int **tempHists = new unsigned int *[threads_to_use];
    const unsigned int chunk_size = size / threads_to_use + ((size % threads_to_use > 0) ? 1 : 0);
    for (int i = 0 ; i < threads_to_use; i++){
        tempHists[i] = new unsigned int[num_of_buckets]();            // (!) init to 0
        if (i * chunk_size < size ) scheduler->schedule(new HistJob(joinField, i * chunk_size, MIN((i+1) * chunk_size, size), tempHists[i], bucket_nums, H1_N));
    }
    scheduler->waitUntilAllJobsHaveFinished();
    // and then sum them up to create one Hist
    bool failed = false;
    for (unsigned int j = 0 ; j < threads_to_use ; j++){
        if (tempHists[j] == NULL) { failed = true; continue; }
        for (unsigned int i = 0 ; i < num_of_buckets ; i++){
            Hist[i] += tempHists[j][i];
        }
        delete[] tempHists[j];
    }
    delete[] tempHists;
    if (failed) { cerr << "Error: HistJob failed!" << endl; delete[] Hist; delete[] bucket_nums; return false; }   // should not happen
    // 2) convert Hist table to Psum table
    unsigned int sum = 0;
    for (unsigned int i = 0 ; i < num_of_buckets ; i++){
        unsigned int temp = Hist[i];
        Hist[i] = sum;
        sum += temp;
    }
    Psum = Hist;
    numberOfBuckets = num_of_buckets;
    // 3) create new re-ordered versions for joinField and rowids based on their bucket_nums using available threads
    intField *newJoinField = new intField[size]();
    unsigned int *newRowids = new unsigned int[size]();
    unsigned int *nextBucketPos = new unsigned int[num_of_buckets]();
    pthread_mutex_t *bucket_pos_locks = new pthread_mutex_t[num_of_buckets];   // one lock per bucket i to protect nextBocketPos[i]
    for (unsigned int i = 0 ; i < num_of_buckets ; i++){
        CHECK_PERROR(pthread_mutex_init(&bucket_pos_locks[i], NULL) , "pthread_mutex_init failed", )
    }
    for (int i = 0 ; i < threads_to_use ; i++){
        if (i * chunk_size < size ) scheduler->schedule(new PartitionJob(newJoinField, joinField, newRowids, rowids, nextBucketPos, i * chunk_size, MIN((i + 1) * chunk_size, size), num_of_buckets, bucket_nums, Psum, bucket_pos_locks));
    }
    scheduler->waitUntilAllJobsHaveFinished();
    for (unsigned int i = 0 ; i < num_of_buckets ; i++){
        CHECK_PERROR(pthread_mutex_destroy(&bucket_pos_locks[i]) , "pthread_mutex_init failed", )
    }
    delete[] bucket_pos_locks;
    delete[] nextBucketPos;
    delete[] bucket_nums;
    // 4) overwrite joinField and rowids with new re-ordered versions of it
    delete[] joinField;
    joinField = newJoinField;
    delete[] rowids;
    rowids = newRowids;
    return true;
}

// (single-threaded version) phase 1: partition in place JoinRelation R into buckets and fill Psum to distinguish them (|Psum| = num_of_buckets = 2^H1_N)
bool JoinRelation::partitionRelationSequentially(unsigned int H1_N) {
    if (this->getSize() == 0 || rowids == NULL || joinField == NULL ) return true;     // nothing to partition
    const unsigned int num_of_buckets = (unsigned int) pow(2, H1_N);
    // 1) calculate Hist - O(n)
    unsigned int *Hist = new unsigned int[num_of_buckets]();    // all Hist[i] are initialized to 0
    unsigned int *bucket_nums = new unsigned int[size];
    for (unsigned int i = 0 ; i < size ; i++){
        bucket_nums[i] = H1(joinField[i], H1_N);
        if ( bucket_nums[i] >= num_of_buckets ){ delete[] Hist; delete[] bucket_nums; return false; }  // ERROR CHECK
        Hist[bucket_nums[i]]++;
    }
    // 2) convert Hist table to Psum table
    unsigned int sum = 0;
    for (unsigned int i = 0 ; i < num_of_buckets ; i++){
        unsigned int temp = Hist[i];
        Hist[i] = sum;
        sum += temp;
    }
    Psum = Hist;
    numberOfBuckets = num_of_buckets;
    // 3) create new re-ordered versions for joinField and rowids based on their bucket_nums - O(n)
    intField *newJoinField = new intField[size]();
    unsigned int *newRowids = new unsigned int[size]();
    unsigned int *nextBucketPos = new unsigned int[num_of_buckets]();
    for (unsigned int i = 0 ; i < size ; i++) {
        const unsigned int pos = Psum[bucket_nums[i]] + nextBucketPos[bucket_nums[i]];   // value's position in the re-ordered version
        if ( pos >= size ) { delete[] newJoinField; delete[] newRowids; delete[] bucket_nums; delete[] nextBucketPos; Psum = NULL; numberOfBuckets = 0;  return false; }  // ERROR CHECK
        newJoinField[pos] = joinField[i];
        newRowids[pos] = rowids[i];
        nextBucketPos[bucket_nums[i]]++;
    }
    delete[] nextBucketPos;
    delete[] bucket_nums;
    // 4) overwrite joinField and rowids with new re-ordered versions of it
    delete[] joinField;
    joinField = newJoinField;
    delete[] rowids;
    rowids = newRowids;
    return true;
}


#ifdef DDEBUG
void JoinRelation::printDebugInfo() {
    if (Psum != NULL) {
        printf("This JoinRelation is partitioned.\n%u buckets created with Psum as follows:\n", numberOfBuckets);
        for (unsigned int i = 0; i < numberOfBuckets; i++) {
            printf("Psum[%u] = %u\n", i, Psum[i]);
        }
    }
    printf(" joinField | rowids\n");
    for (unsigned int i = 0 ; i < size ; i++){
        printf("%10u | %u\n", (unsigned int) joinField[i], rowids[i]);
    }
}
#endif


/* QueryRelation Implementation */
bool *QueryRelation::filterField(intField *field, unsigned int size, intField value, char cmp, unsigned int &count){
#ifdef DO_FILTER_PARALLEL
    const unsigned int threads_to_use = MIN(CPU_CORES, scheduler->get_number_of_threads());
    const unsigned int chunk_size = size / threads_to_use + ((size % threads_to_use > 0) ? 1 : 0);
    class FilterJob : public Job {
        const unsigned int start, end;
        const char cmp;
        intField *field;
        intField value;
        unsigned int *countptr;
        bool *filter;
    public:
        FilterJob(unsigned int s, unsigned int e, char c, unsigned int *cptr, intField *f, intField v, bool *fltr)
                 : Job(), start(s), end(e), cmp(c), countptr(cptr), field(f), value(v), filter(fltr) {}
        bool run(){
            (*countptr) = 0;
            switch(cmp){
                case '>':
                    for (unsigned int i = start ; i < end ; i++) {
                        filter[i] = (field[i] > value);
                        if (filter[i]) (*countptr)++;
                    }
                    break;
                case '<':
                    for (unsigned int i = start ; i < end ; i++) {
                        filter[i] = (field[i] < value);
                        if (filter[i]) (*countptr)++;
                    }
                    break;
                case '=':
                    for (unsigned int i = start ; i < end ; i++) {
                        filter[i] = (field[i] == value);
                        if (filter[i]) (*countptr)++;
                    }
                    break;
                default:
                    cerr << "Warning: Unknown comparison symbol from parsing" << endl;
                    return false;
            }
            return true;
        }
    };
    bool *filter = new bool[size]();
    unsigned int *thread_count = new unsigned int[threads_to_use]();
    for (int i = 0 ; i < threads_to_use ; i++){
        unsigned int start = i * chunk_size, end = MIN((i+1) * chunk_size, size);
        if (i * chunk_size < size ) {
            scheduler->schedule(new FilterJob(start, end, cmp, &thread_count[i], field, value, filter));
        }
    }
    scheduler->waitUntilAllJobsHaveFinished();
    count = 0;
    for (int i = 0 ; i < threads_to_use ; i++) {
        count += thread_count[i];
    }
    delete[] thread_count;
    return filter;
#else
    count = 0;
    bool *filter = new bool[size]();
    switch(cmp){
        case '>':
            for (unsigned int i = 0 ; i < size ; i++) {
                filter[i] = (field[i] > value);
                if (filter[i]) count++;
            }
            break;
        case '<':
            for (unsigned int i = 0 ; i < size ; i++) {
                filter[i] = (field[i] < value);
                if (filter[i]) count++;
            }
            break;
        case '=':
            for (unsigned int i = 0 ; i < size ; i++) {
                filter[i] = (field[i] == value);
                if (filter[i]) count++;
            }
            break;
        default:
            cerr << "Warning: Unknown comparison symbol from parsing" << endl;
            break;
    }
    return filter;
#endif
}

bool *QueryRelation::eqColumnsFields(intField *field1, intField *field2, unsigned int size, unsigned int &count) {
#ifdef DO_EQUALCOLUMNS_PARALLEL
    const unsigned int threads_to_use = MIN(CPU_CORES, scheduler->get_number_of_threads());
    const unsigned int chunk_size = size / threads_to_use + ((size % threads_to_use > 0) ? 1 : 0);
    class EqColumnsJob : public Job {
        const unsigned int start, end;
        intField *field1, *field2;
        unsigned int *countptr;
        bool *eqColumns;
    public:
        EqColumnsJob(unsigned int s, unsigned int e, unsigned int *cptr, intField *f1, intField *f2, bool *eqcl)
                    : Job(), start(s), end(e), countptr(cptr), field1(f1), field2(f2), eqColumns(eqcl) {}
        bool run() {
            *(countptr) = 0;
            for (unsigned int i = start; i < end; i++) {
                eqColumns[i] = (field1[i] == field2[i]);
                if (eqColumns[i]) (*countptr)++;
            }
            return true;
        }
    };
    bool *eqColumns = new bool[size]();
    unsigned int *thread_count = new unsigned int[threads_to_use]();
    for (int i = 0 ; i < threads_to_use ; i++) {
        unsigned int start = i * chunk_size, end = MIN((i+1) * chunk_size, size);
        if (i * chunk_size < size ) {
            scheduler->schedule(new EqColumnsJob(start, end, &thread_count[i], field1, field2, eqColumns));
        }
    }
    scheduler->waitUntilAllJobsHaveFinished();
    count = 0;
    for (int i = 0 ; i < threads_to_use ; i++) {
        count += thread_count[i];
    }
    delete[] thread_count;
    return eqColumns;
#else
    count = 0;
    bool *eqColumns = new bool[size]();
    for (unsigned int i = 0 ; i < size ; i++){
        eqColumns[i] = (field1[i] == field2[i]);
        if (eqColumns[i]) count++;
    }
    return eqColumns;
#endif
}


/* Relation Implementation */
Relation::Relation(unsigned int _size, unsigned int _num_of_columns) : QueryRelation(false), allocatedWithMmap(false), id(-1), size(_size), num_of_columns(_num_of_columns) {
    columns = new intField*[_num_of_columns]();   // initialize to NULL
}

Relation::Relation(const char* file) : QueryRelation(false), allocatedWithMmap(true), id(-1) , size(0) {
    int fd = open(file,O_RDONLY);
    if (fd == -1) throw 0;

    struct stat sb;
    if (fstat(fd, &sb) == -1) throw 0;

    void *p = mmap(0,sb.st_size,PROT_READ,MAP_PRIVATE,fd,0);
    if (p == MAP_FAILED) throw 0;

    intField *all = (intField*) p;
    size = all[0];
    num_of_columns = all[1];
    columns = new intField*[num_of_columns]();
    all += 2;

    for (unsigned int i = 0; i < num_of_columns; ++i) {
        columns[i] = all;
        all += size;
    }
}

Relation::~Relation() {
    if (allocatedWithMmap) {
        if (size > 0) munmap(columns[0] - 2, (size * num_of_columns + 2) * sizeof(intField));
    } else {
        for (unsigned int i = 0 ; i < num_of_columns ; i++){
            delete[] columns[i];   // "delete" accounts for possible NULL value
        }
    }
    delete[] columns;
}

intField Relation::getValueAt(unsigned int columnNum, unsigned int rowId) const {
    if (columns != NULL && columnNum < num_of_columns && columns[columnNum] != NULL && rowId < size) return columns[columnNum][rowId];
    else return 0;
}

bool Relation::addColumn(unsigned int col_num, const intField *values) {   // (!) values must be of length == size, lest we get seg fault
    if (col_num >= num_of_columns) return false;
    if (columns[col_num] != NULL) {
        delete[] columns[col_num];   // overwrite previous column
    }
    columns[col_num] = new intField[size];
    for (unsigned int i = 0 ; i < size ; i++){
        columns[col_num][i] = values[i];
    }
    return true;
}

JoinRelation *Relation::extractJoinRelation(unsigned int index_of_JoinField) const {
    unsigned int *rowids = new unsigned int[size];
    for (unsigned int i = 0 ; i < size ; i++) { rowids[i] = i+1; }
    JoinRelation *res = new JoinRelation(size, columns[index_of_JoinField], rowids);
    delete[] rowids;
    return res;
}

IntermediateRelation *Relation::performFilter(unsigned int rel_id, unsigned int col_id, intField value, char cmp) {
    // Note: rel_id is not needed here as there is only one relation to filter
    unsigned int count = 0;
    bool *passing_rowids = QueryRelation::filterField(columns[col_id], size, value, cmp, count);   // count will change accordingly
    unsigned int *newrowids = getPassingRowIds(passing_rowids, count);
    delete[] passing_rowids;
    IntermediateRelation *result = new IntermediateRelation(this->id, newrowids, count, this);
    delete[] newrowids;
    return result;
}

IntermediateRelation *Relation::performEqColumns(unsigned int rela_id, unsigned int relb_id, unsigned int cola_id, unsigned int colb_id) {
    // Note: rela_id and relb_id are not needed here as there is only one relation to perform eq columns on
    unsigned int count = 0;
    bool *passing_rowids = QueryRelation::eqColumnsFields(columns[cola_id], columns[colb_id], size, count);   // count will change accordingly
    unsigned int *newrowids = getPassingRowIds(passing_rowids, count);
    delete[] passing_rowids;
    IntermediateRelation *result = new IntermediateRelation(this->id, newrowids, count, this);
    delete[] newrowids;
    return result;
}

unsigned int *Relation::getPassingRowIds(const bool *passing_rowids, unsigned int count) {
    unsigned int *newrowids = NULL;
    if (count > 0) {
        newrowids = new unsigned int[count];
        unsigned int j = 0;
        for (unsigned int i = 0; i < size; i++) {
            if (passing_rowids[i]) {
                CHECK( j < count, "Warning: miscounted passing rowids in Relation::performEqColumns()", break; )
                newrowids[j++] = i + 1;   // keep rowid for intermediate, not the intField columns[cold_id][i].
            }
        }
    }
    return newrowids;
}


IntermediateRelation *Relation::performJoinWith(QueryRelation &B, unsigned int rela_id, unsigned int cola_id, unsigned int relb_id, unsigned int colb_id) {
    return (B.isIntermediate) ? performJoinWithIntermediate((IntermediateRelation &) B, rela_id, cola_id, relb_id, colb_id) : performJoinWithOriginal((Relation &) B, rela_id, cola_id, relb_id, colb_id);
}

IntermediateRelation *Relation::performJoinWithOriginal(const Relation &B, unsigned int rela_id, unsigned int cola_id, unsigned int relb_id, unsigned int colb_id) {
    // extract the correct join relations
    JoinRelation *JA = extractJoinRelation(cola_id);
    JoinRelation *JB = B.extractJoinRelation(colb_id);

    // run RadixHashJoin
    Result *AxB = radixHashJoin(*JA, *JB);
    delete JA;
    delete JB;

    // get # of join tuples
    unsigned long long int number_of_tuples = AxB->getSize();

    IntermediateRelation *result = NULL;
    if (number_of_tuples > 0) {
        // create the new map
        unsigned int *rowids_a = new unsigned int[number_of_tuples];
        unsigned int *rowids_b = new unsigned int[number_of_tuples];

        // iterate over results to create IntermediateRelation
        Iterator I(AxB);
        unsigned int aid, bid, pos = 0;
        while (I.getNext(aid, bid)) {
            CHECK( aid > 0 && bid > 0, "WARNING: row id is zero in Relation::performJoinWithOriginal()", )
            rowids_a[pos] = aid;
            rowids_b[pos] = bid;
            pos++;
        }

        result = new IntermediateRelation(rela_id, relb_id, rowids_a, rowids_b, number_of_tuples, this, &B);
        delete[] rowids_b;
        delete[] rowids_a;
    } else {
        result = new IntermediateRelation(rela_id, relb_id, (unsigned int *) NULL, (unsigned int *) NULL, 0, this, &B);
    }
    delete AxB;
    return result;
}

IntermediateRelation *Relation::performJoinWithIntermediate(IntermediateRelation &B, unsigned int rela_id, unsigned int cola_id, unsigned int relb_id, unsigned int colb_id) {
    return B.performJoinWithOriginal(*this, relb_id, colb_id, rela_id, cola_id);   // symmetric
}

IntermediateRelation *Relation::performCrossProductWith(QueryRelation &B) {
    return (B.isIntermediate) ? performCrossProductWithIntermediate((IntermediateRelation &) B) : performCrossProductWithOriginal((Relation &) B);
}

IntermediateRelation *Relation::performCrossProductWithOriginal(const Relation &B) {
    unsigned int sizeB = B.getSize();
    unsigned long long number_of_tuples = size * sizeB;

    IntermediateRelation *result = NULL;
    if (number_of_tuples > 0) {
        // create the new map
        unsigned int *rowids_a = new unsigned int[number_of_tuples];
        unsigned int *rowids_b = new unsigned int[number_of_tuples];

        unsigned int pos = 0;
        // every row from A (this) is matched with every row from B
        for (unsigned int bi = 1; bi <= sizeB; bi++) {
            for (unsigned int ai = 1; ai <= size; ai++, pos++) {
                rowids_a[pos] = ai;
                rowids_b[pos] = bi;
            }
        }

        result = new IntermediateRelation(id, B.getId(), rowids_a, rowids_b, number_of_tuples, this, &B);
        delete[] rowids_b;
        delete[] rowids_a;
    } else {
        result = new IntermediateRelation(id, B.getId(), (unsigned int *) NULL, (unsigned int *) NULL, 0, this, &B);
    }
    return result;
}

IntermediateRelation *Relation::performCrossProductWithIntermediate(IntermediateRelation &B) {
    return B.performCrossProductWithOriginal(*this);   // symmetric
}

void Relation::performSelect(projection *projections, unsigned int nprojections) {
    if (size <= 0){
        for (unsigned int j = 0 ; j < nprojections ; j++) {
            printf("NULL ");
        }
        printf("\n\n");
        return;
    }
    for (unsigned int j = 0 ; j < nprojections ; j++){
        printf("%3d.%2d", projections[j].rel_id, projections[j].col_id);
    }
    printf("\n");
    for (unsigned int i = 0 ; i < size ; i++){
        for (unsigned int j = 0 ; j < nprojections ; j++){
            printf("%6lu", this->getValueAt(projections[j].col_id, i));
        }
        printf("\n");
    }
    printf("\n");
}

void Relation::performSum(projection *projections, unsigned int nprojections) {
    if (size == 0) {
        for (unsigned int k = 0 ; k < nprojections - 1 ; ++k) printf("NULL ");
        printf("NULL\n");
        return;
    }

    intField *sum = new intField[nprojections]();

    for (unsigned int i=0; i<size; ++i) {
        for (unsigned int j = 0; j < nprojections; ++j){
            sum[j] += this->getValueAt(projections[j].col_id, i);
        }
    }
    for (unsigned int k = 0; k < nprojections - 1 ; ++k) printf("%lu ", sum[k]);
    printf("%lu\n", sum[nprojections - 1]);

    delete[] sum;
}


/* IntermediateRelation Implementation */
IntermediateRelation::IntermediateRelation(unsigned int rel_id, unsigned int *_rowids, unsigned int _size, const Relation *original_rel, unsigned int _maplength) : QueryRelation(true), numberOfRelations(1), size(_size), maplength(_maplength) {
    CHECK( maplength > 0 ,"maplength = 0 in Intermediate constructor! Have you forgotten to set_nrelations in QueryRelation?", )
    rowids = new unsigned int *[maplength];
    originalRelations = new const Relation *[maplength];
    for (unsigned int i = 0 ; i < maplength ; i++){
        rowids[i] = NULL;
        originalRelations[i] = NULL;
    }
    if (size > 0) {
        unsigned int *column = new unsigned int[size];
        for (unsigned int i = 0; i < size; i++) {
            column[i] = _rowids[i];
        }
        rowids[rel_id] = column;
    }
    originalRelations[rel_id] = original_rel;
}

IntermediateRelation::IntermediateRelation(unsigned int rela_id, unsigned int relb_id, unsigned int *_rowids_a, unsigned int *_rowids_b, unsigned int _size, const Relation *original_rel_a, const Relation *original_rel_b, unsigned int _maplength) : QueryRelation(true), numberOfRelations(2), size(_size), maplength(_maplength) {
    CHECK( maplength > 0 ,"maplength = 0 in Intermediate constructor! Have you forgotten to set_nrelations in QueryRelation?", )
    rowids = new unsigned int *[maplength];
    originalRelations = new const Relation *[maplength];
    for (unsigned int i = 0 ; i < maplength ; i++){
        rowids[i] = NULL;
        originalRelations[i] = NULL;
    }
    if (size > 0) {
        unsigned int *column_a = new unsigned int[size];
        unsigned int *column_b = new unsigned int[size];
        for (unsigned int i = 0; i < size; i++) {
            column_a[i] = _rowids_a[i];
            column_b[i] = _rowids_b[i];
        }
        rowids[rela_id] = column_a;
        rowids[relb_id] = column_b;
    }
    originalRelations[rela_id] = original_rel_a;
    originalRelations[relb_id] = original_rel_b;
}

IntermediateRelation::~IntermediateRelation() {
    if (size > 0) {
        for (unsigned int i = 0 ; i < maplength ; i++){
            delete[] rowids[i];        // some might be NULL
        }
    }
    delete[] rowids;
    delete[] originalRelations;
}

JoinRelation *IntermediateRelation::extractJoinRelation(unsigned int rel_id, unsigned int col_id) {
    // find rel_id's original Relation in R
    const Relation *OriginalR = originalRelations[rel_id];
    CHECK(OriginalR != NULL, "Warning: rel_id invalid or originalRelations map corrupted in IntermediateRelation::extractJoinRelation()", return NULL; )
    // create intField for col_id
    intField *joinField = new intField[size];
    unsigned int *rowIds = new unsigned int[size];   // (!) rowids must be those of Intermediate rows!!!
    for(unsigned int i = 0 ; i < size ; i++){
        joinField[i] = OriginalR->getValueAt(col_id, rowids[rel_id][i] - 1);
        rowIds[i] = i + 1;
    }
    // create JoinRelation
    JoinRelation *result = new JoinRelation(size, joinField, rowIds);
    delete[] joinField;
    delete[] rowIds;
    return result;
}

IntermediateRelation *IntermediateRelation::performFilter(unsigned int rel_id, unsigned int col_id, intField value, char cmp) {
    if (size <= 0) return this;
    CHECK( rel_id < maplength, "Invalid rel_id: smaller than maplength = nrelations!", return NULL; )
    CHECK( rowids[rel_id] != NULL && originalRelations[rel_id] != NULL, "Error: filter requested on intermediate for non existing relation", return NULL; )
    // recreate intField to be filtered from rowids
    const unsigned int *fieldrowids = rowids[rel_id];
    intField *field = new intField[size];
    const Relation *OriginalR = originalRelations[rel_id];
    CHECK(OriginalR != NULL, "Warning: rel_id invalid or originalRelations map corrupted in IntermediateRelation::performFilter()", delete[] field; return NULL; )
    for (int i = 0 ; i < size ; i++){
        field[i] = OriginalR->getValueAt(col_id, fieldrowids[i] - 1);   // (!) -1 because rowids start at 1
    }
    // filter field
    unsigned int count = 0;
    bool *passing_rowids = QueryRelation::filterField(field, size, value, cmp, count);   // count will change accordingly
    delete[] field;
    // keep only passing rowids for all relations
    keepOnlyMarkedRows(passing_rowids, count);
    delete[] passing_rowids;
    return this;
}

IntermediateRelation *IntermediateRelation::performEqColumns(unsigned int rela_id, unsigned int relb_id, unsigned int cola_id, unsigned int colb_id) {
    if (size <= 0) return this;
    CHECK( rela_id < maplength && relb_id < maplength, "Invalid rela_id or relb_id: smaller than maplength = nrelations!", return NULL; )
    CHECK( rowids[rela_id] != NULL && rowids[relb_id] != NULL && originalRelations[rela_id] != NULL && originalRelations[relb_id] != NULL, "Error: equal columns requested on intermediate for non existing relation", return NULL; )
    // recreate intFields to be checked for equal values from rowids
    const unsigned int *fieldrowids_a = rowids[rela_id];
    const unsigned int *fieldrowids_b = rowids[relb_id];
    intField *field1 = new intField[size];
    intField *field2 = new intField[size];
    const Relation *OriginalRa = originalRelations[rela_id];
    const Relation *OriginalRb = originalRelations[relb_id];
    CHECK(OriginalRa != NULL && OriginalRb != NULL, "Warning: rela_id or relb_id invalid or originalRelations map corrupted in IntermediateRelation::performEqColumns()", delete[] field1; delete[] field2; return NULL; )
    for (int i = 0 ; i < size ; i++){
        field1[i] = OriginalRa->getValueAt(cola_id, fieldrowids_a[i] - 1);   // (!) -1 because rowids start at 1
        field2[i] = OriginalRb->getValueAt(colb_id, fieldrowids_b[i] - 1);   // ^^
    }
    // mark equal columns
    unsigned int count = 0;
    bool *passing_rowids = QueryRelation::eqColumnsFields(field1, field2, size, count);   // count will change accordingly
    delete[] field1;
    delete[] field2;
    // keep only passing rowids for all relations
    keepOnlyMarkedRows(passing_rowids, count);
    delete[] passing_rowids;
    return this;
}

IntermediateRelation *IntermediateRelation::performJoinWith(QueryRelation &B, unsigned int rela_id, unsigned int cola_id, unsigned int relb_id, unsigned int colb_id) {
    return (B.isIntermediate) ? performJoinWithIntermediate((IntermediateRelation &) B, rela_id, cola_id, relb_id, colb_id) : performJoinWithOriginal((Relation &) B, rela_id, cola_id, relb_id, colb_id);
}

IntermediateRelation *IntermediateRelation::performJoinWithOriginal(const Relation &B, unsigned int rela_id, unsigned int cola_id, unsigned int relb_id, unsigned int colb_id) {
    CHECK( rela_id < maplength && relb_id < maplength, "Invalid rela_id or relb_id: smaller than maplength = nrelations!", return NULL; )
    if (size <= 0) {
        numberOfRelations++;
        originalRelations[relb_id] = &B;   // insert new original relation's address (previously NULL)
        return this;
    } else if (B.getSize() <= 0){
        // clear the old map
        for (unsigned int i = 0 ; i < maplength ; i++){
            delete[] rowids[i];            // some might already be NULL
            rowids[i] = NULL;
        }
        // change variables accordingly
        size = 0;
        numberOfRelations++;
        originalRelations[relb_id] = &B;   // insert new original relation's address
    }
    // extract the correct join relations
    JoinRelation *JA = extractJoinRelation(rela_id, cola_id);
    JoinRelation *JB = B.extractJoinRelation(colb_id);

    // run RadixHashJoin
    Result *AxB = radixHashJoin(*JA, *JB);
    delete JA;
    delete JB;

    // get # of join tuples
    unsigned long long int number_of_tuples = AxB->getSize();

    if (number_of_tuples > 0) {
        // create new rowids map
        unsigned int **new_rowids = new unsigned int *[maplength];
        for (unsigned int i = 0 ; i < maplength ; i++){
            if (rowids[i] != NULL){
                new_rowids[i] = new unsigned int[number_of_tuples];
            } else {
                new_rowids[i] = NULL;
            }
        }
        CHECK( new_rowids[relb_id] == NULL, "Warning: join with rel_id that is already a part of intermediate?", delete[] new_rowids[relb_id]; )
        new_rowids[relb_id] = new unsigned int[number_of_tuples];

        // based on previous and RHJ's results
        Iterator I(AxB);
        unsigned int aid, bid, pos = 0;
        while (I.getNext(aid, bid)) {
            CHECK( aid > 0 && bid > 0, "WARNING: row id is zero in IntermediateRelation::performJoinWithOriginal()", )
            CHECK(pos < number_of_tuples, "Error: pos >= number_of_tuples", break; )
            CHECK(aid > 0 && aid <= size, "Error: aid out of bounds: aid = " + to_string(aid), break; )
            for (unsigned int i = 0 ; i < maplength ; i++) {
                if (rowids[i] != NULL) {
                    new_rowids[i][pos] = rowids[i][aid - 1];
                }
            }
            new_rowids[relb_id][pos] = bid;
            pos++; //current # of tuples
        }

        // clear the old map and replace it with the new
        for (unsigned int i = 0 ; i < maplength ; i++) {
            delete[] rowids[i];            // some might be NULL
            rowids[i] = new_rowids[i];
        }
        delete[] new_rowids;

        // change size accordingly
        size = number_of_tuples;

    } else {
        // clear the old map
        for (unsigned int i = 0 ; i < maplength ; i++){
            delete[] rowids[i];            // some might be NULL
            rowids[i] = NULL;
        }

        // change size accordingly
        size = 0;
    }
    numberOfRelations++;
    originalRelations[relb_id] = &B;   // insert new original relation's address
    delete AxB;
    return this;
}

IntermediateRelation *IntermediateRelation::performJoinWithIntermediate(IntermediateRelation &B, unsigned int rela_id, unsigned int cola_id, unsigned int relb_id, unsigned int colb_id) {
    CHECK( rela_id < maplength && relb_id < maplength, "Invalid rela_id or relb_id: smaller than maplength = nrelations!", return NULL; )
    if (size <= 0) {
        // clear the old map in B and add originalRelations in B to those in *this
        for (unsigned int i = 0 ; i < maplength ; i++){
            delete[] B.rowids[i];            // some might already be NULL
            B.rowids[i] = NULL;
            if ( B.originalRelations[i] != NULL ){
                CHECK(originalRelations[i] == NULL, "Intermediate join with zero-sized intermediate have the same rel_id!", )
                originalRelations[i] = B.originalRelations[i];
            }
        }
        // change variables accordingly
        B.size = 0;
        numberOfRelations += B.numberOfRelations;
        return this;
    } else if ( B.getSize() <= 0 ){
        // clear the old map and  add originalRelations in B to those in *this
        for (unsigned int i = 0 ; i < maplength ; i++) {
            delete[] rowids[i];    // some might be NULL
            rowids[i] = NULL;
            if (B.originalRelations[i] != NULL){
                originalRelations[i] = B.originalRelations[i];
            }
        }
        // change variables accordingly
        size = 0;
        numberOfRelations += B.numberOfRelations;
        return this;
    }

    // extract the correct join relations
    JoinRelation *JA = extractJoinRelation(rela_id, cola_id);
    JoinRelation *JB = B.extractJoinRelation(relb_id, colb_id);

    // run RadixHashJoin
    Result *AxB = radixHashJoin(*JA, *JB);
    delete JA;
    delete JB;

    // get # of join tuples
    unsigned long long int number_of_tuples = AxB->getSize();

    if (number_of_tuples > 0) {
        //create new rowids map
        unsigned int **new_rowids = new unsigned int *[maplength];
        for (unsigned int i = 0 ; i < maplength ; i++) {
            if (rowids[i] != NULL && B.rowids[i] != NULL) {
                cerr << "Different Intermediates with the same relation_id in them at join!" << endl;
                delete[] new_rowids;
                delete AxB;
                return NULL;
            } else if ( rowids[i] != NULL || B.rowids[i] != NULL ){
                new_rowids[i] = new unsigned int[number_of_tuples];
            } else {
                new_rowids[i] = NULL;
            }
        }

        // based on previous and RHJ's results
        Iterator I(AxB);
        unsigned int aid, bid, pos = 0;
        while (I.getNext(aid, bid)) {
            CHECK( aid > 0 && bid > 0, "WARNING: row id is zero in IntermediateRelation::performJoinWithIntermediate()", )
            for (unsigned int i = 0 ; i < maplength ; i++) {
                if ( rowids[i] != NULL ){
                    new_rowids[i][pos] = rowids[i][aid - 1];
                } else if ( B.rowids[i] != NULL ){
                    new_rowids[i][pos] = B.rowids[i][bid - 1];
                }
            }
            pos++; //current # of tuples
        }

        // clear the old map and replace it with the new
        for (unsigned int i = 0 ; i < maplength ; i++) {
            delete[] rowids[i];             // some might be NULL
            rowids[i] = new_rowids[i];
        }
        delete[] new_rowids;

        // change size accordingly
        size = number_of_tuples;
    } else {
        // clear the old map
        for (unsigned int i = 0 ; i < maplength ; i++){
            delete[] rowids[i];            // some might be NULL
            rowids[i] = NULL;
        }

        // change size accordingly
        size = 0;
    }
    // add originalRelations in B to those in *this
    for (unsigned int i = 0 ; i < maplength ; i++){
        if ( B.originalRelations[i] != NULL ){
            CHECK(originalRelations[i] == NULL, "Intermediate join intermediate have the same rel_id!", )
            originalRelations[i] = B.originalRelations[i];
        }
    }
    numberOfRelations += B.numberOfRelations;
    delete AxB;
    return this;
}

IntermediateRelation *IntermediateRelation::performCrossProductWith(QueryRelation &B) {
    return (B.isIntermediate) ? performCrossProductWithIntermediate((IntermediateRelation &) B) : performCrossProductWithOriginal((Relation &) B);
}

IntermediateRelation *IntermediateRelation::performCrossProductWithOriginal(const Relation &B) {
    CHECK( B.getId() < maplength, "Invalid B.getId(): smaller than maplength = nrelations!", return NULL; )
    if (size <= 0) {
        numberOfRelations++;
        originalRelations[B.getId()] = &B;   // insert new original relation's address
        return this;
    } else if (B.getSize() <= 0){
        // clear the old map
        for (unsigned int i = 0 ; i < maplength ; i++){
            delete[] rowids[i];            // some might already be NULL
            rowids[i] = NULL;
        }
        // change variables accordingly
        size = 0;
        numberOfRelations++;
        originalRelations[B.getId()] = &B;
        return this;
    }

    unsigned int sizeB = B.getSize();
    unsigned long long number_of_tuples = size * sizeB;

    if (number_of_tuples > 0) {
        // create new rowids map
        unsigned int **new_rowids = new unsigned int *[maplength];
        for (unsigned int i = 0 ; i < maplength ; i++){
            if (rowids[i] != NULL){
                new_rowids[i] = new unsigned int[number_of_tuples];
            } else {
                new_rowids[i] = NULL;
            }
        }
        CHECK( new_rowids[B.getId()] == NULL, "Warning: cross product with rel_id that is already a part of intermediate?", delete[] new_rowids[B.getId()]; )
        new_rowids[B.getId()] = new unsigned int[number_of_tuples];

        unsigned int pos = 0;
        // every row from A (this) is matched with every row from B
        for (unsigned int bi = 1; bi <= sizeB; bi++) {
            for (unsigned int ai = 0; ai < size; ai++, pos++) {
                for (unsigned int i = 0 ; i < maplength ; i++) {
                    if (rowids[i] != NULL) {
                        new_rowids[i][pos] = rowids[i][ai];
                    }
                }
                new_rowids[B.getId()][pos] = bi;
            }
        }

        // clear the old map and replace it with the new
        for (unsigned int i = 0 ; i < maplength ; i++) {
            delete[] rowids[i];            // some might be NULL
            rowids[i] = new_rowids[i];
        }
        delete[] new_rowids;

        // change size accordingly
        size = number_of_tuples;
    } else {
        // clear the old map
        for (unsigned int i = 0 ; i < maplength ; i++){
            delete[] rowids[i];            // some might be NULL
            rowids[i] = NULL;
        }

        // change size accordingly
        size = 0;
    }
    numberOfRelations++;
    originalRelations[B.getId()] = &B;
    return this;
}

IntermediateRelation *IntermediateRelation::performCrossProductWithIntermediate(IntermediateRelation &B) {
    if (size <= 0) {
        // clear the old map in B and add originalRelations in B to those in *this
        for (unsigned int i = 0 ; i < maplength ; i++){
            delete[] B.rowids[i];            // some might already be NULL
            B.rowids[i] = NULL;
            if ( B.originalRelations[i] != NULL ){
                CHECK(originalRelations[i] == NULL, "Intermediate join with zero-sized intermediate have the same rel_id!", )
                originalRelations[i] = B.originalRelations[i];
            }
        }
        // change variables accordingly
        B.size = 0;
        numberOfRelations += B.numberOfRelations;
        return this;
    } else if ( B.getSize() <= 0 ){
        // clear the old map and  add originalRelations in B to those in *this
        for (unsigned int i = 0 ; i < maplength ; i++) {
            delete[] rowids[i];    // some might be NULL
            rowids[i] = NULL;
            if (B.originalRelations[i] != NULL){
                originalRelations[i] = B.originalRelations[i];
            }
        }
        // change variables accordingly
        size = 0;
        numberOfRelations += B.numberOfRelations;
        return this;
    }

    unsigned int sizeB = B.getSize();
    unsigned long long number_of_tuples = size * sizeB;

    if (number_of_tuples > 0) {
        //create new rowids map
        unsigned int **new_rowids = new unsigned int *[maplength];
        for (unsigned int i = 0 ; i < maplength ; i++) {
            if (rowids[i] != NULL && B.rowids[i] != NULL) {
                cerr << "Different Intermediates with the same relation_id in them at join!" << endl;
                delete[] new_rowids;
                return NULL;
            } else if ( rowids[i] != NULL || B.rowids[i] != NULL ){
                new_rowids[i] = new unsigned int[number_of_tuples];
            } else {
                new_rowids[i] = NULL;
            }
        }

        unsigned int pos = 0;
        // every row from A (this) is matched with every row from B
        for (unsigned int bi = 0; bi < sizeB; bi++) {
            for (unsigned int ai = 0; ai < size; ai++, pos++) {
                for (unsigned int i = 0 ; i < maplength ; i++) {
                    if ( rowids[i] != NULL ){
                        new_rowids[i][pos] = rowids[i][ai];
                    } else if ( B.rowids[i] != NULL ){
                        new_rowids[i][pos] = B.rowids[i][bi];
                    }
                }
            }
        }

        // clear the old map and replace it with the new
        for (unsigned int i = 0 ; i < maplength ; i++) {
            delete[] rowids[i];             // some might be NULL
            rowids[i] = new_rowids[i];
        }
        delete[] new_rowids;

        // change size and numberOfRelations accordingly
        size = number_of_tuples;
    } else {
        // clear the old map
        for (unsigned int i = 0 ; i < maplength ; i++){
            delete[] rowids[i];            // some might be NULL
            rowids[i] = NULL;
        }

        // change size accordingly
        size = 0;
    }
    // add originalRelations in B to those in *this
    for (unsigned int i = 0 ; i < maplength ; i++){
        if ( B.originalRelations[i] != NULL ){
            CHECK(originalRelations[i] == NULL, "Intermediate join intermediate have the same rel_id!", )
            originalRelations[i] = B.originalRelations[i];
        }
    }
    numberOfRelations += B.numberOfRelations;
    return this;
}

void IntermediateRelation::performSelect(projection *projections, unsigned int nprojections) {
    if (size <= 0) {
        for (unsigned int j = 0 ; j < nprojections ; j++) {
            printf("NULL ");
        }
        printf("\n\n");
        return;
    }

    const Relation **OriginalRs = new const Relation *[nprojections];
    for (unsigned int j = 0 ; j < nprojections ; j++) {
        OriginalRs[j] = originalRelations[projections[j].rel_id];
        CHECK(OriginalRs[j] != NULL, "Warning: rel_id invalid or originalRelations map corrupted in IntermediateRelation::performSelect() ", break; )
    }

    for (unsigned int j = 0 ; j < nprojections ; j++){
        printf("%3d.%2d", projections[j].rel_id, projections[j].col_id);
    }
    printf("\n");
    for (unsigned int i = 0 ; i < size ; i++){
        for (unsigned int j = 0 ; j < nprojections ; j++){
            printf("%6lu", OriginalRs[j]->getValueAt(projections[j].col_id, rowids[projections[j].rel_id][i] - 1));   // (!) -1 because rowids start from 1
        }
        printf("\n");
    }
    printf("\n");

    delete[] OriginalRs;
}

void IntermediateRelation::performSum(projection *projections, unsigned int nprojections) {
    if (size == 0) {
        for (unsigned int k=0; k<nprojections-1; ++k) printf("NULL ");
        printf("NULL\n");
        return;
    }

    intField *sum = new intField[nprojections]();

    for (unsigned int j = 0; j < nprojections; ++j) {
        const Relation *OriginalR = originalRelations[projections[j].rel_id];
        CHECK(OriginalR != NULL, "Warning: rel_id invalid or originalRelations map corrupted in IntermediateRelation::performSum() ", break; )
        for (unsigned int i = 0; i < size; ++i) {
            sum[j] += OriginalR->getValueAt(projections[j].col_id, rowids[projections[j].rel_id][i] - 1);
        }
    }

    for (unsigned int k = 0; k < nprojections - 1 ; ++k) printf("%lu ", sum[k]);
    printf("%lu\n", sum[nprojections - 1]);

    delete[] sum;
}

void IntermediateRelation::keepOnlyMarkedRows(const bool *passing_rowids, unsigned int count) {
    if (count > 0) {
        for (unsigned int i = 0 ; i < maplength ; i++) {
            if (rowids[i] != NULL){
                unsigned int *rids = rowids[i];
                unsigned int *newrowids = new unsigned int[count];
                unsigned int j = 0;
                for (unsigned int i = 0; i < size; i++) {
                    if (passing_rowids[i]) {
                        CHECK( j < count, "Warning: miscounted passing rowids in IntermediateRelation::keepOnlyMarkedRows()", break; )
                        newrowids[j++] = rids[i];
                    }
                }
                delete[] rids;
                rowids[i] = newrowids;
            }
        }
        size = count;                      // (!) size must now change to count
    } else {
        for (unsigned int i = 0 ; i < maplength ; i++){
            delete[] rowids[i];            // some might be NULL
            rowids[i] = NULL;
        }
        size = 0;
    }
}

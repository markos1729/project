#include <iostream>
#include <cmath>
#include <unordered_map>
#include <cstdio>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include "../Headers/Relation.h"
#include "../Headers/RadixHashJoin.h"


using namespace std;


extern Relation **R;
extern int Rlen;


/* H1 Function used in partitioning*/
unsigned int H1(intField value, unsigned int n){
    intField mask = 0;
    for (unsigned int i = 0 ; i < n ; i++ ){
        mask |= 0x01<<i;
    }
    return (unsigned int) (mask & value);
}


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

// phase 1: partition in place JoinRelation R into buckets and fill Psum to distinguish them (|Psum| = num_of_buckets = 2^H1_N)
bool JoinRelation::partitionRelation(unsigned int H1_N) {
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
    // 3) create new re-ordered versions for joinField and temporary_code_dump based on their bucket_nums - O(n)
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
    // 4) overwrite joinField and temporary_code_dump with new re-ordered versions of it
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
    printf(" joinField | temporary_code_dump\n");
    for (unsigned int i = 0 ; i < size ; i++){
        printf("%10u | %u\n", (unsigned int) joinField[i], temporary_code_dump[i]);
    }
}
#endif


/* QueryRelation Implementation */
bool *QueryRelation::filterField(intField *field, unsigned int size, intField value, char cmp, unsigned int &count){
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
}

bool *QueryRelation::eqColumnsFields(intField *field1, intField *field2, unsigned int size, unsigned int &count) {
    count = 0;
    bool *eqColumns = new bool[size]();
    for (unsigned int i = 0 ; i < size ; i++){
        eqColumns[i] = (field1[i] == field2[i]);
        if (eqColumns[i]) count++;
    }
    return eqColumns;
}


/* Relation Implementation */
Relation::Relation(unsigned int _size, unsigned int _num_of_columns) : QueryRelation(false), allocatedWithMmap(false), id(-1), size(_size), num_of_columns(_num_of_columns) {
    columns = new intField*[_num_of_columns]();   // initialize to NULL
}

Relation::Relation(const char* file) : QueryRelation(false), allocatedWithMmap(true), id(-1) {
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

JoinRelation *Relation::extractJoinRelation(unsigned int index_of_JoinField) {
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
    unsigned int *newrowids = NULL;
    if (count > 0) {
        newrowids = new unsigned int[count];
        unsigned int j = 0;
        for (unsigned int i = 0; i < size; i++) {
            if (passing_rowids[i]) {
                if (j >= count) {
                    cerr << "Warning: miscounted passing temporary_code_dump? : count  = " << count << endl;
                    break;
                }
                newrowids[j++] = i + 1;   // keep rowid for intermediate, not the intField columns[cold_id][i].
            }
        }
    }
    delete[] passing_rowids;
    IntermediateRelation *result = new IntermediateRelation(this->id, newrowids, count);
    delete[] newrowids;
    return result;
}

IntermediateRelation *Relation::performEqColumns(unsigned int rel_id, unsigned int cola_id, unsigned int colb_id) {
    // Note: rel_id is not needed here as there is only one relation to perform eq columns on
    unsigned int count = 0;
    bool *passing_rowids = QueryRelation::eqColumnsFields(columns[cola_id], columns[colb_id], size, count);   // count will change accordingly
    unsigned int *newrowids = NULL;
    if (count > 0) {
        newrowids = new unsigned int[count];
        unsigned int j = 0;
        for (unsigned int i = 0; i < size; i++) {
            if (passing_rowids[i]) {
                if (j >= count) {
                    cerr << "Warning: miscounted passing temporary_code_dump? : count  = " << count << endl;
                    break;
                }
                newrowids[j++] = i + 1;   // keep rowid for intermediate, not the intField columns[cold_id][i].
            }
        }
    }
    delete[] passing_rowids;
    IntermediateRelation *result = new IntermediateRelation(this->id, newrowids, count);
    delete[] newrowids;
    return result;
}

IntermediateRelation *Relation::performJoinWith(QueryRelation &B, unsigned int rela_id, unsigned int cola_id, unsigned int relb_id, unsigned int colb_id) {
    return (B.isIntermediate) ? performJoinWithIntermediate((IntermediateRelation &) B, rela_id, cola_id, relb_id, colb_id) : performJoinWithOriginal((Relation &) B, rela_id, cola_id, relb_id, colb_id);
}

IntermediateRelation *Relation::performJoinWithOriginal(const Relation &B, unsigned int rela_id, unsigned int cola_id, unsigned int relb_id, unsigned int colb_id) {
    //TODO:
    // extract the correct join relations from Relation
    // RHJ join relations
    // convert Result to two columns of unsigned int temporary_code_dump
    // create and return new IntermediateRelation using the second constructor
    return nullptr;  // new IntermediateRelation(...);
}

IntermediateRelation *Relation::performJoinWithIntermediate(IntermediateRelation &B, unsigned int rela_id, unsigned int cola_id, unsigned int relb_id, unsigned int colb_id) {
    // TODO:
    // extract join relation from this Relation and IntermediateRelation &B
    // RHJ join relations
    // change IntermediateRelation B to represent the join's result (do NOT create new IntermediateRelation)
    // return the address of (now changed) B
    return &B;
}

IntermediateRelation *Relation::performCrossProductWith(QueryRelation &B) {
    // TODO
    return nullptr;
}

void Relation::performSelect(projection *projections, unsigned int nprojections) {
    for (unsigned int j = 0 ; j < nprojections ; j++){
        printf("%3d.%2d", projections[j].rel_id, projections[j].col_id);
    }
    printf("\n");
    for (unsigned int i = 0 ; i < size ; i++){
        for (unsigned int j = 0 ; j < nprojections ; j++){
            printf("%6lu", this->getValueAt(projections[i].col_id, i));
        }
        printf("\n");
    }
}


/* IntermediateRelation Implementation */
IntermediateRelation::IntermediateRelation(unsigned int rel_id, unsigned int *_rowids, unsigned int _size) : QueryRelation(true), numberOfRelations(1), size(_size) {
    if (size > 0) {
        unsigned int *column = new unsigned int[size];
        for (unsigned int i = 0; i < size; i++) {
            column[i] = _rowids[i];
        }
        rowids.insert(make_pair(rel_id, column));
    } else {
        rowids.insert(make_pair(rel_id, (unsigned int *) NULL));
    }
}

IntermediateRelation::IntermediateRelation(unsigned int rela_id, unsigned int relb_id, unsigned int *_rowids_a, unsigned int *_rowids_b, unsigned int _size) : QueryRelation(true), numberOfRelations(2), size(_size) {
    if (size > 0) {
        unsigned int *column_a = new unsigned int[size];
        unsigned int *column_b = new unsigned int[size];
        for (unsigned int i = 0; i < size; i++) {
            column_a[i] = _rowids_a[i];
            column_b[i] = _rowids_b[i];
        }
        rowids.insert(make_pair(rela_id, column_a));
        rowids.insert(make_pair(relb_id, column_b));
    } else {
        rowids.insert(make_pair(rela_id, (unsigned int *) NULL));
        rowids.insert(make_pair(relb_id, (unsigned int *) NULL));
    }
}

IntermediateRelation::~IntermediateRelation() {
    if (size > 0) {
        for (auto iter = rowids.begin(); iter != rowids.end(); iter++) {
            delete[] iter->second;   // get unsigned int * from <unsigned int, unsigned int *> pair (!) if is necessary!
        }
    }
}


JoinRelation *IntermediateRelation::extractJoinRelation(unsigned int number_of_relation, const Relation &R, unsigned int index_of_JoinField) {
    // TODO
    return nullptr;
}

IntermediateRelation *IntermediateRelation::performFilter(unsigned int rel_id, unsigned int col_id, intField value, char cmp) {
    if (size <= 0) return this;
    if ( rowids.find(rel_id) == rowids.end() ){   // non existant relation in intermediate
        cerr << "Fatal error: filter requested on intermediate for non existing relation" << endl;
        return NULL;
    }
    // recreate intField to be filtered from temporary_code_dump
    const unsigned int *fieldrowids = rowids[rel_id];
    intField *field = new intField[size];
    for (int i = 0 ; i < size ; i++){
        //DEBUG Note: R[rel_id] is NOT the correct relation as rel_ids are only for the 'FROM' tables. We have to calculate it by searching or keeping info on it (TODO?)
        int rel_pos_in_R = find_pos_in_R(rel_id);
        if (rel_pos_in_R == -1) { cerr << "Warning: rel_id or Rlen invalid in performFilter for intermediate" << endl; delete[] field; return NULL; }
        field[i] = R[rel_pos_in_R]->getValueAt(col_id, fieldrowids[i] - 1);   // (!) -1 because temporary_code_dump start at 1
    }
    // filter field
    unsigned int count = 0;
    bool *passing_rowids = QueryRelation::filterField(field, size, value, cmp, count);   // count will change accordingly
    delete[] field;
    // keep only passing temporary_code_dump for all relations
    keepOnlyMarkedRows(passing_rowids, count);
    delete[] passing_rowids;
    return this;
}

IntermediateRelation *IntermediateRelation::performEqColumns(unsigned int rel_id, unsigned int cola_id, unsigned int colb_id) {
    if (size <= 0) return this;
    if ( rowids.find(rel_id) == rowids.end() ){   // non existant relation in intermediate
        cerr << "Fatal error: filter requested on intermediate for non existing relation" << endl;
        return NULL;
    }
    // recreate intFields to be checked for equal values from temporary_code_dump
    const unsigned int *fieldrowids = rowids[rel_id];
    intField *field1 = new intField[size];
    intField *field2 = new intField[size];
    for (int i = 0 ; i < size ; i++){
        //DEBUG Note: R[rel_id] is NOT the correct relation as rel_ids are only for the 'FROM' tables. We have to calculate it by searching or keeping info on it (TODO?)
        int rel_pos_in_R = find_pos_in_R(rel_id);
        if (rel_pos_in_R == -1) { cerr << "Warning: rel_id or Rlen invalid in performFilter for intermediate" << endl; delete[] field1; delete[] field2; return NULL; }
        field1[i] = R[rel_pos_in_R]->getValueAt(cola_id, fieldrowids[i] - 1);   // (!) -1 because temporary_code_dump start at 1
        field2[i] = R[rel_pos_in_R]->getValueAt(colb_id, fieldrowids[i] - 1);   // ^^
    }
    // mark equal columns
    unsigned int count = 0;
    bool *passing_rowids = QueryRelation::eqColumnsFields(field1, field2, size, count);   // count will change accordingly
    delete[] field1;
    delete[] field2;
    // keep only passing temporary_code_dump for all relations
    keepOnlyMarkedRows(passing_rowids, count);
    delete[] passing_rowids;
    return this;
}

IntermediateRelation *IntermediateRelation::performJoinWith(QueryRelation &B, unsigned int rela_id, unsigned int cola_id, unsigned int relb_id, unsigned int colb_id) {
    if (size <= 0) return this;
    return (B.isIntermediate) ? performJoinWithIntermediate((IntermediateRelation &) B, rela_id, cola_id, relb_id, colb_id) : performJoinWithOriginal((Relation &) B, rela_id, cola_id, relb_id, colb_id);
}

IntermediateRelation *IntermediateRelation::performJoinWithOriginal(const Relation &B, unsigned int rela_id, unsigned int cola_id, unsigned int relb_id, unsigned int colb_id) {
    // TODO:
    // extract join relation from this IntermediateRelation and Relation &B
    // RHJ join relations
    // change this IntermediateRelation to represent the join's result (do NOT create new IntermediateRelation)
    // return the address of (now changed) this
    return this;
}

IntermediateRelation *IntermediateRelation::performJoinWithIntermediate(IntermediateRelation &B, unsigned int rela_id, unsigned int cola_id, unsigned int relb_id, unsigned int colb_id) {
    // TODO:
    // extract join relation from both IntermediateRelations (this and B)
    // RHJ join relations
    // change this IntermediateRelation to reporesent the join's result (do NOT create new IntermediateRelation)
    // (!) do NOT delete other IntermediateRelation &B as joiner-main already does so
    return this;
}

IntermediateRelation *IntermediateRelation::performCrossProductWith(QueryRelation &B) {
    if (size <= 0) return this;
    // TODO
    return nullptr;
}

void IntermediateRelation::performSelect(projection *projections, unsigned int nprojections) {
    if (size <= 0) return;
    unsigned int **allrowids = new unsigned int*[numberOfRelations];
    {   int i = 0;
        for (auto iter = rowids.begin() ; iter != rowids.end() ; iter++) {
            if ( i >= numberOfRelations ) { cerr << "Warning: Miscalculated numberOfRelations?" << endl; break; }
            allrowids[i++] = iter->second;
        }
    }
    for (unsigned int j = 0 ; j < nprojections ; j++){
        printf("%3d.%2d", projections[j].rel_id, projections[j].col_id);
    }
    printf("\n");
    for (unsigned int i = 0 ; i < size ; i++){
        for (unsigned int j = 0 ; j < nprojections ; j++){
            //DEBUG Note: R[rel_id] is NOT the correct relation as rel_ids are only for the 'FROM' tables. We have to calculate it by searching or keeping info on it (TODO?)
            int rel_pos_in_R = find_pos_in_R(projections[j].rel_id);
            if (rel_pos_in_R == -1) { cerr << "Warning: rel_id or Rlen invalid in performSelect for intermediate" << endl; }
            else printf("%6lu", R[rel_pos_in_R]->getValueAt(projections[j].col_id, allrowids[projections[j].rel_id][i] - 1));   // (!) -1 because temporary_code_dump start from 1
        }
        printf("\n");
    }
    delete[] allrowids;
}

int IntermediateRelation::find_pos_in_R(unsigned int rel_id) const {
    //TODO: Change this so it's not a linear search? Maybe keep track of each rel_id->pos_in_R during the query?
    int rel_pos_in_R = -1;
    for (int i = 0 ; i < Rlen ; i++){
        if (R[i]->getId() == rel_id){
            rel_pos_in_R = i;
            break;
        }
    }
    return rel_pos_in_R;
}

void IntermediateRelation::keepOnlyMarkedRows(const bool *passing_rowids, unsigned int count) {
    if (count > 0) {
        for (auto iter = rowids.begin(); iter != rowids.end(); iter++) {
            unsigned int *rids = iter->second;
            unsigned int *newrowids = new unsigned int[count];
            unsigned int j = 0;
            for (unsigned int i = 0; i < size; i++) {
                if (passing_rowids[i]) {
                    if (j >= count) {
                        cerr << "Warning: miscounted passing temporary_code_dump in intermediate? count = " << count << endl;
                        break;
                    }
                    newrowids[j++] = rids[i];
                }
            }
            delete[] rids;
            iter->second = newrowids;
        }
        size = count;                      // (!) size must now change to count
    } else {
        for (auto iter = rowids.begin(); iter != rowids.end(); iter++) {
            delete[] iter->second;
            iter->second = NULL;
        }
        size = 0;
    }
}

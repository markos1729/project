#include <cmath>
#include "stdint.h"
#include "../Headers/Relation.h"


unsigned int H1(intField value){
    intField mask = 0;
    for (int i = 0 ; i < H1_N ; i++ ){
        mask |= 0x01<<i;
    }
    return (unsigned int) (mask & value);
}


Relation::Relation(unsigned int _size, intField *_joinField, unsigned int *_rowids) : size(_size), joinField(_joinField), rowids(_rowids), Psum(NULL) {}

Relation::~Relation() {
    if (Psum != NULL) delete[] Psum;
    if (joinField != NULL) delete[] joinField;
    if (rowids != NULL) delete[] rowids;
}

// phase 1: partition in place Relation R into buckets and fill Psum to distinguish them (|Psum| = 2^n)
bool Relation::partitionRelation() {
    const unsigned int num_of_buckets = pow(2, H1_N);
    // 1) calculate Hist (in linear time)
    unsigned int *Hist = new unsigned int[num_of_buckets];
    for (unsigned int i = 0 ; i < num_of_buckets ; i++){ Hist[i] = 0; }
    unsigned int *bucket_nums = new unsigned int[size];
    for (unsigned int i = 0 ; i < size ; i++){
        bucket_nums[i] = H1(joinField[i]);
        if ( bucket_nums[i] < 0 || bucket_nums[i] >= num_of_buckets ){ delete[] Hist; delete[] bucket_nums; return false; }  // ERROR CHECK
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
    // 3) create new re-ordered versions for joinField and rowids based on their bucket_nums (in linear time)
    intField *newJoinField = new intField[size];
    unsigned int *newRowids = new unsigned int[size];
    for (unsigned int i = 0 ; i < size ; i++) { newJoinField[i] = 0; newRowids[i] = 0; }
    unsigned int *nextBucketPos = new unsigned int[num_of_buckets];
    for (unsigned int i = 0 ; i < num_of_buckets ; i++){ nextBucketPos[i] = 0; }
    for (unsigned int i = 0 ; i < size ; i++) {
        const int pos = Psum[bucket_nums[i]] + nextBucketPos[bucket_nums[i]];   // value's position in the re-ordered version
        if ( pos < 0 || pos >= size ) { delete[] newJoinField; delete[] newRowids; delete[] bucket_nums; delete[] nextBucketPos;  return false; }  // ERROR CHECK
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

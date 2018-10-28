#include <cmath>
#include <cstdio>
#include "stdint.h"
#include "../Headers/Relation.h"


unsigned int H1(intField value, unsigned int n){
    intField mask = 0;
    for (int i = 0 ; i < n ; i++ ){
        mask |= 0x01<<i;
    }
    return (unsigned int) (mask & value);
}


Relation::Relation(unsigned int _size, const intField *_joinField, const unsigned int *_rowids) : size(_size), joinField(NULL), rowids(NULL), Psum(NULL), numberOfBuckets(0) {
    joinField = new intField[size];
    rowids = new unsigned int[size];
    for (unsigned int i = 0; i < size; i++) {
        joinField[i] = _joinField[i];
        rowids[i] = _rowids[i];
    }
}

Relation::~Relation() {
    delete[] Psum;
    delete[] joinField;
    delete[] rowids;
}

// phase 1: partition in place Relation R into buckets and fill Psum to distinguish them (|Psum| = 2^n)
bool Relation::partitionRelation(unsigned int avg_bucket_size, unsigned int forced_H1_N) {
    const unsigned int H1_N = (forced_H1_N == 0) ? pickH1_N(avg_bucket_size) : forced_H1_N;
    const unsigned int num_of_buckets = (unsigned int) pow(2, H1_N);
    // 1) calculate Hist (in linear time)
    unsigned int *Hist = new unsigned int[num_of_buckets]();    // all Hist[i] are initialized to 0
    unsigned int *bucket_nums = new unsigned int[size];
    for (unsigned int i = 0 ; i < size ; i++){
        bucket_nums[i] = H1(joinField[i], H1_N);
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
    numberOfBuckets = num_of_buckets;
    // 3) create new re-ordered versions for joinField and rowids based on their bucket_nums (in linear time)
    intField *newJoinField = new intField[size]();
    unsigned int *newRowids = new unsigned int[size]();
    unsigned int *nextBucketPos = new unsigned int[num_of_buckets]();
    for (unsigned int i = 0 ; i < size ; i++) {
        const int pos = Psum[bucket_nums[i]] + nextBucketPos[bucket_nums[i]];   // value's position in the re-ordered version
        if ( pos < 0 || pos >= size ) { delete[] newJoinField; delete[] newRowids; delete[] bucket_nums; delete[] nextBucketPos; Psum = NULL; numberOfBuckets = 0;  return false; }  // ERROR CHECK
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

// pick a good N value for H1 hash function
unsigned int Relation::pickH1_N(unsigned int avg_bucket_size) {       //TODO: make a better scheme?
    return (unsigned int) log2( ((double) size) / avg_bucket_size );
}

// DEBUG
void Relation::printDebugInfo() {
    if (Psum != NULL) {
        printf("This Relation is partitioned.\n%u buckets created with Psum as follows:\n", numberOfBuckets);
        for (unsigned int i = 0; i < numberOfBuckets; i++) {
            printf("Psum[%u] = %u\n", i, Psum[i]);
        }
    }
    printf(" joinField | rowids\n");
    for (unsigned int i = 0 ; i < size ; i++){
        printf("%10u | %u\n", (unsigned int) joinField[i], rowids[i]);
    }
}



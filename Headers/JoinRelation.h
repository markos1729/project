#ifndef PROJECT_JOINRELATION_H
#define PROJECT_JOINRELATION_H

#include <iostream>
#include "FieldTypes.h"


class JoinRelation {     // Relation struct used for RadixHashJoin, only stores Join Field and rowids
private:
    unsigned int size;   // number of tuples
    intField *joinField;
    unsigned int *rowids;
    unsigned int *Psum;
    unsigned int numberOfBuckets;
public:
    JoinRelation(unsigned int _size, const intField *_joinField, const unsigned int *_rowids);
    ~JoinRelation();
    /* Accessors  */
    unsigned int getSize() const { return size; }
    intField getJoinField(unsigned int pos) const { return (rowids != NULL && pos < size) ? joinField[pos] : 0; }
    unsigned int getRowId(unsigned pos) const { return (rowids != NULL && pos < size) ? rowids[pos] : 0; }
    unsigned int getNumberOfBuckets() const { return numberOfBuckets; }
    unsigned int *getRowIdsBucket(unsigned int i) const { return (Psum != NULL && i < numberOfBuckets) ? rowids + Psum[i] : 0; }
    intField *getJoinFieldBucket(unsigned int i) const { return (Psum != NULL && i < numberOfBuckets) ? joinField + Psum[i] : 0; }
    unsigned int getBucketSize(unsigned int i) const { return (Psum != NULL && i < numberOfBuckets) ? ((i == numberOfBuckets - 1) ? size - Psum[i] : Psum[i + 1] - Psum[i]) : 0; }
    /* Operations */
    bool partitionRelation(unsigned int H1_N);     // partitions JoinRelation by creating Psum and reordering it's tuples
    bool partitionRelationSequentially(unsigned int H1_N);
#ifdef DDEBUG
    void printDebugInfo();
#endif
};

#endif

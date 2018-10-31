#ifndef RELATION_H
#define RELATION_H

#include "FieldTypes.h"

#define L1 4096  //32KB
#define L2 32768 //256KB

unsigned int H1(intField, unsigned int N);

class Relation {
	private:
		unsigned int size;
        intField *joinField;
        unsigned int *rowids;
        unsigned int *Psum;
        unsigned int numberOfBuckets;
    public:
		Relation(unsigned int _size, const intField *_joinField, const unsigned int *_rowids);
		~Relation();
		/* Accessors  */
        unsigned int getSize() const { return size; }
        unsigned int getNumberOfBuckets() const { return numberOfBuckets; }
        unsigned int *getRowIds(int i) const { return rowids + Psum[i]; }
        intField *getJoinField(int i) const { return joinField + Psum[i]; }
        unsigned int getBucketSize(int i) const { return (i == numberOfBuckets - 1) ? size - Psum[i] : Psum[i + 1] - Psum[i]; }
        /* Operations */
        bool partitionRelation(unsigned int H1_N);     // partitions Relation by creating Psum and reordering it's tuples
        /* Debug */
        void printDebugInfo();
};

#endif

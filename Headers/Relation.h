#ifndef RELATION_H
#define RELATION_H

#include "FieldTypes.h"

#define CPU_CACHE 4096
#define H2SIZE 3

unsigned int H1(intField, unsigned int N);

class Relation {
	private:
		unsigned int size;
        intField *joinField;
        unsigned int *rowids;
        unsigned int *Psum;
        unsigned int numberOfBuckets;
    public:
		Relation(unsigned int _size, intField *_joinField, unsigned int *_rowids);
		~Relation();
		/* Accessors  */
        unsigned int getBuckets() const { return numberOfBuckets; }
        unsigned int *getIds(int i) const { return rowids+Psum[i]; }
        intField *getField(int i) const { return joinField+Psum[i]; }
        unsigned int getSize(int i) const { return i==numberOfBuckets-1 ? size-Psum[i] : Psum[i+1]-Psum[i]; }
        /* Operations */
		bool partitionRelation();   // partitions Relation by creating Psum and reordering it's tuples
        /* Debug */
        void printDebugInfo();
    private:
        unsigned int pickH1_N();
};

#endif

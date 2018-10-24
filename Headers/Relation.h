#ifndef RELATION_H
#define RELATION_H

#include "FieldTypes.h"

#define CPU_CACHE 4096


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
        unsigned int *getPsum(unsigned int &size) const { size = numberOfBuckets; return Psum; }
        /* Operations */
		bool partitionRelation();   // partitions Relation by creating Psum and reordering it's tuples
        /* Debug */
        void printDebugInfo();
    private:
        unsigned int pickH1_N();
};

#endif

#ifndef RELATION_H
#define RELATION_H

#include "FieldTypes.h"

#define H1_N 10        // number of bits for hash function H1


unsigned int H1(intField);


class Relation {
	private:
		unsigned int size;
        intField *joinField;
        unsigned int *rowids;
        unsigned int *Psum;
public:
		Relation(unsigned int _size, intField *_joinField, unsigned int *_rowids);
		~Relation();
		/* Accessors  */
        unsigned int *getPsum() const { return Psum; }
        /* Operations */
		bool partitionRelation();   // partitions Relation by creating Psum and reordering it's tuples
};

#endif

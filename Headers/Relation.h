#ifndef RELATION_H
#define RELATION_H

#include "FieldTypes.h"

class Relation {
	private:
		int size;
        intField *joinField;
        unsigned int *rowids;
	public:
		Relation(int _size, intField *_joinField, unsigned int *_rowids);
		~Relation();
};

#endif

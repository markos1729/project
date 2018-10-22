#ifndef RELATION_H
#define RELATION_H

#include <cstdint>

class Relation {
	private:
		int size;
		uint32_t *joinField, *rowids;
	public:
		Relation(int _size, uint32_t *_joinField, uint32_t *_rowids);
		~Relation();
};

#endif

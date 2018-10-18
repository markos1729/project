#include "stdint.h"
#include "relation.h"

relation::relation(int _size,uint32_t *_column) : size(_size), column(_column) {};

relation::~relation() {}

void relation::build(uint32_t _H1,uint32_t _H2) {
	H1=_H1;
	H2=_H2;
}

#include "stdint.h"
#include "../Headers/Relation.h"

Relation::Relation(int _size, intField *_joinField, unsigned int *_rowids) : size(_size), joinField(_joinField), rowids(_rowids) {}

Relation::~Relation() {}

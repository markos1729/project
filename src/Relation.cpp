#include "stdint.h"
#include "../Headers/Relation.h"

Relation::Relation(int _size, uint32_t *_joinField, uint32_t *_rowids) : size(_size), joinField(_joinField), rowids(_rowids) {}

Relation::~Relation() {}

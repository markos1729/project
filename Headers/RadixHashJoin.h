#ifndef JOIN_H
#define JOIN_H

#include "Relation.h"
#include "JoinResults.h"

Result *radixHashJoin(Relation &R, Relation &S);

#endif

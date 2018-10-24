#ifndef JOIN_H
#define JOIN_H

#include "Relation.h"
#include "JoinResults.h"

Result *radixHashJoin(Relation &R, Relation &S);
bool indexRelation(intField *bucketJoinField,unsigned int bucketSize,int *&chain,int *&table);

#endif

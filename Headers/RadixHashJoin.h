#ifndef RADIXHASHJOIN_H
#define RADIXHASHJOIN_H

#include "Relation.h"
#include "JoinResults.h"

Result *radixHashJoin(Relation &R, Relation &S);
bool indexRelation(intField *bucketJoinField,unsigned int bucketSize,int *&chain,int *&table);
bool probeResults(intField *LbucketJoinField, unsigned int *LbucketRowIds, intField *IbucketJoinField, unsigned int *IbucketRowIds,int *&chain,int *&H2HashTable,unsigned int bucketSize, Result *result);

#endif

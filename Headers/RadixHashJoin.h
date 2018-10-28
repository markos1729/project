#ifndef RADIXHASHJOIN_H
#define RADIXHASHJOIN_H

#include "Relation.h"
#include "JoinResults.h"

Result *radixHashJoin(Relation &R, Relation &S);
bool indexRelation(intField *bucketJoinField, unsigned int bucketSize, unsigned int *&chain, unsigned int *&table);
bool probeResults(const intField *LbucketJoinField, const unsigned int *LbucketRowIds, const intField *IbucketJoinField, const unsigned int *IbucketRowIds, const unsigned int *chain, const unsigned int *H2HashTable, unsigned int bucketSize, Result *result, bool saveLfirst);

#endif

#ifndef RADIXHASHJOIN_H
#define RADIXHASHJOIN_H

#include "Relation.h"
#include "JoinResults.h"

void setH(unsigned int _H1_N,unsigned int _H2_N);
Result *radixHashJoin(JoinRelation &R, JoinRelation &S);
bool indexRelation(intField *bucketJoinField, unsigned int bucketSize, unsigned int *&chain, unsigned int *&table);
bool probeResults(const intField *LbucketJoinField, const unsigned int *LbucketRowIds, const intField *IbucketJoinField, const unsigned int *IbucketRowIds, const unsigned int *chain, const unsigned int *H2HashTable, unsigned int bucketSize, Result *result, bool saveLfirst);

#endif

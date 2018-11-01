#ifndef RELATION_H
#define RELATION_H

#include <iostream>
#include "FieldTypes.h"

#define CACHE 4096  //32KB

unsigned int H1(intField, unsigned int N);

class JoinRelation {    // Relation struct used for RadixHashJoin, only stores Join Field and rowids
private:
	unsigned int size;  // number of tuples
	intField *joinField;
	unsigned int *rowids;
	unsigned int *Psum;
	unsigned int numberOfBuckets;
public:
	JoinRelation(unsigned int _size, const intField *_joinField, const unsigned int *_rowids);
	~JoinRelation();
	/* Accessors  */
	unsigned int getSize() const { return size; }
	intField getJoinField(int pos) const { return (rowids != NULL && pos >= 0 && pos < size) ? joinField[pos] : 0; }
	unsigned int getRowId(int pos) const { return (rowids != NULL && pos >= 0 && pos < size) ? rowids[pos] : 0; }
	unsigned int getNumberOfBuckets() const { return numberOfBuckets; }
	unsigned int *getRowIdsBucket(int i) const { return (Psum != NULL && i >= 0 && i < numberOfBuckets) ? rowids + Psum[i] : 0; }
	intField *getJoinFieldBucket(int i) const { return (Psum != NULL && i >= 0 && i < numberOfBuckets) ? joinField + Psum[i] : 0; }
	unsigned int getBucketSize(int i) const { return (Psum != NULL &&  i >= 0 && i < numberOfBuckets) ? ((i == numberOfBuckets - 1) ? size - Psum[i] : Psum[i + 1] - Psum[i]) : 0; }
	/* Operations */
	bool partitionRelation(unsigned int H1_N);     // partitions JoinRelation by creating Psum and reordering it's tuples
	/* Debug */
	void printDebugInfo();
};


class Relation {        // Relation struct storing all fields for a relation
private:
    unsigned int size;  // number of tuples
    unsigned int num_of_columns;
    intField **columns; // each column saved as a sequential array of intFields
public:
    Relation(unsigned int _size, unsigned int _num_of_columns);
    Relation(const char* file);
    ~Relation();
    bool addColumn(unsigned int col_num, const intField *values);   // NOTE: use addColumns to build Relation column-by-column, (!) values lenth must be == size
    JoinRelation *extractJoinRelation(unsigned int index_of_JoinField);
};

#endif

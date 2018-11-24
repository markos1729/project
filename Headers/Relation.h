#ifndef RELATION_H
#define RELATION_H

#include <iostream>
#include "JoinResults.h"
#include "FieldTypes.h"

//#define DDEBUG           // define this if functions used for debugging such as printing info should be compiled

#define CACHE 4096       // 32KB
#define MAX_JOIN_RELATIONS 32

unsigned int H1(intField, unsigned int N);


class JoinRelation {     // Relation struct used for RadixHashJoin, only stores Join Field and rowids
private:
	unsigned int size;   // number of tuples
	intField *joinField;
	unsigned int *rowids;
	unsigned int *Psum;
	unsigned int numberOfBuckets;
public:
	JoinRelation(unsigned int _size, const intField *_joinField, const unsigned int *_rowids);
	~JoinRelation();
	/* Accessors  */
	unsigned int getSize() const { return size; }
	intField getJoinField(unsigned int pos) const { return (rowids != NULL && pos < size) ? joinField[pos] : 0; }
	unsigned int getRowId(unsigned pos) const { return (rowids != NULL && pos < size) ? rowids[pos] : 0; }
	unsigned int getNumberOfBuckets() const { return numberOfBuckets; }
	unsigned int *getRowIdsBucket(unsigned int i) const { return (Psum != NULL && i < numberOfBuckets) ? rowids + Psum[i] : 0; }
	intField *getJoinFieldBucket(unsigned int i) const { return (Psum != NULL && i < numberOfBuckets) ? joinField + Psum[i] : 0; }
	unsigned int getBucketSize(unsigned int i) const { return (Psum != NULL && i < numberOfBuckets) ? ((i == numberOfBuckets - 1) ? size - Psum[i] : Psum[i + 1] - Psum[i]) : 0; }
	/* Operations */
	bool partitionRelation(unsigned int H1_N);     // partitions JoinRelation by creating Psum and reordering it's tuples
	#ifdef DDEBUG
	void printDebugInfo();
	#endif
};


class Relation {        // Relation struct storing all fields for a relation
private:
	const bool allocatedWithMmap;
    unsigned int size;  // number of tuples
    unsigned int num_of_columns;
    intField **columns; // each column saved as a sequential array of intFields
public:
    Relation(unsigned int _size, unsigned int _num_of_columns);
    Relation(const char* file);
    ~Relation();
    unsigned int getSize() const { return size; }
    unsigned int getNumOfColumns() const { return num_of_columns; }
    intField getValueAt(unsigned int columnNum, unsigned int rowId) const;
    bool addColumn(unsigned int col_num, const intField *values);
    JoinRelation *extractJoinRelation(unsigned int index_of_JoinField);
};


class IntermediateRelation {          // Intermediate Relations need only store the rowids of tuples from their original Relations
	unsigned int size;                // number of rowid tuples
	unsigned int numberOfRelations;   // number of original Relations represented by this Intermediate Relation
	unsigned int **rowids;            // rowids[0] -> rowids for the first original Relation this IntermediateRelation represents, etc
public:
	IntermediateRelation();
	JoinRelation *extractJoinRelation(unsigned int number_of_relation, const Relation &R, unsigned int index_of_JoinField);
	// either we do the next one and then create another Intermediate Relation using the selected tuples 
	unsigned int *selectTuplesWithIndexes(unsigned int *indexes, unsigned int length);   // return subtable of "rowids" for the indexes given
	// OR we do the following and transform this IntermediateRelation into a completely new one using already stored rowids (but only if their index is in 'rowids_that_stay') and new ones 
	bool addResults(unsigned int *rowids_that_stay, unsigned int *new_rowids, unsigned int length);   // all: size, numberOfRelations, rowids will potentially change
	// I prefer the second way!
};

#endif

#ifndef RELATION_H
#define RELATION_H

#include <iostream>
#include "JoinResults.h"
#include "FieldTypes.h"

//#define DDEBUG         // define this if functions used for debugging such as printing info should be compiled

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


class QueryRelation {    // QueryRelation is an interface for the orginal and the intermediate relations
public:
	const bool isIntermediate;
	QueryRelation(bool _isIntermediate) : isIntermediate(_isIntermediate) {}
	virtual Intermediate *performFilter(unsigned int col_id, intField value, char cmp) = 0;       // create an Intermediate if isIntermediate == false but change yourself if isIntermediate == true
	virtual Intermediate *performEqColumns(unsigned int cola_id, unsigned int colb_id) = 0;       // ^^
	virtual Intermediate *performEqColumns(unsigned int cola_id, unsigned int colb_id, unsigned int rela_id, unsigned int relb_id) = 0;    // ^^
	virtual Intermediate *performJoinWith(const QueryRelation &B, unsigned int cola_id, unsigned int colb_id) = 0;      // create a new Intermediate for the result and replace yourself with it
	virtual Intermediate *performCrossProductWith(const QueryRelation &B) = 0;                                          // ^^
	virtual bool containsRelation(unsigned int rel_id) = 0;
	virtual void performSelect(const Relation *&OrginalRelations, projection *projections, unsigned int size) = 0;      // write select to stdout
}


class Relation : public QueryRelation {        // Relation struct storing all fields for a relation
private:
	unsigned int id;
	const bool allocatedWithMmap;
    unsigned int size;  // number of tuples
    unsigned int num_of_columns;
    intField **columns; // each column saved as a sequential array of intFields
public:
    Relation(unsigned int _size, unsigned int _num_of_columns, unsigned int _id);
    Relation(const char* file, unsigned int _id);
    ~Relation();
    unsigned int getSize() const { return size; }
    unsigned int getNumOfColumns() const { return num_of_columns; }
    intField getValueAt(unsigned int columnNum, unsigned int rowId) const;
    bool addColumn(unsigned int col_num, const intField *values);
    JoinRelation *extractJoinRelation(unsigned int index_of_JoinField);
    /* @Overide */
    bool containsRelation(unsigned int rel_id) { return rel_id == id; }
    Intermediate *performFilter(unsigned int col_id, intField value, char cmp);
	Intermediate *performEqColumns(unsigned int cola_id, unsigned int colb_id);
	Intermediate *performEqColumns(unsigned int cola_id, unsigned int colb_id, unsigned int rela_id, unsigned int relb_id) { return performEqColumns(cola_id, colb_id); }
	Intermediate *performJoinWith(const QueryRelation &B, unsigned int cola_id, unsigned int colb_id);
	Intermediate *performCrossProductWith(const QueryRelation &B);
	void performSelect(const Relation *&OrginalRelations, projection *projections, unsigned int size);
};


class IntermediateRelation : public QueryRelation {    // Intermediate Relations need only store the rowids of tuples from their original Relations
	bool orginalRelations[MAX_JOIN_RELATIONS];         // orginalRelations[i] = true if i-th original relation "exists" in intermediate else false
	unsigned int size;                // number of rowid tuples
	unsigned int numberOfRelations;   // number of original Relations represented by this Intermediate Relation
	unsigned int **rowids;            // rowids[0] -> rowids for the first original Relation this IntermediateRelation represents, etc
public:
	IntermediateRelation() : QueryRelation(true) { /*TODO*/ }
	JoinRelation *extractJoinRelation(unsigned int number_of_relation, const Relation &R, unsigned int index_of_JoinField);
	/* @Overide */
	bool containsRelation(unsigned int rel_id) { return (rel_id < MAX_JOIN_RELATIONS) ? originalRelations[rel_id] : false; }
	Intermediate *performFilter(unsigned int col_id, intField value, char cmp);
	Intermediate *performEqColumns(unsigned int cola_id, unsigned int colb_id) { return NULL; }
	Intermediate *performEqColumns(unsigned int cola_id, unsigned int colb_id, unsigned int rela_id, unsigned int relb_id);
	Intermediate *performJoinWith(const QueryRelation &B, unsigned int cola_id, unsigned int colb_id);
	Intermediate *performCrossProductWith(const QueryRelation &B);
	void performSelect(const Relation *&OrginalRelations, projection *projections, unsigned int size);
};

#endif

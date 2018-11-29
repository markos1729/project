#ifndef RELATION_H
#define RELATION_H

#include <iostream>
#include <unordered_map>
#include "JoinResults.h"
#include "FieldTypes.h"
#include "SQLParser.h"

//#define DDEBUG         // define this if functions used for debugging such as printing info should be compiled

#define CACHE 4096       // 32KB

// Forward delcarations:
class Relation;
class IntermediateRelation;


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
    virtual ~QueryRelation() = default;
    explicit QueryRelation(bool _isIntermediate) : isIntermediate(_isIntermediate) {}
    virtual bool containsRelation(unsigned int rel_id) = 0;
    virtual IntermediateRelation *performFilter(unsigned int rel_id, unsigned int col_id, intField value, char cmp) = 0;    // create an Intermediate if isIntermediate == false but change yourself if isIntermediate == true
    virtual IntermediateRelation *performEqColumns(unsigned int rel_id, unsigned int cola_id, unsigned int colb_id) = 0;    // ^^
    virtual IntermediateRelation *performJoinWith(const QueryRelation &B, unsigned int rela_id, unsigned int cola_id, unsigned int relb_id,unsigned int colb_id) = 0;      // create a new Intermediate for the result and replace yourself with it
    virtual IntermediateRelation *performCrossProductWith(const QueryRelation &B) = 0;                                      // ^^
    virtual void performSelect(projection *projections, unsigned int nprojections) = 0;          // write select to stdout
protected:
	bool *filterField(intField *field, unsigned int size, intField value, char cmp, unsigned int &count);
};


class Relation : public QueryRelation {        // Relation struct storing all fields for a relation
private:
    unsigned int id;
    const bool allocatedWithMmap;
    unsigned int size;  // number of tuples
    unsigned int num_of_columns;
    intField **columns; // each column saved as a sequential array of intFields
public:
    Relation(unsigned int _size, unsigned int _num_of_columns);
    Relation(const char* file);
    ~Relation() override;
    unsigned int getSize() const { return size; }
    unsigned int getNumOfColumns() const { return num_of_columns; }
    unsigned int getId() const { return id; }
    void setId(unsigned int _id) { id = _id; }
    intField getValueAt(unsigned int columnNum, unsigned int rowId) const;
    bool addColumn(unsigned int col_num, const intField *values);
    JoinRelation *extractJoinRelation(unsigned int index_of_JoinField);
    /* @Override */
    bool containsRelation(unsigned int rel_id) override { return rel_id == id; }
    IntermediateRelation *performFilter(unsigned int rel_id, unsigned int col_id, intField value, char cmp) override;
    IntermediateRelation *performEqColumns(unsigned int rel_id, unsigned int cola_id, unsigned int colb_id) override;
    IntermediateRelation *performJoinWith(const QueryRelation &B, unsigned int rela_id, unsigned int cola_id, unsigned int relb_id, unsigned int colb_id) override;
    IntermediateRelation *performCrossProductWith(const QueryRelation &B) override;
    void performSelect(projection *projections, unsigned int nprojections) override;
};


class IntermediateRelation : public QueryRelation {       // Intermediate Relations need only store the rowids of tuples from their original Relations
    unsigned int size;                                    // size of rowids array for EVERY relation_id in 'rowids' hashmap
    unsigned int numberOfRelations;                       // number of original Relations represented by this Intermediate Relation
    unordered_map<unsigned int, unsigned int *> rowids;   // a hash table map for: <key=relation_id, value=rowids_of_that_relation>
public:
    IntermediateRelation(unsigned int rel_id, unsigned int *_rowids, unsigned int _size);
    IntermediateRelation(unsigned int rela_id, unsigned int relb_id, unsigned int *_rowids_a, unsigned int *_rowids_b, unsigned int _size);
    ~IntermediateRelation();
    JoinRelation *extractJoinRelation(unsigned int number_of_relation, const Relation &R, unsigned int index_of_JoinField);
    /* @Override */
    bool containsRelation(unsigned int rel_id) override { return rowids.find(rel_id) != rowids.end(); }
    IntermediateRelation *performFilter(unsigned int rel_id, unsigned int col_id, intField value, char cmp) override;
    IntermediateRelation *performEqColumns(unsigned int rel_id, unsigned int cola_id, unsigned int colb_id) override;
    IntermediateRelation *performJoinWith(const QueryRelation &B, unsigned int rela_id, unsigned int cola_id, unsigned int relb_id, unsigned int colb_id) override;
    IntermediateRelation *performCrossProductWith(const QueryRelation &B) override;
    void performSelect(projection *projections, unsigned int nprojections) override;
};

#endif

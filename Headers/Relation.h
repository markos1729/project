#ifndef RELATION_H
#define RELATION_H

#include <iostream>
#include "JoinResults.h"
#include "JoinRelation.h"
#include "FieldTypes.h"
#include "SQLParser.h"
#include "ConfigureParameters.h"


// Forward delcarations:
class Relation;
class IntermediateRelation;


class QueryRelation {    // QueryRelation is an interface for the orginal and the intermediate relations
public:
    const bool isIntermediate;
    virtual ~QueryRelation() = default;
    explicit QueryRelation(bool _isIntermediate) : isIntermediate(_isIntermediate) {}
    virtual bool containsRelation(unsigned int rel_id) = 0;
    virtual IntermediateRelation *performFilter(unsigned int rel_id, unsigned int col_id, intField value, char cmp) = 0;    // create an Intermediate if isIntermediate == false but change yourself if isIntermediate == true
    virtual IntermediateRelation *performEqColumns(unsigned int rela_id, unsigned int relb_id, unsigned int cola_id, unsigned int colb_id) = 0;    // ^^
    virtual IntermediateRelation *performJoinWith(QueryRelation &B, unsigned int rela_id, unsigned int cola_id, unsigned int relb_id, unsigned int colb_id) = 0;      // create a new Intermediate for the result and replace yourself with it
    virtual IntermediateRelation *performCrossProductWith(QueryRelation &B) = 0;                                      // ^^
    virtual void performSelect(projection *projections, unsigned int nprojections) = 0;          // write select to stdout
    virtual void performSum(projection *projections, unsigned int nprojections) = 0;
    virtual unsigned int getSize() const = 0;
    static void set_nrelations(unsigned int nrelations) { NumberOfRelationsInQuery = nrelations; }
protected:
    static unsigned int NumberOfRelationsInQuery;
    bool *filterField(intField *field, unsigned int size, intField value, char cmp, unsigned int &count);
    bool *eqColumnsFields(intField *field1, intField *field2, unsigned int size, unsigned int &count);
};


class Relation : public QueryRelation {        // Relation struct storing all fields for a relation
    unsigned int id;
    const bool allocatedWithMmap;
    unsigned int size;  // number of tuples
    unsigned int num_of_columns;
    intField **columns; // each column saved as a sequential array of intFields
public:
    Relation(unsigned int _size, unsigned int _num_of_columns);
    explicit Relation(const char* file);
    ~Relation() override;
    unsigned int getSize() const { return size; }
    unsigned int getNumOfColumns() const { return num_of_columns; }
    unsigned int getId() const { return id; }
    void setId(unsigned int _id) { id = _id; }
    intField getValueAt(unsigned int columnNum, unsigned int rowId) const;
    bool addColumn(unsigned int col_num, const intField *values);
    JoinRelation *extractJoinRelation(unsigned int index_of_JoinField) const;
    IntermediateRelation *performJoinWithOriginal(const Relation &B, unsigned int rela_id, unsigned int cola_id, unsigned int relb_id, unsigned int colb_id);
    IntermediateRelation *performJoinWithIntermediate(IntermediateRelation &B, unsigned int rela_id, unsigned int cola_id, unsigned int relb_id, unsigned int colb_id);
    IntermediateRelation *performCrossProductWithOriginal(const Relation &B);
    IntermediateRelation *performCrossProductWithIntermediate(IntermediateRelation &B);
    /* @Override */
    bool containsRelation(unsigned int rel_id) override { return rel_id == id; }
    IntermediateRelation *performFilter(unsigned int rel_id, unsigned int col_id, intField value, char cmp) override;
    IntermediateRelation *performEqColumns(unsigned int rela_id, unsigned int relb_id, unsigned int cola_id, unsigned int colb_id) override;
    IntermediateRelation *performJoinWith(QueryRelation &B, unsigned int rela_id, unsigned int cola_id, unsigned int relb_id, unsigned int colb_id) override;
    IntermediateRelation *performCrossProductWith(QueryRelation &B) override;
    void performSelect(projection *projections, unsigned int nprojections) override;
    void performSum(projection *projections, unsigned int nprojections) override;
private:
    unsigned int *getPassingRowIds(const bool *passing_rowids, unsigned int count);
};


class IntermediateRelation : public QueryRelation {       // Intermediate Relations need only store the rowids of tuples from their original Relations
    unsigned int size;                                    // size of rowids array for EVERY relation_id in 'rowids' hashmap
    unsigned int numberOfRelations;                       // number of original Relations represented by this Intermediate Relation
    const unsigned int maplength;                               // lentgh of arrays rowids and orginalRelations used as hash maps
    unsigned int **rowids;                                 // an array used as a hash table map for: <key=relation_id, value=rowids_of_that_relation>
    const Relation **originalRelations;                    // an array used as a hash table map for: <key=relation_id, value=pointer to the original Relation for this relation_id>
public:
    IntermediateRelation(unsigned int rel_id, unsigned int *_rowids, unsigned int _size, const Relation *original_rel, unsigned int _maplength = NumberOfRelationsInQuery);
    IntermediateRelation(unsigned int rela_id, unsigned int relb_id, unsigned int *_rowids_a, unsigned int *_rowids_b, unsigned int _size, const Relation *original_rel_a, const Relation *original_rel_b, unsigned int _maplength = NumberOfRelationsInQuery);
    ~IntermediateRelation() override;
    unsigned int getSize() const { return size; }
    unsigned int *getRowIdsFor(unsigned int rel_id) { if ( containsRelation(rel_id) ) return rowids[rel_id]; else return NULL; }
    JoinRelation *extractJoinRelation(unsigned int rel_id, unsigned int col_id);
    IntermediateRelation *performJoinWithOriginal(const Relation &B, unsigned int rela_id, unsigned int cola_id, unsigned int relb_id, unsigned int colb_id);
    IntermediateRelation *performJoinWithIntermediate(IntermediateRelation &B, unsigned int rela_id, unsigned int cola_id, unsigned int relb_id, unsigned int colb_id);
    IntermediateRelation *performCrossProductWithOriginal(const Relation &B);
    IntermediateRelation *performCrossProductWithIntermediate(IntermediateRelation &B);
    /* @Override */
    bool containsRelation(unsigned int rel_id) override { return (rel_id < maplength) ? (originalRelations[rel_id] != NULL) : false; }
    IntermediateRelation *performFilter(unsigned int rel_id, unsigned int col_id, intField value, char cmp) override;
    IntermediateRelation *performEqColumns(unsigned int rela_id, unsigned int relb_id, unsigned int cola_id, unsigned int colb_id) override;
    IntermediateRelation *performJoinWith(QueryRelation &B, unsigned int rela_id, unsigned int cola_id, unsigned int relb_id, unsigned int colb_id) override;
    IntermediateRelation *performCrossProductWith(QueryRelation &B) override;
    void performSelect(projection *projections, unsigned int nprojections) override;
    void performSum(projection *projections, unsigned int nprojections) override;
private:
    void keepOnlyMarkedRows(const bool *passing_rowids, unsigned int count);
};

#endif

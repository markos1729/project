#ifndef OPERATIONS_H
#define OPERATIONS_H


#include "Relation.h"
#include "FieldTypes.h"


#define DEFAULT_COST 100  // TODO


/* Thoughts(Updated):
 * This idea will probably not work. Each time a predicate is executed the following operations change as their arguements change.
 * We should probably figure out an order for the operations initially parsed and then execute it without further optimization.
 * This code should be changed (or dumped) later
 */


/* Base class for all operations involving Relations */
class Operation {
	const bool isBinary;
public:
	Operation(const bool _isBinary) : isBinary(_isBinary) {}
	bool getIsBinary() const { return isBinary; }
	virtual double calculateCost() const { return DEFAULT_COST; };    // calculates a cost for this operation
};


/* Operations can either be unary or binary */
class UnaryOperation : public Operation {        // unary operations modify their one and only reference to a relations
	Relation &R;
public:
	UnaryOperation(Relation &_R) : Operation(false), R(_R) {}
	virtual bool performUnary() = 0;             // performs the operation to R, returns true for success.
};

class BinaryOperation : public Operation {       // binary operations do not modify their two Relation references, instead they return a new table as a result
	const Relation &R, &S;
public:
	Operation(const Relation &_R, const Relation &_S) : Operation(true), R(_R), S(_S) {}
	virtual Result *performBinary() = 0;       // returns a pointer to the new Relation after performing the operation
};


/* Derived classes for specific operations */
/* Unary Operations */
class FilterOp : public UnaryOperation {
	intField c;                                  // constant value
	unsigned int col_num;                        // the index of the column to filter
public:
	FilterOp(Relation &_R, intField _c, unsigned int _col_num) : UnaryOperation(_R), c(_c), col_num(_col_num) {}
	bool performUnary();                         // marks the rows which "pass" the filter and modifies R to now contain only those rows
	double calculateCost() const;
};

class EqualColumnsOp : public UnaryOperation{
	unsigned int col_num1, col_num2;
public:
	EqualColumnsOp(unsigned int _col_num1, unsigned int _col_num2) : col_num1(_col_num1), col_num2(_col_num2) {}
	bool performUnary();                         // marks the rows which have equal values on given fields and modifies R to now contain only those rows 
	double calculateCost() const;
}

/* Binary Operations */
class JoinOp : public BinaryOperation {
	unsigned int joinFieldR, joinFieldS;
public:
	JoinOp(const Relation &_R, const Relation &_S, unsigned int _joinFieldR, unsigned int _joinFieldS) : BinaryOperation(_R, _S), joinFieldR(_joinFieldR), joinFieldS(_joinFieldS) {}
	Result *performBinary();                   // performs RadixHashJoin betweeen R, S: R|><|S --> JoinR|><|JoinS --RHJ--> Result(rowidR,rowidS) --> Relation RxS
	double calculateCost() const;
};

class CrossProductOp : public BinaryOperation {
public:
	CrossProductOp(const Relation &_R, const Relation &_S) : BinaryOperation(_R, _S) {}
	Result *performBinary();
	double calculateCost() const;
};

#endif

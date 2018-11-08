#ifndef OPERATIONS_H
#define OPERATIONS_H


#include "Relation.h"
#include "FieldTypes.h"


#define DEFAULT_COST 100  // TODO


/* Thoughts:
 * Should BinaryOperations return fully constructed Relation objects even if some of their fields will not be used in future Operations?
 * Maybe, because all of them could be selected at the end. However, we can know in advance which of them are in the 'select' part
 * so maybe we should use NULL pointers instead of storing the rows that are not "interesting" in that way?
 */


/* Abstract (pure virtual) base class for all operations involving Relations */
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
	virtual Relation *performBinary() = 0;       // returns a pointer to the new Relation after performing the operation
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
	Relation *performBinary();                   // performs RadixHashJoin betweeen R, S: R|><|S --> JoinR|><|JoinS --RHJ--> Result(rowidR,rowidS) --> Relation RxS
	double calculateCost() const;
};

#endif

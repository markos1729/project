//#define CATCH_CONFIG_MAIN  // This tells Catch to provide a main() - only do this in one cpp file
//#include "Headers/catch.hpp"

#include "Headers/Relation.h"
#include "Headers/RadixHashJoin.h"

int main() {
	char file_r[]="R";
	char file_s[]="S";
	
	Relation R(file_r);
	Relation S(file_s);
	
	JoinRelation JR=*R.extractJoinRelation(0);
	JoinRelation JS=*S.extractJoinRelation(1);

	Result *J=radixHashJoin(JR,JS);
	J->printRowIds();
	
	//BRUTE FORCE & COMPARE TUPLES
	
	return 0;
}

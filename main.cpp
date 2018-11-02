#include "Headers/Relation.h"
#include "Headers/RadixHashJoin.h"

#include <vector>
#include <algorithm>

int main() {
	char file_r[]="r7";
	char file_s[]="r12";
	
	try {
		Relation R(file_r);
		Relation S(file_s);

		vector<pair<unsigned int,unsigned int>> found;
		vector<pair<unsigned int,unsigned int>> expected;
		
		JoinRelation JR=*R.extractJoinRelation(1);
		JoinRelation JS=*S.extractJoinRelation(1);

		for (unsigned int i=0; i<JR.getSize(); ++i) {
			intField RV=JR.getJoinField(i);
			for (unsigned int j=0; j<JS.getSize(); ++j) {
				intField RS=JS.getJoinField(j);
				if (RS==RV) expected.push_back(make_pair(i+1,j+1));
			}	
		}	
		
		unsigned int i,j;
		Result *J=radixHashJoin(JR,JS);
		Iterator I(J);
		while (I.getNext(i,j)) found.push_back(make_pair(i,j));

		sort(found.begin(),found.end());
		sort(expected.begin(),expected.end());
		if (found==expected) printf("CORRECT\n");
		else printf("WRONG\n");
	}
	catch (...) { printf("Could not load relations\n"); }
	
	return 0;
}


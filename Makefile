all: RadixHashJoin.o test.o Relation.o
	g++ -o test test.o RadixHashJoin.o Relation.o

Relation.o: src/Relation.cpp Headers/Relation.h
	g++ -c src/Relation.cpp

RadixHashJoin.o: src/RadixHashJoin.cpp Headers/RadixHashJoin.h Headers/Relation.h
	g++ -c src/RadixHashJoin.cpp
		
test.o: test.cpp Headers/RadixHashJoin.h Headers/Relation.h
	g++ -c test.cpp
	
clean:
	rm -f *.o test

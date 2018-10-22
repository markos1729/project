#DEPRICATED

all: join.o test.o relation.o
	g++ -o test test.o join.o relation.o

relation.o: relation.cpp relation.h
	g++ -c relation.cpp

join.o: join.cpp join.h relation.h
	g++ -c join.cpp
		
test.o: test.cpp join.h relation.h
	g++ -c test.cpp
	
clean:
	rm -f *.o test

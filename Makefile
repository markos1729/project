CXX	      = g++
CXXFLAGS  = -g3 -pedantic -std=c++98 -Wall -Wextra
OBJECTS   = Relation.o RadixHashJoin.o JoinResults.o test.o
SOURCES   = src/*.cpp
HEADERS   = Headers/*.h


all: test

test: $(OBJECTS)
	$(CXX) $(CXXFLAGS) $(OBJECTS) -o test

Relation.o: src/Relation.cpp $(HEADERS)
	$(CXX) -c $(CXXFLAGS) src/Relation.cpp

RadixHashJoin.o: src/RadixHashJoin.cpp $(HEADERS)
	$(CXX) -c $(CXXFLAGS) src/RadixHashJoin.cpp

JoinResults.o: src/JoinResults.cpp $(HEADERS)
    $(CXX) -c $(CXXFLAGS) src/JoinResults.cpp

test.o: test.cpp $(HEADERS)
	$(CXX) -c $(CXXFLAGS) test.cpp


clean:
	rm -f $(OBJECTS) test

count:
	wc $(SOURCES) $(HEADERS)

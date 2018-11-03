CXX	      = g++
CXXFLAGS  = -g3 -pedantic -std=c++11 #-Wall -Wextra
OBJECTS   = objects/main.o objects/Relation.o objects/RadixHashJoin.o objects/JoinResults.o objects/test.o
SOURCES   = src/*.cpp
HEADERS   = Headers/*.h


all: project

project: $(OBJECTS)
	$(CXX) $(CXXFLAGS) $(OBJECTS) -o project

objects/main.o: $(OBJECTS) $(HEADERS) unit_testing/catch.hpp
	$(CXX) -c $(CXXFLAGS) unit_testing/main.cpp
	mv main.o objects/main.o

objects/Relation.o: src/Relation.cpp $(HEADERS)
	$(CXX) -c $(CXXFLAGS) src/Relation.cpp
	mv Relation.o objects/Relation.o

objects/RadixHashJoin.o: src/RadixHashJoin.cpp $(HEADERS)
	$(CXX) -c $(CXXFLAGS) src/RadixHashJoin.cpp
	mv RadixHashJoin.o objects/RadixHashJoin.o

objects/JoinResults.o: src/JoinResults.cpp $(HEADERS)
	$(CXX) -c $(CXXFLAGS) src/JoinResults.cpp
	mv JoinResults.o objects/JoinResults.o

objects/test.o: unit_testing/test.cpp $(HEADERS) unit_testing/catch.hpp
	$(CXX) -c $(CXXFLAGS) unit_testing/test.cpp
	mv test.o objects/test.o

clean:
	rm -f $(OBJECTS) project

count:
	wc $(SOURCES) $(HEADERS)


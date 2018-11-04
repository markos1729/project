CXX         = g++
CXXFLAGS    = -g3 -pedantic -std=c++11 #-Wall -Wextra
COMMON_OBJ  = objects/Relation.o objects/RadixHashJoin.o objects/JoinResults.o
COMMON_HEAD = Headers/*.h


all: project test


project: objects/project-main.o $(COMMON_OBJ)
	$(CXX) $(CXXFLAGS) objects/project-main.o $(COMMON_OBJ) -o project

objects/project-main.o: project-main.cpp $(COMMON_HEAD)
	$(CXX) -c $(CXXFLAGS) project-main.cpp
	mv project-main.o objects/project-main.o

objects/Relation.o: src/Relation.cpp $(COMMON_HEAD)
	$(CXX) -c $(CXXFLAGS) src/Relation.cpp
	mv Relation.o objects/Relation.o

objects/RadixHashJoin.o: src/RadixHashJoin.cpp $(COMMON_HEAD)
	$(CXX) -c $(CXXFLAGS) src/RadixHashJoin.cpp
	mv RadixHashJoin.o objects/RadixHashJoin.o

objects/JoinResults.o: src/JoinResults.cpp $(COMMON_HEAD)
	$(CXX) -c $(CXXFLAGS) src/JoinResults.cpp
	mv JoinResults.o objects/JoinResults.o


test: objects/test-main.o objects/test.o $(COMMON_OBJ)
	$(CXX) $(CXXFLAGS) objects/test-main.o objects/test.o $(COMMON_OBJ) -o test

objects/test-main.o: unit_testing/test-main.cpp $(COMMON_HEAD) unit_testing/catch.hpp
	$(CXX) -c $(CXXFLAGS) unit_testing/test-main.cpp
	mv test-main.o objects/test-main.o

objects/test.o: unit_testing/test.cpp $(COMMON_HEAD) unit_testing/catch.hpp
	$(CXX) -c $(CXXFLAGS) unit_testing/test.cpp
	mv test.o objects/test.o


clean:
	rm -f $(COMMON_OBJ) objects/project-main.o objects/test-main.o objects/test.o project test


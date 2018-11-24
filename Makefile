CXX         = g++
CXXFLAGS    = -O2 -g3 -pedantic -std=c++11 #-Wall -Wextra
COMMON_OBJ  = objects/Relation.o objects/RadixHashJoin.o objects/JoinResults.o
COMMON_HEAD = Headers/*.h


all: project test joiner


project: assert_objects_dir objects/project-main.o $(COMMON_OBJ)
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

test: assert_objects_dir objects/test-main.o objects/test.o $(COMMON_OBJ)
	$(CXX) $(CXXFLAGS) objects/test-main.o objects/test.o $(COMMON_OBJ) -o test

objects/test-main.o: unit_testing/test-main.cpp $(COMMON_HEAD) unit_testing/catch.hpp
	$(CXX) -c $(CXXFLAGS) unit_testing/test-main.cpp
	mv test-main.o objects/test-main.o

objects/test.o: unit_testing/test.cpp $(COMMON_HEAD) unit_testing/catch.hpp
	$(CXX) -c $(CXXFLAGS) unit_testing/test.cpp
	mv test.o objects/test.o


joiner: assert_objects_dir objects/joiner-main.o objects/util.o objects/SQLParser.o $(COMMON_OBJ)
	$(CXX) $(CXXFLAGS) objects/joiner-main.o objects/util.o objects/SQLParser.o $(COMMON_OBJ) -o joiner

objects/joiner-main.o: joiner-main.cpp $(COMMON_HEAD)
	$(CXX) -c $(CXXFLAGS) joiner-main.cpp
	mv joiner-main.o objects/joiner-main.o

objects/util.o: src/util.cpp $(COMMON_HEAD)
	$(CXX) -c $(CXXFLAGS) src/util.cpp
	mv util.o objects/util.o

objects/SQLParser.o: src/SQLParser.cpp $(COMMON_HEAD) Headers/SQLParser.h
	$(CXX) -c $(CXXFLAGS) src/SQLParser.cpp
	mv SQLParser.o objects/SQLParser.o

assert_objects_dir:
	mkdir -p objects


clean:
	rm -f $(COMMON_OBJ) objects/util.o objects/project-main.o objects/test-main.o objects/joiner-main.o objects/SQLParser.o project test joiner

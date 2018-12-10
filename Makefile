CXX         = g++
CXXFLAGS    = -O2 -g3 -pedantic -std=c++11 #-Wall -Wextra
COMMON_OBJ  = objects/Relation.o objects/RadixHashJoin.o objects/JoinResults.o
JOINER_OBJ  = objects/joiner-main.o objects/SQLParser.o objects/util.o
COMMON_HEAD = Headers/*.h


all: radixhashjoin joiner test-radixhashjoin test-joiner


#################
# radixhashjoin #
#################

radixhashjoin: assert_objects_dir objects/radixhashjoin-main.o $(COMMON_OBJ)
	$(CXX) $(CXXFLAGS) objects/radixhashjoin-main.o $(COMMON_OBJ) -o radixhashjoin

objects/radixhashjoin-main.o: radixhashjoin-main.cpp $(COMMON_HEAD)
	$(CXX) -c $(CXXFLAGS) radixhashjoin-main.cpp
	mv radixhashjoin-main.o objects/radixhashjoin-main.o

objects/Relation.o: src/Relation.cpp $(COMMON_HEAD)
	$(CXX) -c $(CXXFLAGS) src/Relation.cpp
	mv Relation.o objects/Relation.o

objects/RadixHashJoin.o: src/RadixHashJoin.cpp $(COMMON_HEAD)
	$(CXX) -c $(CXXFLAGS) src/RadixHashJoin.cpp
	mv RadixHashJoin.o objects/RadixHashJoin.o

objects/JoinResults.o: src/JoinResults.cpp $(COMMON_HEAD)
	$(CXX) -c $(CXXFLAGS) src/JoinResults.cpp
	mv JoinResults.o objects/JoinResults.o


##########
# joiner #
##########

joiner: assert_objects_dir $(JOINER_OBJ) $(COMMON_OBJ)
	$(CXX) $(CXXFLAGS) objects/joiner-main.o objects/util.o objects/SQLParser.o $(COMMON_OBJ) -o joiner

objects/joiner-main.o: joiner-main.cpp $(COMMON_HEAD)
	$(CXX) -c $(CXXFLAGS) joiner-main.cpp
	mv joiner-main.o objects/joiner-main.o

objects/SQLParser.o: src/SQLParser.cpp $(COMMON_HEAD) Headers/SQLParser.h
	$(CXX) -c $(CXXFLAGS) src/SQLParser.cpp
	mv SQLParser.o objects/SQLParser.o

objects/util.o: src/util.cpp $(COMMON_HEAD)
	$(CXX) -c $(CXXFLAGS) src/util.cpp
	mv util.o objects/util.o


################
# Unit Testing #
################

test-radixhashjoin: assert_objects_dir objects/test-main.o objects/test-radixhashjoin.o $(COMMON_OBJ)
	$(CXX) $(CXXFLAGS) objects/test-main.o objects/test-radixhashjoin.o $(COMMON_OBJ) -o test-radixhashjoin

objects/test-radixhashjoin.o: unit_testing/test-radixhashjoin.cpp $(COMMON_HEAD) unit_testing/catch.hpp
	$(CXX) -c $(CXXFLAGS) unit_testing/test-radixhashjoin.cpp
	mv test-radixhashjoin.o objects/test-radixhashjoin.o


test-joiner: assert_objects_dir objects/test-main.o objects/test-joiner.o $(COMMON_OBJ)
	$(CXX) $(CXXFLAGS) objects/test-main.o objects/test-joiner.o $(COMMON_OBJ) -o test-joiner

objects/test-joiner.o: unit_testing/test-joiner.cpp $(COMMON_HEAD) unit_testing/catch.hpp
	$(CXX) -c $(CXXFLAGS) unit_testing/test-joiner.cpp
	mv test-joiner.o objects/test-joiner.o


objects/test-main.o: unit_testing/test-main.cpp $(COMMON_HEAD) unit_testing/catch.hpp
	$(CXX) -c $(CXXFLAGS) unit_testing/test-main.cpp
	mv test-main.o objects/test-main.o


###########
# UTILITY #
###########

assert_objects_dir:
	mkdir -p objects

clean:
	rm -f $(COMMON_OBJ) $(JOINER_OBJ) objects/radixhashjoin-main.o objects/test-main.o objects/test-joiner.o objects/test-radixhashjoin.o radixhashjoin joiner test-radixhashjoin test-joiner

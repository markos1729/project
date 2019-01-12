CXX       = g++
CXXFLAGS  = -O2 -g3 -pedantic -std=c++11 #-Wall -Wextra
HEADERS   = ../Headers/*.h
EXE       = queryOptimizationTest

all: QueryOptimization

QueryOptimization: QueryOptimization.cpp  ../src/* $(HEADERS)
	$(CXX) $(CXXFLAGS) QueryOptimization.cpp ../src/* -o $(EXE) -pthread

clean:
	rm -f $(EXE)

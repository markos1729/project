#pragma once

#include <cassert>
#include <memory>
#include <stdexcept>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <set>
#include "Relation.hpp"
#include "Parser.hpp"
//---------------------------------------------------------------------------
namespace std {
  /// Simple hash function to enable use with unordered_map
  template<> struct hash<SelectInfo> {
    std::size_t operator()(SelectInfo const& s) const noexcept { return s.binding ^ (s.colId << 5); }
  };
};
//---------------------------------------------------------------------------
class Operator {
  /// Operators materialize their entire result

  protected:
  /// Mapping from select info to data
  std::unordered_map<SelectInfo,unsigned> select2ResultColId;
  /// The materialized results
  std::vector<uint64_t*> resultColumns;
  /// The tmp results
  std::vector<std::vector<uint64_t>> tmpResults;


  public:
  /// Require a column and add it to results
  virtual bool require(SelectInfo info) = 0;
  /// Resolves a column
  unsigned resolve(SelectInfo info) { assert(select2ResultColId.find(info)!=select2ResultColId.end()); return select2ResultColId[info]; }
  /// Run
  virtual void run() = 0;
  /// Get  materialized results
  virtual std::vector<uint64_t*> getResults();
  /// The result size
  uint64_t resultSize=0;
  /// The destructor
  virtual ~Operator() {};
};
//---------------------------------------------------------------------------
class Scan : public Operator {
  protected:
  /// The relation
  Relation& relation;
  /// The name of the relation in the query
  unsigned relationBinding;

  public:
  /// The constructor
  Scan(Relation& r,unsigned relationBinding) : relation(r), relationBinding(relationBinding) {};
  /// Require a column and add it to results
  bool require(SelectInfo info) override;
  /// Run
  void run() override;
  /// Get  materialized results
  virtual std::vector<uint64_t*> getResults() override;
};
//---------------------------------------------------------------------------
class FilterScan : public Scan {
  /// The filter info
  std::vector<FilterInfo> filters;
  /// The input data
  std::vector<uint64_t*> inputData;
  /// Apply filter
  bool applyFilter(uint64_t id,FilterInfo& f);
  /// Copy tuple to result
  void copy2Result(uint64_t id);

  public:
  /// The constructor
  FilterScan(Relation& r,std::vector<FilterInfo> filters) : Scan(r,filters[0].filterColumn.binding), filters(filters)  {};
  /// The constructor
  FilterScan(Relation& r,FilterInfo& filterInfo) : FilterScan(r,std::vector<FilterInfo>{filterInfo}) {};
  /// Require a column and add it to results
  bool require(SelectInfo info) override;
  /// Run
  void run() override;
  /// Get  materialized results
  virtual std::vector<uint64_t*> getResults() override { return Operator::getResults(); }
};
//---------------------------------------------------------------------------
class Join : public Operator {
  /// The input operators
  std::unique_ptr<Operator> left, right;
  /// The join predicate info
  PredicateInfo& pInfo;
  /// Copy tuple to result
  void copy2Result(uint64_t leftId,uint64_t rightId);
  /// Create mapping for bindings
  void createMappingForBindings();

  using HT=std::unordered_multimap<uint64_t,uint64_t>;

  /// The hash table for the join
  HT hashTable;
  /// Columns that have to be materialized
  std::unordered_set<SelectInfo> requestedColumns;
  /// Left/right columns that have been requested
  std::vector<SelectInfo> requestedColumnsLeft,requestedColumnsRight;


  /// The entire input data of left and right
  std::vector<uint64_t*> leftInputData,rightInputData;
  /// The input data that has to be copied
  std::vector<uint64_t*>copyLeftData,copyRightData;

  public:
  /// The constructor
  Join(std::unique_ptr<Operator>&& left,std::unique_ptr<Operator>&& right,PredicateInfo& pInfo) : left(std::move(left)), right(std::move(right)), pInfo(pInfo) {};
  /// Require a column and add it to results
  bool require(SelectInfo info) override;
  /// Run
  void run() override;
};
//---------------------------------------------------------------------------
class SelfJoin : public Operator {
  /// The input operators
  std::unique_ptr<Operator> input;
  /// The join predicate info
  PredicateInfo& pInfo;
  /// Copy tuple to result
  void copy2Result(uint64_t id);
  /// The required IUs
  std::set<SelectInfo> requiredIUs;

  /// The entire input data
  std::vector<uint64_t*> inputData;
  /// The input data that has to be copied
  std::vector<uint64_t*>copyData;

  public:
  /// The constructor
  SelfJoin(std::unique_ptr<Operator>&& input,PredicateInfo& pInfo) : input(std::move(input)), pInfo(pInfo) {};
  /// Require a column and add it to results
  bool require(SelectInfo info) override;
  /// Run
  void run() override;
};
//---------------------------------------------------------------------------
class Checksum : public Operator {
  /// The input operator
  std::unique_ptr<Operator> input;
  /// The join predicate info
  std::vector<SelectInfo>& colInfo;

  public:
  std::vector<uint64_t> checkSums;
  /// The constructor
  Checksum(std::unique_ptr<Operator>&& input,std::vector<SelectInfo>& colInfo) : input(std::move(input)), colInfo(colInfo) {};
  /// Request a column and add it to results
  bool require(SelectInfo info) override { throw; /* check sum is always on the highest level and thus should never request anything */ }
  /// Run
  void run() override;
};
//---------------------------------------------------------------------------

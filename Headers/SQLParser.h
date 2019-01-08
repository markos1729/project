#ifndef SQLPARSER_H
#define SQLPARSER_H

#include "FieldTypes.h"

//#define DDEBUG

struct projection {
	unsigned int rel_id;
	unsigned int col_id;
};

struct predicate {
	unsigned int rela_id;
	unsigned int cola_id;
	unsigned int relb_id;
	unsigned int colb_id;
};

struct filter {
	char cmp;
	intField value;
	unsigned int rel_id;
	unsigned int col_id;
};


struct SQLParser {
	unsigned int nrelations;
	unsigned int npredicates;
	unsigned int nfilters;
	unsigned int nprojections;
	
	unsigned int* relations;
	predicate* predicates;
	filter* filters;	
	projection* projections;

	explicit SQLParser(const char *query);
	~SQLParser();
#ifdef DDEBUG
    void show();
#endif
};

#endif


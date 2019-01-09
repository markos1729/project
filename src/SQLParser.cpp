#include <cstdio>
#include <cstring>
#include <cstdlib>
#include "../Headers/SQLParser.h"


/* Local Functions */
void split3(char *query, char* &first, char* &second, char* &third) {
	first = strtok(query, "|");
	second = strtok(NULL, "|");
	third = strtok(NULL, "|");
}

unsigned int strcnt(char *s,char c) {
	unsigned cnt = 0;
	for (unsigned int i=0; i<strlen(s); ++i)
		if (s[i] == c) cnt++;
	return cnt;
}

void parse_relations(char *s, unsigned int &nrelations, unsigned int* &relations) {
	char *token;
	nrelations = strcnt(s,' ')+1;
	relations = new unsigned int[nrelations];
	
	unsigned int i = 0;
	token = strtok(s," ");
	while (token != NULL) {
		relations[i++] = atoi(token);
		token = strtok(NULL," ");
	}
	
}

void parse_projections(char *s, unsigned int &nprojections, projection* &projections) {
	char *token;
	nprojections = strcnt(s,' ')+1;
	projections = new projection[nprojections];
	
	unsigned int i = 0;
	token = strtok(s, " ");
	while (token != NULL) {
		sscanf(token, "%u.%u", &projections[i].rel_id, &projections[i].col_id);
		token = strtok(NULL, " ");
		i++;
	}
	
}

void parse_bindings(char *s, unsigned int &npredicates, predicate* &predicates, unsigned int &nfilters, filter* &filters) {
	char *token;
	filters = new filter[strcnt(s,'&') + 1]();
	predicates = new predicate[strcnt(s,'&') + 1]();
	
	nfilters = 0;
	npredicates = 0;
	token = strtok(s,"&");
	
	while (token != NULL) {
		if (strcnt(token,'.') == 2) {
			sscanf(token, "%u.%u=%u.%u",&predicates[npredicates].rela_id, &predicates[npredicates].cola_id, &predicates[npredicates].relb_id,&predicates[npredicates].colb_id);
			npredicates++;
		}
		else {
			sscanf(token, "%u.%u%c%lu", &filters[nfilters].rel_id,&filters[nfilters].col_id, &filters[nfilters].cmp, &filters[nfilters].value);
			nfilters++;
		}
		token = strtok(NULL, "&");
	}
}


/* SQLParser Implementation */
SQLParser::SQLParser(const char *query) {
	char *first,*second,*third;
	char *queryCopy = new char[strlen(query) + 1];
	strcpy(queryCopy, query);

	// check for invalid query
	bool invalid = false;
	int count_seperators = 0;
	for (int i = 0; i < strlen(queryCopy); i++){
		if (queryCopy[i] == '|'){ count_seperators++; }
	}
	invalid = strlen(queryCopy) <= 2 || (count_seperators != 2)
			  || (queryCopy[0] == '|' && queryCopy[1] == '|')
			  || (queryCopy[strlen(queryCopy) - 2] == '|' && queryCopy[strlen(queryCopy) - 1] == '|');
	if (invalid){
        nrelations = npredicates = nfilters = nprojections = 0;
		predicates = NULL;
		filters = NULL;
		projections = NULL;
		relations = NULL;
		delete[] queryCopy;
		return;
	}

	// check for empty where clause
	bool no_where = false;
	for (int i=0; i<strlen(queryCopy)-1; ++i)
		if (queryCopy[i]=='|' && queryCopy[i+1]=='|') no_where = true;

	split3(queryCopy, first, second, third);

	if (no_where) {
		npredicates=nfilters=0;
		predicates=NULL,filters=NULL;
		parse_relations(first, nrelations, relations);
		parse_projections(second, nprojections, projections);
	} else {
		parse_relations(first, nrelations, relations);
		parse_bindings(second, npredicates, predicates, nfilters, filters);
		parse_projections(third, nprojections, projections);
	}
	#ifdef DDEBUG
    show();
	#endif
	delete[] queryCopy;
}

SQLParser::~SQLParser() {
	delete[] relations;
	delete[] predicates;
	delete[] filters;
	delete[] projections;
}

#ifdef DDEBUG
void SQLParser::show() {
	printf("%u relations:\n",nrelations);
	for (int i=0; i<nrelations; ++i)
		printf("  %u\n",relations[i]);

	printf("%u predicates:\n",npredicates);
	for (int i=0; i<npredicates; ++i)
		printf("  %u(%u) ⨝ %u(%u)\n",predicates[i].rela_id,predicates[i].cola_id,predicates[i].relb_id,predicates[i].colb_id);
	
	printf("%u filters:\n",nfilters);
	for (int i=0; i<nfilters; ++i)
		printf("  %u(%u) %c %lu\n",filters[i].rel_id,filters[i].col_id,filters[i].cmp,filters[i].value);

	printf("%u projections:\n",nprojections);
	for (int i=0; i<nprojections; ++i)
		printf("  σ %u(%u)\n",projections[i].rel_id,projections[i].col_id);
	printf("\n");
}
#endif

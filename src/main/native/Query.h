#ifndef QUERY_H
#define QUERY_H

#include "location.h"

class Query
{
public:

	// Builders
	Query();

	void accumulate_matches (std::map<location, uint16_t>& res);

private:

};

#endif // QUERY_H

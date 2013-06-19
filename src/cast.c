#include <stdlib.h>

double bytes_to_double(void* buf) {
	double val;
	memcpy(&val, buf, sizeof(double));
	return val;
}

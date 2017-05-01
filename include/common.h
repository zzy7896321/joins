#ifndef COMMON_H
#define COMMON_H

#include <cstdint>
using std::uint64_t;

typedef std::uint64_t attr_type;

struct value_type {
    attr_type key1,
              key2;
};

#endif

#pragma once
#include <cstdint>

using Key = uint64_t;
using Value = uint64_t;

struct KVPair
{
    Key key;
    Value value;
};

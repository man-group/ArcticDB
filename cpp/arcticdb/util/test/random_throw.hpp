#pragma once

#include <stdexcept>
#include <cstdlib>

#define GENERATE_RANDOM_EXCEPTIONS
#ifdef GENERATE_RANDOM_EXCEPTIONS
#define ARCTICDB_DEBUG_THROW(percentage) \
    do { \
        if (static_cast<double>(std::rand()) / RAND_MAX * 100 < percentage) { \
            throw std::runtime_error("Exception intentionally thrown"); \
        } \
    } while(0);
#else
#define ARCTICDB_DEBUG_THROW(percentage)
#endif


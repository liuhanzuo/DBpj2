#include "babydb.hpp"

// Macros to disable copying and moving
#define DISALLOW_COPY(cname)                        \
  cname(const cname &) = delete;                    \
  auto operator=(const cname &)->cname & = delete; 

#define DISALLOW_MOVE(cname)                   \
  cname(cname &&) = delete;                    \
  auto operator=(cname &&)->cname & = delete; 

#define DISALLOW_COPY_AND_MOVE(cname) \
  DISALLOW_COPY(cname);               \
  DISALLOW_MOVE(cname);

#pragma once
#include <stdio.h>
#include <assert.h>
#include "utils/rtc.h"

#define DEBUG_SHOW_LINE printf("%s:%d (func:%s)\n", __FILE__, __LINE__, __func__)
#define DEBUG_PRINTF(...) printf(...)
#define DEBUG(...) (...)

#define EMBED_DEBUG_SIGNATURE int signature_ = 0xbeefcafe;
#define CHECK_DEBUG_SIGNATURE(that) assert((that).signature_ == 0xbeefcafe);
#define ERASE_DEBUG_SIGNATURE signature_ = -1;

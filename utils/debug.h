#pragma once
#include <stdio.h>
#include "utils/rtc.h"

#define DEBUG_SHOW_LINE printf("%s:%d (func:%s)\n", __FILE__, __LINE__, __func__)
#define DEBUG_PRINTF(...) printf(...)
#define DEBUG(...) (...)
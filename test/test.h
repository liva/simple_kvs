#pragma once
#include <assert.h>
#include <stdio.h>
#define START_TEST printf("test at %s:%d %s\n", __FILE__, __LINE__, __PRETTY_FUNCTION__);
#define START_TEST_WITH_POSTFIX(str) printf("test at %s:%d %s %s\n", __FILE__, __LINE__, __PRETTY_FUNCTION__, str);
#pragma once
#include "utils/slice.h"

using namespace HayaguiKvs;

static ConstSlice CreateSliceFromChar(char c, int cnt)
{
    char buf[cnt];
    for (int i = 0; i < cnt; i++)
    {
        buf[i] = c;
    }
    return ConstSlice(buf, cnt);
}

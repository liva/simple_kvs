#include "../utils/slice.h"
#include "./test.h"
#include <assert.h>
#include <utility>

using namespace HayaguiKvs;

static void set()
{
    START_TEST;
    SliceContainer sc;
    ConstSlice slice("aaa", 3);
    sc.Set(slice);
    assert(sc.DoesMatch(slice));
}

static void get_validslice()
{
    START_TEST;
    SliceContainer sc;
    ConstSlice slice("abcd", 4);
    sc.Set(slice);
    assert(sc.IsSliceAvailable());
    assert(sc.CreateConstSlice().DoesMatch(slice));
}

void slice_container_main()
{
    set();
    get_validslice();
}
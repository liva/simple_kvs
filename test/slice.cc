#include "../utils/slice.h"
#include "./test.h"
#include <assert.h>
#include <stdlib.h>

using namespace HayaguiKvs;

template <class S1, class S2>
void helper_compare_invalid_sliced(const S1 &slice1, const S2 &slice2)
{
    CmpResult result;
    assert(slice1.Cmp(slice2, result).IsError());
    assert(static_cast<const Slice &>(slice1).Cmp(slice2, result).IsError());
    assert(slice1.Cmp(static_cast<const Slice &>(slice2), result).IsError());
    assert(static_cast<const Slice &>(slice1).Cmp(static_cast<const Slice &>(slice2), result).IsError());
}

template <class S1, class S2>
void helper_compare_valid_sliced(const S1 &slice1, const S2 &slice2, CmpResult expected_result)
{
    CmpResult result;
    assert(slice1.Cmp(slice2, result).IsOk());
    assert(result.IsSameResult(expected_result));
    assert(static_cast<const Slice &>(slice1).Cmp(slice2, result).IsOk());
    assert(result.IsSameResult(expected_result));
    assert(slice1.Cmp(static_cast<const Slice &>(slice2), result).IsOk());
    assert(result.IsSameResult(expected_result));
    assert(static_cast<const Slice &>(slice1).Cmp(static_cast<const Slice &>(slice2), result).IsOk());
    assert(result.IsSameResult(expected_result));
}

static void cmp_invalid_slices()
{
    START_TEST;
    InvalidSlice slice1, slice2;
    helper_compare_invalid_sliced(slice1, slice2);
}

static void cmp_valid_and_invalid_slice()
{
    START_TEST;
    InvalidSlice islice;
    ConstSlice vslice("aaa", 3);
    helper_compare_invalid_sliced(islice, vslice);
    helper_compare_invalid_sliced(vslice, islice);
}

static void cmp_empty_slices()
{
    START_TEST;
    ConstSlice slice1("", 0), slice2("", 0);
    helper_compare_valid_sliced(slice1, slice2, CmpResult(0));
    helper_compare_valid_sliced(slice2, slice1, CmpResult(0));
}

static void cmp_empty_and_valid_slice()
{
    START_TEST;
    ConstSlice slice1("", 0), slice2("aaa", 3);
    helper_compare_valid_sliced(slice1, slice2, CmpResult(-1));
    helper_compare_valid_sliced(slice2, slice1, CmpResult(1));
}

static void cmp_same_valid_slice()
{
    START_TEST;
    ConstSlice slice1("aaa", 3), slice2("aaa", 3);
    helper_compare_valid_sliced(slice1, slice2, CmpResult(0));
    helper_compare_valid_sliced(slice2, slice1, CmpResult(0));
}

static void cmp_same_len_different_slice()
{
    START_TEST;
    ConstSlice slice1("aaa", 3), slice2("abc", 3);
    helper_compare_valid_sliced(slice1, slice2, CmpResult(-1));
    helper_compare_valid_sliced(slice2, slice1, CmpResult(1));
}

static void cmp_different_len_different_slice()
{
    START_TEST;
    ConstSlice slice1("aaaa", 4), slice2("abc", 3);
    helper_compare_valid_sliced(slice1, slice2, CmpResult(-1));
    helper_compare_valid_sliced(slice2, slice1, CmpResult(1));
}

static void cmp_substrings()
{
    START_TEST;
    ConstSlice slice1("abcd", 4), slice2("abc", 3);
    helper_compare_valid_sliced(slice1, slice2, CmpResult(1));
    helper_compare_valid_sliced(slice2, slice1, CmpResult(-1));
}

void slice_main()
{
    cmp_invalid_slices();
    cmp_valid_and_invalid_slice();
    cmp_empty_slices();
    cmp_empty_and_valid_slice();
    cmp_same_valid_slice();
    cmp_same_len_different_slice();
    cmp_different_len_different_slice();
    cmp_substrings();
}
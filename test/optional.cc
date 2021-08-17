#include "test.h"
#include "../utils/optional.h"

using namespace HayaguiKvs;

class A
{
public:
    A() = delete;
    A(int i) : i_(i)
    {
    }
    bool DoesMatch(int i)
    {
        return i == i_;
    }
private:
    int i_;
};

static void get_valid()
{
    START_TEST;
    Optional<A> a = Optional<A>::CreateValidObj(A(1));
    assert(a.isPresent());
    A &&b = a.get();
    assert(b.DoesMatch(1));
}

static void get_invalid()
{
    START_TEST;
    Optional<A> a = Optional<A>::CreateInvalidObj(A(2));
    assert(!a.isPresent());
}

void optional_main()
{
    get_valid();
    get_invalid();
}
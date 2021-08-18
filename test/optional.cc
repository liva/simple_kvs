#include "../utils/optional.h"
#include "test.h"
#include <assert.h>
#include <vector>
std::vector<int> dummy;

using namespace HayaguiKvs;

// methods you do not need to implement on underlying objects
// - copy constructor
// - copy assingment operator
// - default constructor
class A
{
public:
    A() = delete;
    A(int i) : i_(i)
    {
    }
    A(A &&obj) : i_(obj.i_)
    {
        obj.i_ += 100;
    }
    A &operator=(A &&obj)
    {
        i_ = obj.i_;
        obj.i_ += 100;
        return *this;
    }
    ~A()
    {
    }
    A(const A &obj) = delete;
    A &operator=(const A &obj) = delete;
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
    A b = a.get();
    assert(b.DoesMatch(1));
}

static void get_invalid()
{
    START_TEST;
    Optional<A> a = Optional<A>::CreateInvalidObj(A(2));
    assert(!a.isPresent());
}

int main()
{
    get_valid();
    get_invalid();
    return 0;
}
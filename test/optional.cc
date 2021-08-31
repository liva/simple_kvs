#include "../utils/optional.h"
#include "test.h"
#include <assert.h>
#include <vector>
std::vector<int> dummy;

using namespace HayaguiKvs;

static int live_cnt = 0;

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
        live_cnt++;
    }
    A(A &&obj) : i_(obj.i_)
    {
        live_cnt++;
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
        live_cnt--;
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
    Optional<A> a = Optional<A>::CreateInvalidObj();
    assert(!a.isPresent());
}

static void copy_valid()
{
    START_TEST;
    {
        Optional<A> a = Optional<A>::CreateValidObj(A(2));
        Optional<A> b(std::move(a));
        assert(b.isPresent());
        assert(b.get().DoesMatch(2));
    }
    assert(live_cnt == 0);
}

static void copy_invalid()
{
    START_TEST;
    {
        Optional<A> a = Optional<A>::CreateInvalidObj();
        Optional<A> b(std::move(a));
        assert(!b.isPresent());
    }
    assert(live_cnt == 0);
}

static void assign_valid()
{
    START_TEST;
    {
        Optional<A> a = Optional<A>::CreateValidObj(A(2));
        Optional<A> b = Optional<A>::CreateValidObj(A(3));
        b = std::move(a);
        assert(b.isPresent());
        assert(b.get().DoesMatch(2));
        Optional<A> c = Optional<A>::CreateValidObj(A(4));
        Optional<A> d = Optional<A>::CreateInvalidObj();
        d = std::move(c);
        assert(d.isPresent());
        assert(d.get().DoesMatch(4));
    }
    assert(live_cnt == 0);
}

static void assign_invalid()
{
    START_TEST;
    {
        Optional<A> a = Optional<A>::CreateInvalidObj();
        Optional<A> b = Optional<A>::CreateValidObj(A(3));
        b = std::move(a);
        assert(!b.isPresent());
        Optional<A> c = Optional<A>::CreateInvalidObj();
        Optional<A> d = Optional<A>::CreateInvalidObj();
        d = std::move(c);
        assert(!d.isPresent());
    }
    assert(live_cnt == 0);
}

int main()
{
    get_valid();
    get_invalid();
    copy_valid();
    copy_invalid();
    assign_valid();
    assign_invalid();
    return 0;
}
class Assert
{
public:
    void test(bool flag)
    {
        if (!flag)
        {
            asserted_ = true;
        }
    }
    void RaiseErrorIfNotAsserted();

private:
    bool asserted_ = false;
};
Assert *current_assert = nullptr;
#define assert(flag) current_assert->test(flag)
#include "../utils/status.h"
#undef assert
#include "./test.h"
#include <vector>
std::vector<int> dummy;

using namespace HayaguiKvs;

void Assert::RaiseErrorIfNotAsserted()
{
    if (!asserted_)
    {
        assert(false);
    }
}

static void check_state()
{
    START_TEST;
    Status ok_state = Status::CreateOkStatus();
    Status err_state = Status::CreateErrorStatus();
    assert(ok_state.IsOk());
    assert(!ok_state.IsError());
    assert(err_state.IsError());
    assert(!err_state.IsOk());
}

static void assure_statecheck()
{
    START_TEST;
    Assert assert_obj;
    current_assert = &assert_obj;
    {
        Status ok_state = Status::CreateOkStatus();
    }
    assert_obj.RaiseErrorIfNotAsserted();
}

int main()
{
    check_state();
    assure_statecheck();
    return 0;
}
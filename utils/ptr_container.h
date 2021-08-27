#pragma once
namespace HayaguiKvs
{
    struct PtrObjInterface
    {
        virtual ~PtrObjInterface() = 0;
        virtual void Destroy() = 0;
    };
    inline PtrObjInterface::~PtrObjInterface() {}

    template <class PtrObj>
    class PtrContainer
    {
    public:
        PtrContainer(PtrObj *base)
        {
            assert(base != nullptr);
            base_ = base;
        }
        PtrContainer(const PtrContainer &obj) = delete;
        PtrContainer(PtrContainer &&obj)
        {
            base_ = obj.base_;
            MarkUsedFlagToMovedObj(std::move(obj));
        }
        ~PtrContainer()
        {
            DestroyBase();
        }
        PtrContainer &operator=(const PtrContainer &obj) = delete;
        PtrContainer &operator=(PtrContainer &&obj)
        {
            DestroyBase();
            base_ = obj.base_;
            MarkUsedFlagToMovedObj(std::move(obj));
            return *this;
        }

    protected:
        PtrContainer()
        {
            // should be called only for an invalid object
            base_ = nullptr;
        }
        void DestroyBase()
        {
            if (base_)
            {
                base_->Destroy();
            }
        }
        void MarkUsedFlagToMovedObj(PtrContainer &&obj)
        {
            obj.base_ = nullptr;
        }
        PtrObj *base_;

    private:
        typedef char correct_type;
        typedef struct
        {
            char dummy[2];
        } incorrect_type;

        static correct_type type_check(const volatile PtrObjInterface *);
        static incorrect_type type_check(...);
        static_assert(sizeof(type_check((PtrObj *)0)) == sizeof(correct_type), "PtrObj should be derived from PtrObjInterface.");
    };
}
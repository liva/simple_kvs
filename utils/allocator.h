#pragma once
#include <cstdlib>

namespace HayaguiKvs
{
    class MemAllocator
    {
    public:
        template <class T>
        static inline T *alloc()
        {
            return (T *)MemAllocator::alloc(sizeof(T));
        }
        static inline char *alloc(size_t len)
        {
            return (char *)std::malloc(len);
        }
        template <class T>
        static inline void free(T *ptr)
        {
            std::free(ptr);
        }
        static inline void free(char *ptr)
        {
            std::free(ptr);
        }
    };
}
#ifndef CHUNCK_ALLOCATOR_H
#define CHUNCK_ALLOCATOR_H

#include <vector>
#include <cstddef>
#include <algorithm>
#include "chunck.h"

class chunck_allocator
{
public:
    using value_type = location;

    chunck_allocator();
    chunck_allocator(const chunck_allocator& src);
    chunck_allocator(chunck_allocator&&) = default;


    chunck_allocator& operator = (const chunck_allocator& src);
    chunck_allocator& operator = (chunck_allocator&&) = default;

    void min_chunk_size(std::size_t n);
    std::size_t min_chunk_size() const noexcept;

    bool reserve(std::size_t total);

    location* allocate(std::size_t n);
    void deallocate(location* p, std::size_t);

    template<class U, class... Args> void construct(U* p, Args&&... args) {
        new (reinterpret_cast<void*>(p)) U(std::forward<Args>(args)...);
    }

    template<class U> void destroy(U* p) {
        p->~U();
    }

    chunck_allocator select_on_container_copy_construction() const;

private:
    std::size_t minChunkSize_;
    std::size_t freeSize_;
    std::vector<Chunck> chunks_;
};
/*
bool operator == (const chunck_allocator& a, const chunck_allocator& b) {
    return (&a == &b);
}

bool operator != (const chunck_allocator& a, const chunck_allocator& b) {
    return !(a == b);
}
*/
#endif // CHUNCK_ALLOCATOR_H


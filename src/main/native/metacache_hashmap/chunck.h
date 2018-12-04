#ifndef CHUNCK_H
#define CHUNCK_H

#include <memory>
#include "../location.h"

class Chunck
{
public:
    //Chunck();
    explicit Chunck(std::size_t size) noexcept;

    Chunck(const Chunck&);

    Chunck(Chunck&& src) noexcept;

    Chunck& operator = (const Chunck& src) = delete;

    Chunck& operator = (Chunck&& src) noexcept;

    location* begin()      const noexcept;
    location* begin_free() const noexcept;
    location* end()        const noexcept;

    std::size_t total_size() const noexcept;
    std::size_t used_size()  const noexcept;
    std::size_t free_size()  const noexcept;

    bool owns(const location* p) const noexcept;

    location* next_buffer(std::size_t n) noexcept;

private:
    std::unique_ptr<location[]> mem_;
    location* bof_;
    location* end_;
};

#endif // CHUNCK_H

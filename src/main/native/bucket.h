#ifndef BUCKET_H
#define BUCKET_H

#include <cstdint>
#include <utility>
#include "location.h"
#include "chunck_allocator.h"

class Bucket
{
public:

    using iterator         = location*;
    using const_iterator   = const location*;

    // Builders
    Bucket();
    Bucket(unsigned int&& key);

    // Variables
    location *values_;
    unsigned int key_;
    std::uint8_t capacity_;
    std::uint8_t size_;

    // Functions
    bool unused() const noexcept;
    bool empty() const noexcept;

    std::uint8_t size() const noexcept;
    std::uint8_t capacity() const noexcept;


    const unsigned int& key() const noexcept;

    location& operator [](std::uint8_t i) noexcept;
    const location& operator [](std::uint8_t i) const noexcept;
    location* begin() noexcept;
    const location* begin() const noexcept;
    const location* cbegin() const noexcept;

    location* end() noexcept;
    const location* end() const noexcept;
    const location* cend() const noexcept;

    std::uint8_t probe_length() const noexcept;

    static constexpr std::size_t
        max_bucket_size() noexcept {
            return std::numeric_limits<std::uint8_t>::max();
        }

    void deallocate(chunck_allocator& alloc);

    bool resize(chunck_allocator& alloc, std::uint8_t n);

    void clear();

    void free(chunck_allocator& alloc);

    template<class V> bool insert(chunck_allocator& alloc, V&& v) {
        if(!this->reserve(alloc, this->size_+1)) {
            return false;
        }

        this->values_[this->size_] = std::forward<V>(v);
        ++this->size_;

        return true;
    }

    template<class InputIterator, class EndSentinel> bool insert(chunck_allocator& alloc,
                InputIterator first, EndSentinel last) {
        using std::distance;

        std::uint8_t nsize = this->size_ + std::uint8_t(distance(first,last));

        if(!this->reserve(alloc, nsize)) {
            return false;
        }

        for(location* p = this->values_+this->size_; first != last; ++p, ++first) {
            *p = *first;
        }

        this->size_ = nsize;

        return true;
    }


    bool insert(chunck_allocator&,
                location* values, std::uint8_t size, std::uint8_t capacity);


    //-------------------------------------------

    //-------------------------------------------


    //-----------------------------------------------------
    void key(const unsigned int& key) noexcept;

    //-----------------------------------------------------


    //-----------------------------------------------------


    //-----------------------------------------------------
    /// @brief does not change size!
    bool reserve(chunck_allocator& alloc, std::size_t n);

private:
    //-----------------------------------------------------


};

#endif // BUCKET_H

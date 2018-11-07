#include "bucket.h"

Bucket::Bucket()
{

    this->values_ = nullptr;
    this->capacity_ = 0;
    this->size_ = 0;

}

Bucket::Bucket(unsigned int &&key) {
    this->key_ = std::move(key);
    this->values_ = nullptr;
    this->capacity_ = 0;
    this->size_ = 0;
}


bool Bucket::unused() const noexcept {
    return !values_;
}

bool Bucket::empty() const noexcept {
    return this->size_ < 1;
}

std::uint8_t Bucket::size() const noexcept {
    return this->size_;
}

std::uint8_t Bucket::capacity() const noexcept {
    return this->capacity_;
}


const unsigned int& Bucket::key() const noexcept {
    return key_;
}

location& Bucket::operator [](std::uint8_t i) noexcept {
    return values_[i];
}

const location& Bucket::operator [](std::uint8_t i) const noexcept {
    return values_[i];
}

location* Bucket::begin() noexcept {
    return values_;
}

const location* Bucket::begin() const noexcept {
    return values_;
}

const location* Bucket::cbegin() const noexcept {
    return values_;
}

location* Bucket::end() noexcept {
    return values_ + size_;
}

const location* Bucket::end() const noexcept {
    return values_ + size_;
}

const location* Bucket::cend() const noexcept {
    return values_ + size_;
}

std::uint8_t Bucket::probe_length() const noexcept {
    return 0;
}


bool Bucket::insert(chunck_allocator&,
            location* values, std::uint8_t size, std::uint8_t capacity) {
    this->values_ = values;
    this->size_ = size;
    this->capacity_ = capacity;
    return true;
}

void Bucket::free(chunck_allocator& alloc) {
    if(!this->values_) {
        return;
    }

    this->deallocate(alloc);
    this->values_ = nullptr;
    this->size_ = 0;
    this->capacity_ = 0;
    // this->probelen_ = 0;
}

void Bucket::clear() {
    this->size_ = 0;
}

void Bucket::key(const unsigned int& key) noexcept {
    this->key_ = key;
}

bool Bucket::resize(chunck_allocator& alloc, std::uint8_t n) {

    if(this->size_ < n) {
        if(!this->reserve(alloc, n)) {
            return false;
        }
    }

    this->size_ = n;

    return true;
}

void Bucket::deallocate(chunck_allocator& alloc) {

    if(!std::is_pod<location>::value) {

        //location* e;

        for(auto i = this->values_, e = i + size_; i < e; ++i) {
            std::allocator_traits<chunck_allocator>::destroy(alloc, i);
        }
    }

    std::allocator_traits<chunck_allocator>::deallocate(alloc, this->values_, this->capacity_);
}

bool Bucket::reserve(chunck_allocator& alloc, std::size_t n) {

    if(n > this->max_bucket_size()) {
        return false;
    }

    if(this->values_) {
        if(n > this->capacity_) {
            std::size_t ncap = std::size_t(n + 0.3*this->size_);

            if(ncap > this->max_bucket_size()) {
                ncap = this->max_bucket_size();
            }

            //make new array
            auto nvals = std::allocator_traits<chunck_allocator>::allocate(alloc, ncap);

            if(!nvals) {
                return false;
            }

            if(!std::is_pod<location>::value) {
                for(auto i = nvals, e = i + ncap; i < e; ++i) {
                    std::allocator_traits<chunck_allocator>::construct(alloc, i);
                }
            }

            //move values over
            auto nv = nvals;
            location* ov = this->values_;

            for(std::uint8_t i = 0; i < this->size_; ++i, ++ov, ++nv) {
                *nv = std::move(*ov);
            }

            this->deallocate(alloc);
            this->values_ = nvals;
            this->capacity_ = std::uint8_t(ncap);
        }
        else if(!std::is_pod<location>::value) {
            for(auto i = this->values_ + this->size_, e = this->values_ + n; i < e; ++i) {
                std::allocator_traits<chunck_allocator>::construct(alloc, i);
            }
        }
    }
    else {
        auto nvals = std::allocator_traits<chunck_allocator>::allocate(alloc, n);

        if(!nvals) return false;
        this->values_ = nvals;
        this->capacity_ = std::uint8_t(n);

        if(!std::is_pod<location>::value) {
            for(auto i = this->values_, e = i + this->capacity_; i < e; ++i) {
                std::allocator_traits<chunck_allocator>::construct(alloc, i);
            }
        }
    }
    return true;
}

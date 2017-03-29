#include "chunck.h"

Chunck::Chunck(std::size_t size) noexcept {

    this->bof_ = nullptr;
    this->mem_ = nullptr;
    this->end_ = nullptr;


    try {
        this->bof_ = new location[size];
        this->mem_.reset(bof_);
        this->end_ = this->bof_ + size;

    }
    catch (std::exception&) {
        this->bof_ = nullptr;

    }
}

Chunck::Chunck(const Chunck&) {
    this->mem_ = nullptr;
    this->bof_ = nullptr;
    this->end_ = nullptr;
}

Chunck::Chunck(Chunck&& src) noexcept {
    this->mem_ = std::move(src.mem_);
    this->bof_ = src.bof_;
    this->end_ = src.end_;
    src.bof_ = nullptr;
    src.end_ = nullptr;
}

//Chunck& Chunck:: operator = (const Chunck& src) = delete;
Chunck& Chunck::operator = (Chunck&& src) noexcept {
    this->mem_.swap(src.mem_);
    std::swap(src.bof_, this->bof_);
    std::swap(src.end_, this->end_);
    return *this;
}

location* Chunck::begin() const noexcept {
    return this->mem_.get();
}

location* Chunck::begin_free() const noexcept {
    return this->bof_;
}

location* Chunck::end() const noexcept {
    return this->end_;
}

std::size_t Chunck::total_size() const noexcept {
    return this->end() - this->begin();
}

std::size_t Chunck::used_size()  const noexcept {
    return this->begin_free() - this->begin();
}

std::size_t Chunck::free_size()  const noexcept {
    return this->end() - this->begin_free();
}

bool Chunck::owns(const location* p) const noexcept {
    return p >= this->begin() && p < this->end();
}

location* Chunck::next_buffer(std::size_t n) noexcept {

    if(this->free_size() < n) {
        return nullptr;
    }

    auto p = this->bof_;
    this->bof_ += n;
    return p;
}

//}

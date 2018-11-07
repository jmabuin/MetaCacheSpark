#include "chunck_allocator.h"

chunck_allocator::chunck_allocator() {

    this->minChunkSize_ = 128*1024*1024/sizeof(location);
    this->freeSize_ = 0;
}

chunck_allocator::chunck_allocator(const chunck_allocator& src) {

    this->minChunkSize_ = src.minChunkSize_;

}

chunck_allocator& chunck_allocator::operator = (const chunck_allocator& src) {
    this->minChunkSize_ = src.minChunkSize_;
    return *this;
}

void chunck_allocator::min_chunk_size(std::size_t n) {
    // std::lock_guard<std::mutex> lock(mutables_);
    this->minChunkSize_ = n;
}

std::size_t chunck_allocator::min_chunk_size() const noexcept {
    // std::lock_guard<std::mutex> lock(mutables_);
    return this->minChunkSize_;
}

bool chunck_allocator::reserve(std::size_t total) {
    // std::lock_guard<std::mutex> lock(mutables_);
    if(total > this->freeSize_) {
        this->chunks_.emplace_back(total - this->freeSize_);
        this->freeSize_ += this->chunks_.back().free_size();
    }

    return (this->freeSize_ >= total);
}

location* chunck_allocator::allocate(std::size_t n) {
    // std::lock_guard<std::mutex> lock(mutables_);
    // at the moment chunks will only be used,
    // if they have been reserved explicitly
    if(n <= this->freeSize_) {
        for(Chunck& c : chunks_) {
            location* p = c.next_buffer(n);

            if(p) {
                freeSize_ -= n;
                return p;
            }
        }
    }

    // make new chunk
    //        chunks_.emplace_back(std::max(minChunkSize_,n));
    //        auto p = chunks_.back().next_buffer(n);
    //        if(p) return p;
    // fallback
    try {
        location *p = new location[n];
        return p;
    }
    catch(std::exception&) {
        return nullptr;
    }
}

void chunck_allocator::deallocate(location* p, std::size_t) {
    // std::lock_guard<std::mutex> lock(mutables_);

    // at the moment occupied chunk buffers are not given back
    // Chunck* ??
    auto it = std::find_if(begin(this->chunks_), end(this->chunks_), [p](const Chunck& c){
        return c.owns(p);
    });

    if(it == end(chunks_)) {
        delete[] p;
    }
}





chunck_allocator chunck_allocator::select_on_container_copy_construction() const {
    // don't propagate
    return chunck_allocator{};
}

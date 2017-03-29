#include "hashmultimap.h"


//HashMultiMap::HashMultiMap()
//{
//}

HashMultiMap::HashMultiMap(const value_allocator& valloc,
                           const bucket_allocator& kalloc) :
    numKeys_(0), numValues_(0), maxLoadFactor_(default_max_load_factor()),
    keyEqual_{}, alloc_{valloc},
    buckets_{kalloc}
{
    this->buckets_.resize(5);
}

HashMultiMap::HashMultiMap(const key_equal& keyComp,
                           const value_allocator& valloc,
                           const bucket_allocator& kalloc)
    :
      numKeys_(0), numValues_(0), maxLoadFactor_(default_max_load_factor()),
      keyEqual_{keyComp}, alloc_{valloc},
      buckets_{kalloc}
{
    this->buckets_.resize(5);

}



HashMultiMap::HashMultiMap(const HashMultiMap& src):
    numKeys_(0), numValues_(0),
    maxLoadFactor_(src.maxLoadFactor_),
    keyEqual_{src.keyEqual_},
    alloc_{value_alloc::select_on_container_copy_construction(src.alloc_)},
    buckets_{}
{
    this->reserve_keys(src.numKeys_);
    this->reserve_values(src.numValues_);

    for(const auto& b : src.buckets_) {
        if(!b.unused()) this->insert(b.key(), b.begin(), b.end());
    }
}


HashMultiMap::HashMultiMap(HashMultiMap&& src):
    numKeys_(src.numKeys_), numValues_(src.numValues_),
    maxLoadFactor_(src.maxLoadFactor_),
    keyEqual_{std::move(src.keyEqual_)},
    alloc_{std::move(src.alloc_)},
    buckets_{std::move(src.buckets_)}
{ }

//-------------------------------------------------------------------
HashMultiMap &HashMultiMap::operator =(const HashMultiMap& src) {
    HashMultiMap tmp(src);
    this->swap(tmp);
    return *this;
}

//-----------------------------------------------------
HashMultiMap& HashMultiMap::operator = (HashMultiMap&& src) {
    this->numKeys_ = src.numKeys_;
    this->numValues_ = src.numValues_;
    this->maxLoadFactor_ = src.maxLoadFactor_;
    //hash_ = std::move(src.hash_);
    this->keyEqual_ = std::move(src.keyEqual_);
    this->alloc_ = std::move(src.alloc_);
    this->buckets_ = std::move(src.buckets_);
    return *this;
}

HashMultiMap::~HashMultiMap() {
    for(auto& b :this->buckets_) {

        b.deallocate(this->alloc_);
    }
}


HashMultiMap::size_type HashMultiMap::key_count() const noexcept {
    return this->numKeys_;
}

HashMultiMap::size_type HashMultiMap::value_count() const noexcept {
    return this->numValues_;
}

bool HashMultiMap::empty() const noexcept {
    return (this->numKeys_ < 1);
}


HashMultiMap::size_type HashMultiMap::bucket_count() const noexcept {
    return this->buckets_.size();
}

HashMultiMap::size_type HashMultiMap::buckets_capacity() const noexcept {
    return this->buckets_.capacity();
}

HashMultiMap::size_type HashMultiMap::max_bucket_count() const noexcept {
    return this->buckets_.max_size();
}

HashMultiMap::size_type HashMultiMap::non_empty_bucket_count() const {
    auto n = size_type(0);
    for(const auto& b : buckets_) {
        if(!b.empty()) ++n;
    }
    return n;
}


//---------------------------------------------------------------
HashMultiMap::size_type HashMultiMap::bucket_size(HashMultiMap::size_type i) const noexcept {
    return buckets_[i].size();
}


/****************************************************************
      * @brief if the value_allocator supports pre-allocation
      *        storage space for 'n' values will be allocated
      *
      * @param  n: number of values for which memory should be reserved
      * @return true on success
      */
bool HashMultiMap::reserve_values(size_type n)
{
    return alloc_config::reserve(this->alloc_, n);
}


/****************************************************************
      * @brief  rehashes to accomodate a specific number of keys
      * @param  n: number of keys for which memory should be reserved
      * @return true on success
      */
bool HashMultiMap::reserve_keys(size_type n)
{
    return this->rehash(size_type(1 + (1/this->max_load_factor() * n)));
}


/****************************************************************
      * @brief forces a specific number of buckets
      *
      * @param n: number of buckets in the hash table
      *
      * @return false, if n is less than the key count or is too small
      *         to keep the load factor below the allowed maximum
      */
bool HashMultiMap::rehash(size_type n)
{
    if(!this->rehash_possible(n)) return false;

    //make temporary new map
    HashMultiMap newmap{};
    newmap.maxLoadFactor_ = this->maxLoadFactor_;
    //this might throw
    newmap.buckets_.resize(n);

    //move old bucket contents into new hash slots
    //this should use only non-throwing operations
    for(auto& b : this->buckets_) {
        if(!b.unused()) {
            newmap.insert_into_slot(std::move(b.key_),
                                    b.values_, b.size_, b.capacity_);
        }
    }

    //should all be noexcept
    this->buckets_ = std::move(newmap.buckets_);
    //hash_ = std::move(newmap.hash_);
    return true;
}



//---------------------------------------------------------------
HashMultiMap::iterator HashMultiMap::insert(const key_type& key, const value_type& value) {
    this->make_sure_enough_buckets_left(1);
    return this->insert_into_slot(key, value);
}

HashMultiMap::iterator HashMultiMap::insert(const key_type& key, value_type&& value) {
    this->make_sure_enough_buckets_left(1);
    return this->insert_into_slot(key, std::move(value));
}

HashMultiMap::iterator HashMultiMap::insert(key_type&& key, const value_type& value) {
    this->make_sure_enough_buckets_left(1);
    return this->insert_into_slot(std::move(key), value);
}

HashMultiMap::iterator HashMultiMap::insert(key_type&& key, value_type&& value) {
    this->make_sure_enough_buckets_left(1);
    return this->insert_into_slot(std::move(key), std::move(value));
}

template<class InputIterator, class EndSentinel>
HashMultiMap::iterator HashMultiMap::insert(const key_type& key, InputIterator first, EndSentinel last) {
    this->make_sure_enough_buckets_left(1);
    return this->insert_into_slot(key, first, last);
}

template<class InputIterator, class EndSentinel>
HashMultiMap::iterator HashMultiMap::insert(key_type&& key, InputIterator first, EndSentinel last) {
    this->make_sure_enough_buckets_left(1);
    return this->insert_into_slot(std::move(key), first, last);
}


/****************************************************************
       * @brief discards values with indices n-1 ... size-1
       *        in the bucket associated with 'key'
       */
void
HashMultiMap::shrink(const key_type& key, bucket_size_type n) {
    auto it = this->find(key);
    if(it != this->end()) this->shrink(it,n);
}


void
HashMultiMap::shrink(const_iterator it, bucket_size_type n) {
    if(it->size() > n) {
        const auto old = it->size();
        const_cast<Bucket*>(&(*it))->resize(alloc_, n);
        this->numValues_ -= (old - it->size());
    }
}


/****************************************************************
       * @brief discards all values in bucket, but keeps bucket with key
       */
void
HashMultiMap::clear(const_iterator it) {
    if(!it->empty()) {
        auto n = it->size();
        const_cast<Bucket*>(&(*it))->clear();
        this->numValues_ -= n;
    }
}


//---------------------------------------------------------------
void HashMultiMap::clear() {
    if(this->numKeys_ < 1) return;

    //free bucket memory
    for(auto& b : this->buckets_) {
        b.free(this->alloc_);
    }

    this->numKeys_ = 0;
    this->numValues_ = 0;
}


//---------------------------------------------------------------
/**
       * @brief very dangerous!
       *        clears buckets without memory deallocation
       */
void HashMultiMap::clear_without_deallocation()
{
    for(auto& b : this->buckets_) {
        b.values_ = nullptr;
        b.size_ = 0;
        b.capacity_ = 0;
        //            b.probelen_ = 0;
    }

    this->numKeys_ = 0;
    this->numValues_ = 0;
}


//---------------------------------------------------------------
 HashMultiMap::const_reference HashMultiMap::bucket(size_type i) const noexcept {
     return buckets_[i];
 }
 //-----------------------------------------------------
 HashMultiMap::reference HashMultiMap::bucket(size_type i) noexcept {
     return buckets_[i];
 }


 //---------------------------------------------------------------
 /**
  * @return  iterator to bucket with all values of a key
  *          or end iterator if key not found
  */
 HashMultiMap::iterator HashMultiMap::find(const key_type& key) {
     return this->find_occupied_slot(key);
 }
 //-----------------------------------------------------
 HashMultiMap::const_iterator HashMultiMap::find(const key_type& key) const {
     return const_iterator(
         const_cast<HashMultiMap*>(this)->find_occupied_slot(key) );
 }

 //-----------------------------------------------------
 /*HashMultiMap::size_type HashMultiMap::count(const key_type& key) const {
     iterator it = this->find_occupied_slot(key);
     return (it != this->end()) ? it->size() : 0;
 }*/


 //---------------------------------------------------------------
 float HashMultiMap::load_factor() const noexcept {
     return (this->numKeys_ / float(this->buckets_.size()));
 }

 //---------------------------------------------------------------

 //-----------------------------------------------------
 float HashMultiMap::max_load_factor() const noexcept {
     return this->maxLoadFactor_;
 }
 //-----------------------------------------------------
 void HashMultiMap::max_load_factor(float lf) {
     if(lf > 1.0f) lf = 1.0f;
     if(lf < 0.1f) lf = 0.1f;

     //using std::abs;
     if(abs(maxLoadFactor_ - lf) > 0.00001f) {
         this->maxLoadFactor_ = lf;
         if(this->load_factor() > this->maxLoadFactor_) {
             this->rehash(size_type(1.1 * this->buckets_.size() * this->maxLoadFactor_ + 2));
         }
     }
 }

 //---------------------------------------------------------------
     const HashMultiMap::value_allocator&
     HashMultiMap::get_value_allocator() const noexcept {
         return this->alloc_;
     }




     //---------------------------------------------------------------
     const HashMultiMap::key_equal&
     HashMultiMap::key_eq() const noexcept {
         return this->keyEqual_;
     }


     //---------------------------------------------------------------
     HashMultiMap::iterator
      HashMultiMap::begin() noexcept { return this->buckets_.begin(); }
     //-----------------------------------------------------

     HashMultiMap::const_iterator
      HashMultiMap::begin() const noexcept { return this->buckets_.begin(); }
     //-----------------------------------------------------

     HashMultiMap::const_iterator
     HashMultiMap::cbegin() const noexcept { return this->buckets_.begin(); }

     //-----------------------------------------------------
     HashMultiMap::iterator
      HashMultiMap::end() noexcept { return this->buckets_.end(); }
     //-----------------------------------------------------

     HashMultiMap::const_iterator
      HashMultiMap::end() const noexcept { return this->buckets_.end(); }
     //-----------------------------------------------------

     HashMultiMap::const_iterator
     HashMultiMap::cend() const noexcept { return this->buckets_.end(); }

     //---------------------------------------------------------------
     HashMultiMap::local_iterator
      HashMultiMap::begin(size_type i) noexcept { return this->buckets_[i].begin(); }
     //-----------------------------------------------------

     HashMultiMap::const_local_iterator
      HashMultiMap::begin(size_type i) const noexcept { return this->buckets_[i].begin(); }
     //-----------------------------------------------------

     HashMultiMap::const_local_iterator
     HashMultiMap::cbegin(size_type i) const noexcept { return this->buckets_[i].begin(); }

     //-----------------------------------------------------
     HashMultiMap::local_iterator
      HashMultiMap::end(size_type i) noexcept { return this->buckets_[i].end(); }
     //-----------------------------------------------------
     HashMultiMap::const_local_iterator
      HashMultiMap::end(size_type i) const noexcept { return this->buckets_[i].end(); }
     //-----------------------------------------------------
     HashMultiMap::const_local_iterator
     HashMultiMap::cend(size_type i) const noexcept { return this->buckets_[i].end(); }


     //---------------------------------------------------------------
     void HashMultiMap::swap(HashMultiMap& other)
     {
         std::swap(numKeys_, other.numKeys_);
         std::swap(numValues_, other.numValues_);
         std::swap(maxLoadFactor_, other.maxLoadFactor_);
         //std::swap(hash_, other.hash_);
         std::swap(keyEqual_, other.keyEqual_);
         std::swap(alloc_, other.alloc_);
         std::swap(buckets_, other.buckets_);
     }

     HashMultiMap::iterator
         HashMultiMap::find_occupied_slot(const key_type& key)
         {
             probing_iterator it {
                 //buckets_.begin() + (this->thomas_mueller_hash(key) % buckets_.size()),
                 buckets_.begin() + (key % buckets_.size()),
                 buckets_.begin(), buckets_.end()};

             //probelen_t probelen = 0;

             //find bucket
             do {
                 if(it->unused()) return buckets_.end();
                 if(keyEqual_(it->key(), key)) return iterator(it);
             } while(++it);

             return buckets_.end();
         }

         //-----------------------------------------------------

     template<class... Values>
         HashMultiMap::iterator HashMultiMap::insert_into_slot(key_type key, Values&&... newvalues)
         {
             probing_iterator it {
                 //buckets_.begin() + (this->thomas_mueller_hash(key) % buckets_.size()),
                 buckets_.begin() + (key % buckets_.size()),
                 buckets_.begin(), buckets_.end()};

             do {
                 //empty slot found
                 if(it->unused()) {
                     if(it->insert(alloc_, std::forward<Values>(newvalues)...)) {
                         it->key_ = std::move(key);
                         ++numKeys_;
                         numValues_ += it->size();
                         return iterator(it);
                     }
                     //could not insert
                     return buckets_.end();
                 }
                 //key already inserted
                 if(keyEqual_(it->key(), key)) {
                     auto oldsize = it->size();
                     if(it->insert(alloc_, std::forward<Values>(newvalues)...)) {
                         numValues_ += it->size() - oldsize;
                         return iterator(it);
                     }
                     return buckets_.end();
                 }
             } while(++it);
             return buckets_.end();
         }


         //---------------------------------------------------------------
         void HashMultiMap::make_sure_enough_buckets_left(size_type more)
         {
             auto n = numKeys_ + more;

             if( (n / float(buckets_.size()) > maxLoadFactor_ ) ||
                 (n >= buckets_.size()) )
             {
                 rehash(std::max(
                     size_type(1 + 1.8 * n),
                     size_type(1 + 1.8 * (n / maxLoadFactor_)) ));
             }
         }


         //---------------------------------------------------------------
         bool HashMultiMap::rehash_possible(size_type n) const noexcept
         {
             //number of buckets must be greater or equal to the number of keys
             if(n == bucket_count() || n < key_count()) return false;

             //make sure we stay below the maximum load factor
             auto newload = (load_factor() * (float(n)/bucket_count()));
             if(n < bucket_count() && newload > max_load_factor()) return false;
             return true;
         }


         void HashMultiMap::show() {
             for(const auto& bucket : this->buckets_) {
                         if(!bucket.empty()) {

                             printf("%u:%u,",bucket.key(),bucket.size());


                             for(const auto& v : bucket) {
                                 printf("%u-%u,", v.tgt, v.win);
                             }
                             printf("\n");
                         }
                     }

         }

         HashMultiMap::size_type HashMultiMap::getNumKeys(){
             return this->numKeys_;
         }

void HashMultiMap::read_binary_from_file(const std::string& fileName) {

        std::ifstream is{fileName, std::ios::in | std::ios::binary};

        if(!is.good()) {
                    fprintf(stderr,"can't open file %s\n",fileName.c_str());
                    exit(1);
                }

        this->deserialize(is);


    }

void HashMultiMap::deserialize(std::istream& is) {
        using len_t = std::uint64_t;

        this->clear();

        len_t nkeys = 0;
        mc::read_binary(is, nkeys);
        len_t nvalues = 0;
        mc::read_binary(is, nvalues);

        if(nkeys > 0) {
            this->reserve_values(nvalues);
            this->reserve_keys(nkeys);

            for(len_t i = 0; i < nkeys; ++i) {
                key_type key;
                bucket_size_type nvals = 0;
                mc::read_binary(is, key);
                mc::read_binary(is, nvals);
                if(nvals > 0) {
                    auto it = this->insert_into_slot(std::move(key), nullptr, 0, 0);
                    it->resize(alloc_, nvals);

                    for(auto v = it->values_, e = v+nvals; v < e; ++v) {
                        read_binary(is, *v);
                    }
                }
            }
            this->numKeys_ = nkeys;
            this->numValues_ = nvalues;
        }
    }

void HashMultiMap::write_binary_to_file(std::string& fileName) {

    std::ofstream os{fileName, std::ios::out | std::ios::binary};

            if(!os.good()) {
                fprintf(stderr,"can't open file %s\n",fileName.c_str());
                                    exit(1);
            }

            this->serialize(os);
}

//---------------------------------------------------------------
    /**
     * @brief binary serialization of all non-emtpy buckets
     */
    void HashMultiMap::serialize(std::ostream& os) const
    {
        using len_t = std::uint64_t;

        mc::write_binary(os, len_t(non_empty_bucket_count()));
        mc::write_binary(os, len_t(value_count()));

        for(const auto& bucket : buckets_) {
            if(!bucket.empty()) {
                mc::write_binary(os, bucket.key());
                mc::write_binary(os, bucket.size());

                for(const auto& v : bucket) {
                    write_binary(os, v);
                }
            }
        }
    }
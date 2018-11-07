#ifndef HASHMULTIMAP_H
#define HASHMULTIMAP_H

#include <iostream>
#include <fstream>
#include "bucket.h"
#include "linear_probing_iterator.h"
#include "io_serialize.h"

/*****************************************************************************
 *
 * @brief helper templates for allocator configuration
 *
 *****************************************************************************/
namespace detail {

    template<class T>
    constexpr auto
    check_supports_reserve(int)
        -> decltype(std::declval<T>().reserve(std::size_t(1)), std::true_type{});

    template<class>
    constexpr std::false_type
    check_supports_reserve(char);

    template<class T>
    struct supports_reserve : public decltype(check_supports_reserve<T>(0)) {};

} // namespace detail

//-------------------------------------------------------------------
template<class Alloc, bool = detail::supports_reserve<Alloc>::value>
struct allocator_config
{
    static bool reserve(Alloc&, std::size_t) { return false; }
};

template<class Alloc>
struct allocator_config<Alloc,true>
{
    static bool reserve(Alloc& alloc, std::size_t n) {
        return alloc.reserve(n);
    }
};

/*template<
    class Key = unsigned int,
    class ValueT = location,
    class Hash = std::hash<Key>,
    class KeyEqual = std::equal_to<Key>,
    class ValueAllocator = chunck_allocator,
    class BucketAllocator = std::allocator<Key>,
    class BucketSizeT = std::uint8_t
>*/
class HashMultiMap
{

    static_assert(std::is_integral<std::uint8_t>::value &&
                  std::is_unsigned<std::uint8_t>::value,
                  "bucket size type must be an unsigned integer");


    using value_alloc  = std::allocator_traits<chunck_allocator>;
    using alloc_config = allocator_config<chunck_allocator>;

    using probelen_t = std::uint8_t;

public:

    using key_type         = unsigned int;
    using value_type       = location;
    using mapped_type      = location;
    using hasher           = std::uint32_t;
    using key_equal        = std::equal_to<unsigned int>;
    using value_allocator  = chunck_allocator;
    using bucket_size_type = std::uint8_t;

    using bucket_allocator = typename
    std::allocator_traits<std::allocator<unsigned int>>::template rebind_alloc<Bucket>;

    using bucket_store_t = std::vector<Bucket,bucket_allocator>;
    using size_type       = typename bucket_store_t::size_type;
    using reference       = Bucket&;
    using const_reference = const Bucket&;

    using iterator        = typename bucket_store_t::iterator;
    using const_iterator  = typename bucket_store_t::const_iterator;

    using local_iterator       = typename Bucket::iterator;
    using const_local_iterator = typename Bucket::const_iterator;

    using probing_iterator = linear_probing_iterator<iterator>;


    // Builders
    //HashMultiMap();

    explicit HashMultiMap(const value_allocator& valloc = value_allocator{},
                          const bucket_allocator& kalloc = bucket_allocator{});

    explicit HashMultiMap(const key_equal& keyComp,
                          const value_allocator& valloc = value_allocator{},
                          const bucket_allocator& kalloc = bucket_allocator{});

    HashMultiMap(const HashMultiMap& src);
    HashMultiMap(HashMultiMap&& src);


    HashMultiMap& operator = (const HashMultiMap& src);

    HashMultiMap& operator = (HashMultiMap&& src);


    ~HashMultiMap();

    size_type key_count() const noexcept;
    size_type value_count() const noexcept;

    bool empty() const noexcept;


    //---------------------------------------------------------------
    size_type bucket_count() const noexcept;

    size_type buckets_capacity() const noexcept;
    size_type max_bucket_count() const noexcept;

    /**
     * @return number of buckets (and keys) with at least one value
     */
    size_type non_empty_bucket_count() const;


    //---------------------------------------------------------------
    size_type bucket_size(size_type i) const noexcept;
    void show();

    inline std::uint32_t
    thomas_mueller_hash(std::uint32_t x) noexcept {
        x = ((x >> 16) ^ x) * 0x45d9f3b;
        x = ((x >> 16) ^ x) * 0x45d9f3b;
        x = ((x >> 16) ^ x);
        return x;
    }
    /****************************************************************
         * @brief if the value_allocator supports pre-allocation
         *        storage space for 'n' values will be allocated
         *
         * @param  n: number of values for which memory should be reserved
         * @return true on success
         */
    bool reserve_values(size_type n);


    /****************************************************************
         * @brief  rehashes to accomodate a specific number of keys
         * @param  n: number of keys for which memory should be reserved
         * @return true on success
         */
    bool reserve_keys(size_type n);


    /****************************************************************
         * @brief forces a specific number of buckets
         *
         * @param n: number of buckets in the hash table
         *
         * @return false, if n is less than the key count or is too small
         *         to keep the load factor below the allowed maximum
         */
    bool rehash(size_type n);


    //---------------------------------------------------------------
    iterator
    insert(const key_type& key, const value_type& value);

    iterator
    insert(const key_type& key, value_type&& value);

    iterator
    insert(key_type&& key, const value_type& value);

    iterator
    insert(key_type&& key, value_type&& value);

    //-----------------------------------------------------
    template<class InputIterator, class EndSentinel>
    iterator
    insert(const key_type& key, InputIterator first, EndSentinel last);

    template<class InputIterator, class EndSentinel>
    iterator
    insert(key_type&& key, InputIterator first, EndSentinel last);


    /****************************************************************
         * @brief discards values with indices n-1 ... size-1
         *        in the bucket associated with 'key'
         */
    void shrink(const key_type& key, bucket_size_type n);

    //-----------------------------------------------------
    void
    shrink(const_iterator it, bucket_size_type n);


    /****************************************************************
         * @brief discards all values in bucket, but keeps bucket with key
         */
    void clear(const_iterator it);


    //---------------------------------------------------------------
    void clear();


    //---------------------------------------------------------------
    /**
         * @brief very dangerous!
         *        clears buckets without memory deallocation
         */
    void clear_without_deallocation();


    const_reference bucket(size_type i) const noexcept;

    reference bucket(size_type i) noexcept;


    /**
         * @return  iterator to bucket with all values of a key
         *          or end iterator if key not found
         */
    iterator find(const key_type& key);

    const_iterator
    find(const key_type& key) const;

    //-----------------------------------------------------
    size_type
    count(const key_type& key) const;


    //---------------------------------------------------------------
    float load_factor() const noexcept;

    //---------------------------------------------------------------
    static constexpr float default_max_load_factor() noexcept {
        return 0.95;
    }

    //-----------------------------------------------------
    float max_load_factor() const noexcept;

    //-----------------------------------------------------
    void max_load_factor(float lf);



    //---------------------------------------------------------------
    const value_allocator& get_value_allocator() const noexcept;

    auto
    get_bucket_allocator() const noexcept
        -> decltype(std::declval<bucket_store_t>().get_allocator())
    {
        return buckets_.get_allocator();
    }

    //---------------------------------------------------------------
    const key_equal& key_eq() const noexcept;


    //---------------------------------------------------------------
    iterator begin() noexcept;
    //-----------------------------------------------------
    const_iterator begin() const noexcept;
    //-----------------------------------------------------
    const_iterator cbegin() const noexcept;

    //-----------------------------------------------------
    iterator end() noexcept;
    //-----------------------------------------------------
    const_iterator end() const noexcept;
    //-----------------------------------------------------
    const_iterator
    cend() const noexcept;

    //---------------------------------------------------------------
    local_iterator begin(size_type i) noexcept;
    //-----------------------------------------------------
    const_local_iterator begin(size_type i) const noexcept;
    //-----------------------------------------------------
    const_local_iterator cbegin(size_type i) const noexcept;

    //-----------------------------------------------------
    local_iterator end(size_type i) noexcept;
    //-----------------------------------------------------
    const_local_iterator end(size_type i) const noexcept;
    //-----------------------------------------------------
    const_local_iterator cend(size_type i) const noexcept;


    //---------------------------------------------------------------
    void swap(HashMultiMap& other);
    size_type getNumKeys();

    // IO Functions
    void read_binary_from_file(const std::string& fileName);
    void write_binary_to_file(std::string& fileName);

private:

    //---------------------------------------------------------------
        iterator find_occupied_slot(const key_type& key);

        //-----------------------------------------------------
        template<class... Values>
        iterator
        insert_into_slot(key_type key, Values&&... newvalues);
        //insert_into_slot(key_type key, location&&... newvalues);

        //---------------------------------------------------------------
        void make_sure_enough_buckets_left(size_type more);


        //---------------------------------------------------------------
        bool rehash_possible(size_type n) const noexcept;

    void deserialize(std::istream& is);
    void serialize(std::ostream& os) const;

    size_type numKeys_;
    size_type numValues_;
    float maxLoadFactor_;
    //hasher hash_;
    std::equal_to<unsigned int> keyEqual_;
    chunck_allocator alloc_;
    bucket_store_t buckets_;
};

#endif // HASHMULTIMAP_H

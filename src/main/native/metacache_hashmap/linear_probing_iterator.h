#ifndef LINEAR_PROBING_ITERATOR_H
#define LINEAR_PROBING_ITERATOR_H

/*****************************************************************************
 *
 * @brief  iterator adapter for linear probing within a given iterator range
 *
 * @tparam RAIterator :  random access iterator
 *
 *****************************************************************************/
template<class RAIterator>
class linear_probing_iterator
{
public:
    explicit
    linear_probing_iterator(RAIterator pos, RAIterator beg, RAIterator end):
       pos_(pos), fst_(pos), beg_(beg), end_(end)
    {}

    auto operator -> () const
        -> decltype(std::declval<RAIterator>().operator->())
    {
        return pos_.operator->();
    }

    auto operator * () const
        -> decltype(*std::declval<RAIterator>())
    {
        return *pos_;
    }

    linear_probing_iterator& operator ++ () noexcept {
        ++pos_;
        if(pos_ >= end_) {
            if(beg_ == end_) {
                pos_ = end_;
                return *this;
            }
            pos_ = beg_;
            end_ = fst_;
            beg_ = fst_;
        }
        return *this;
    }

    explicit operator bool() const noexcept {
        return (pos_ < end_);
    }
    explicit operator RAIterator () const {
        return pos_;
    }

private:
    RAIterator pos_;
    RAIterator fst_;
    RAIterator beg_;
    RAIterator end_;
};

#endif // LINEAR_PROBING_ITERATOR_H

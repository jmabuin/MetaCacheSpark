#include <cstdint>
#include <string>
#include <mutex>
#include <atomic>
#include <fstream>

using index_type = std::uint_least64_t;

struct sequence {
    index_type  index;       //number of sequence in file (+ offset)
    std::string header;      //meta information (FASTA >, FASTQ @)
    std::string   data;        //actual sequence data
    std::string   qualities;   //quality scores (FASTQ)
};

class sequence_reader
{
public:


    sequence_reader(): index_{0}, valid_{true} {}

    sequence_reader(const sequence_reader&) = delete;
    sequence_reader& operator = (const sequence_reader&) = delete;
    sequence_reader& operator = (sequence_reader&&) = delete;

    virtual ~sequence_reader() = default;

    /** @brief read & return next sequence */
    sequence next();

    /** @brief skip n sequences */
    void skip(index_type n);

    bool has_next() const noexcept { return valid_.load(); }

    index_type index() const noexcept { return index_.load(); }

    void index_offset(index_type index) { index_.store(index); }

    void seek(std::streampos pos) { do_seek(pos); }
    std::streampos tell()         { return do_tell(); }

protected:
    void invalidate() { valid_.store(false); }

    virtual std::streampos do_tell() = 0;

    virtual void do_seek(std::streampos) = 0;

    //derived readers have to implement this
    virtual void read_next(sequence&) = 0;

    virtual void skip_next() = 0;

private:
    mutable std::mutex mutables_;
    std::atomic<index_type> index_;
    std::atomic<bool> valid_;
};

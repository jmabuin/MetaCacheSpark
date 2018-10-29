#include "sequence_reader.h"

sequence sequence_reader::next()
{
    if(!has_next()) return sequence{};

    std::lock_guard<std::mutex> lock(mutables_);
    sequence seq;
    ++index_;
    seq.index = index_;
    read_next(seq);
    return seq;
}



//-------------------------------------------------------------------
void sequence_reader::skip(index_type skip)
{
    if(skip < 1) return;

    std::lock_guard<std::mutex> lock(mutables_);

    for(; skip > 0 && has_next(); --skip) {
        ++index_;
        skip_next();
    }
}

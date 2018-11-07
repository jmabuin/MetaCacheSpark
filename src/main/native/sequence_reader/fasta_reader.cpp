#include "fasta_reader.h"
#include <sstream>

fasta_reader::fasta_reader(const std::string& filename):
    sequence_reader{},
    file_{},
    linebuffer_{}
{

    if(!filename.empty()) {
        file_.open(filename);

        if(!file_.good()) {
            invalidate();
            //throw file_access_error{"can't open file " + filename};
            throw std::ifstream::failure("can't open file " + filename);
        }
    }
    else {
        //throw file_access_error{"no filename was given"};
        throw std::ifstream::failure("No filename was given");
    }
}



//-------------------------------------------------------------------
void fasta_reader::read_next(sequence& seq)
{
    if(!file_.good()) {
        invalidate();
        return;
    }
    std::string line;

    if(linebuffer_.empty()) {
        getline(file_, line);
    }
    else {
        line.clear();
        using std::swap;
        swap(line, linebuffer_);
    }

    if(line[0] != '>') {
        //throw io_format_error{"malformed fasta file - expected header char > not found"};
        throw std::ifstream::failure("malformed fasta file - expected header char > not found");
        invalidate();
        return;
    }
    seq.header = line.substr(1);
    this->sequ.header = seq.header;

    std::ostringstream seqss;

    while(file_.good()) {
        getline(file_, line);
        if(line[0] == '>') {
            linebuffer_ = line;
            break;
        }
        else {
            seqss << line;
            pos_ = file_.tellg();
        }
    }
    seq.data = seqss.str();
    this->sequ.data = seq.data;

    if(seq.data.empty()) {
        throw std::ifstream::failure("malformed fasta file - zero-length sequence: " + seq.header);
        invalidate();
        return;
    }

    if(!file_.good()) {
        invalidate();
        return;
    }
}



//-------------------------------------------------------------------
void fasta_reader::skip_next()
{
    if(!file_.good()) {
        invalidate();
        return;
    }

    if(linebuffer_.empty()) {
        file_.ignore(1);
    } else {
        linebuffer_.clear();
    }
    file_.ignore(std::numeric_limits<std::streamsize>::max(), '>');
    pos_ = file_.tellg();

    if(!file_.good()) {
        invalidate();
        return;
    } else {
        file_.unget();
        pos_ = file_.tellg();
    }
}



//-------------------------------------------------------------------
void fasta_reader::do_seek(std::streampos pos)
{
    file_.seekg(pos);
    linebuffer_.clear();

    if(!file_.good()) {
        invalidate();
    }
}



//-------------------------------------------------------------------
std::streampos fasta_reader::do_tell()
{
    return pos_;
}

std::string fasta_reader::get_header() {
    return this->sequ.header;

}

std::string fasta_reader::get_data() {
    return this->sequ.data;
}

std::string fasta_reader::get_qua(){
    return this->sequ.qualities;
}
#include "fastq_reader.h"

//-------------------------------------------------------------------
fastq_reader::fastq_reader(const std::string& filename):
    sequence_reader{},
    file_{}
{
    if(!filename.empty()) {
        file_.open(filename);

        if(!file_.good()) {
            invalidate();
            throw std::ifstream::failure{"can't open file " + filename};
        }
    }
    else {
        throw std::ifstream::failure{"no filename was given"};
    }
}



//-------------------------------------------------------------------
void fastq_reader::read_next(sequence& seq)
{
    if(!file_.good()) {
        invalidate();
        return;
    }

    std::string line;
    getline(file_, line);
    if(line.empty()) {
        invalidate();
        return;
    }
    if(line[0] != '@') {
        if(line[0] != '\r') {
            throw std::ifstream::failure{"malformed fastq file - sequence header: "  + line};
        }
        invalidate();
        return;
    }
    seq.header = line.substr(1);
    this->sequ.header = seq.header;

    getline(file_, seq.data);
    this->sequ.data = seq.data;

    getline(file_, line);
    if(line.empty() || line[0] != '+') {
        if(line[0] != '\r') {
            throw std::ifstream::failure{"malformed fastq file - quality header: "  + line};
        }
        invalidate();
        return;
    }
    getline(file_, seq.qualities);
    this->sequ.qualities = seq.qualities;

    if(!file_.good()) {
            invalidate();
            return;
        }
}



//-------------------------------------------------------------------
void fastq_reader::skip_next()
{
    if(!file_.good()) {
        invalidate();
        return;
    }

    //std::string tmp;
    //getline(file_, tmp);
    //getline(file_, tmp);
    //getline(file_, tmp);
    //getline(file_, tmp);

    //TODO does not cover all cases
    file_.ignore(std::numeric_limits<std::streamsize>::max(), '\n');
    file_.ignore(std::numeric_limits<std::streamsize>::max(), '\n');
    file_.ignore(std::numeric_limits<std::streamsize>::max(), '\n');
    file_.ignore(std::numeric_limits<std::streamsize>::max(), '\n');

    if(!file_.good()) {
        invalidate();
        return;
    }
}



//-------------------------------------------------------------------
void fastq_reader::do_seek(std::streampos pos)
{
    file_.seekg(pos);

    if(!file_.good()) {
        invalidate();
    }
}



//-------------------------------------------------------------------
std::streampos fastq_reader::do_tell()
{
    return file_.tellg();
}

std::string fastq_reader::get_header() {
    return this->sequ.header;

}

std::string fastq_reader::get_data() {
    return this->sequ.data;
}

std::string fastq_reader::get_qua(){
    return this->sequ.qualities;
}
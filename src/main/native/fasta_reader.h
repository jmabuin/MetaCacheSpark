#include "sequence_reader.h"


class fasta_reader :
    public sequence_reader
{
public:
    explicit
    fasta_reader(const std::string& filename);
    std::string get_header();
    std::string get_data();
    std::string get_qua();

protected:
    std::streampos do_tell() override;
    void do_seek(std::streampos) override;
    void read_next(sequence&) override;
    void skip_next() override;

private:
    std::ifstream file_;
    std::string linebuffer_;
    std::streampos pos_;
    sequence sequ;
};
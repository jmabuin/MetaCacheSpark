/**
 * Copyright 2019 José Manuel Abuín Mosquera <josemanuel.abuin@usc.es>
 *
 * <p>This file is part of MetaCacheSpark.
 *
 * <p>MetaCacheSpark is free software: you can redistribute it and/or modify it under the terms of the GNU
 * General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * <p>MetaCacheSpark is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details.
 *
 * <p>You should have received a copy of the GNU General Public License along with MetaCacheSpark. If not,
 * see <http://www.gnu.org/licenses/>.
 */

#include "fasta_reader.h"
#include "fastq_reader.h"

#include "com_github_jmabuin_metacachespark_io_SequenceFileReaderNative2.h"

fasta_reader *fasta_reader_pointer;
fastq_reader *fastq_reader_pointer;

bool is_fasta;

JNIEXPORT jint JNICALL Java_com_github_jmabuin_metacachespark_io_SequenceFileReaderNative2_init (JNIEnv *env, jobject obj, jstring file_name_java) {

    const char *nativeString = env->GetStringUTFChars(file_name_java, 0);

    std::string newFileName(nativeString);

    auto n = newFileName.size();
        if(newFileName.find(".fq")    == (n-3) ||
           newFileName.find(".fnq")   == (n-4) ||
           newFileName.find(".fastq") == (n-6) )
        {
            is_fasta = false;
        }
        else if(newFileName.find(".fa")    == (n-3) ||
                newFileName.find(".fna")   == (n-4) ||
                newFileName.find(".fasta") == (n-6) )
        {
            is_fasta = true;
        }

    if (is_fasta) {
        fasta_reader_pointer = new fasta_reader(newFileName);

    }
    else {
        fastq_reader_pointer = new fastq_reader(newFileName);
    }


    return 0;

}


JNIEXPORT jstring JNICALL Java_com_github_jmabuin_metacachespark_io_SequenceFileReaderNative2_next (JNIEnv *env, jobject obj) {

    sequence seq;

    if (is_fasta) {
        if (fasta_reader_pointer->has_next()) {
                seq = fasta_reader_pointer->next();
            }
            else {
                return NULL;
            }

    }
    else {
        if (fastq_reader_pointer->has_next()) {
                        seq = fastq_reader_pointer->next();
                    }
                    else {
                        return NULL;
                    }
    }




    jstring return_value = env->NewStringUTF(seq.data.c_str());

    return return_value;


}

JNIEXPORT jlong JNICALL Java_com_github_jmabuin_metacachespark_io_SequenceFileReaderNative2_total (JNIEnv *env, jobject obj) {

    return 0;

}

JNIEXPORT jlong JNICALL Java_com_github_jmabuin_metacachespark_io_SequenceFileReaderNative2_current (JNIEnv *env, jobject obj) {

    if (is_fasta) {
        return fasta_reader_pointer->index();
    }
    else {
        return fastq_reader_pointer->index();

    }


}

JNIEXPORT void JNICALL Java_com_github_jmabuin_metacachespark_io_SequenceFileReaderNative2_skip (JNIEnv *env, jobject obj, jlong num_skiped) {

    long skip_number = num_skiped;

    if (is_fasta) {
            fasta_reader_pointer->skip(skip_number);
        }
        else {
            fastq_reader_pointer->skip(skip_number);

        }
}

JNIEXPORT jstring JNICALL Java_com_github_jmabuin_metacachespark_io_SequenceFileReaderNative2_get_1header (JNIEnv *env, jobject obj) {

    if (is_fasta) {
        jstring return_value = env->NewStringUTF(fasta_reader_pointer->get_header().c_str());

            return return_value;
    }
    else {
        jstring return_value = env->NewStringUTF(fastq_reader_pointer->get_header().c_str());

            return return_value;
    }


}


JNIEXPORT jstring JNICALL Java_com_github_jmabuin_metacachespark_io_SequenceFileReaderNative2_get_1data (JNIEnv *env, jobject obj) {

    if (is_fasta) {
            jstring return_value = env->NewStringUTF(fasta_reader_pointer->get_data().c_str());

                return return_value;
        }
        else {
            jstring return_value = env->NewStringUTF(fastq_reader_pointer->get_data().c_str());

                return return_value;
        }
}


JNIEXPORT jstring JNICALL Java_com_github_jmabuin_metacachespark_io_SequenceFileReaderNative2_get_1quality (JNIEnv *env, jobject obj) {
    if (is_fasta) {
            jstring return_value = env->NewStringUTF(fasta_reader_pointer->get_qua().c_str());

                return return_value;
        }
        else {
            jstring return_value = env->NewStringUTF(fastq_reader_pointer->get_qua().c_str());

                return return_value;
        }
}

JNIEXPORT void JNICALL Java_com_github_jmabuin_metacachespark_io_SequenceFileReaderNative2_close (JNIEnv *env, jobject obj) {

    //delete(sequence_reader_pointer);
    if (is_fasta) {
            delete(fasta_reader_pointer);

        }
        else {
            delete(fastq_reader_pointer);
        }
}

/*
sequence_reader *make_sequence_reader(const std::string& filename) {

    if(filename.empty()) return nullptr;

    auto n = filename.size();
    if(filename.find(".fq")    == (n-3) ||
       filename.find(".fnq")   == (n-4) ||
       filename.find(".fastq") == (n-6) )
    {
        return new fastq_reader(filename);
    }
    else if(filename.find(".fa")    == (n-3) ||
            filename.find(".fna")   == (n-4) ||
            filename.find(".fasta") == (n-6) )
    {
        return new fasta_reader(filename);
    }

    //try to determine file type content
    std::ifstream is {filename};
    if(is.good()) {
        std::string line;
        getline(is,line);
        if(!line.empty()) {
            if(line[0] == '>') {
                is.close();
                return new fasta_reader(filename);
            }
            else if(line[0] == '@') {
                is.close();
                return new fastq_reader(filename);
            }
        }
        is.close();
        throw std::ifstream::failure{"file format not recognized"};
    }

    throw std::ifstream::failure{"file not accessible"};
    return nullptr;
}
*/
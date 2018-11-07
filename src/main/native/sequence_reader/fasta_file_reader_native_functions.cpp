#include "fasta_reader.h"
#include "com_github_jmabuin_metacachespark_io_SequenceFileReaderNative.h"

fasta_reader *fasta_reader_pointer;

JNIEXPORT jint JNICALL Java_com_github_jmabuin_metacachespark_io_SequenceFileReaderNative_init (JNIEnv *env, jobject obj, jstring file_name_java) {

    const char *nativeString = env->GetStringUTFChars(file_name_java, 0);

    std::string newFileName(nativeString);

    fasta_reader_pointer = new fasta_reader(newFileName);

    return 0;

}


JNIEXPORT jstring JNICALL Java_com_github_jmabuin_metacachespark_io_SequenceFileReaderNative_next (JNIEnv *env, jobject obj) {

    sequence seq;

    if (fasta_reader_pointer->has_next()) {
        seq = fasta_reader_pointer->next();
    }
    else {
        return NULL;
    }


    jstring return_value = env->NewStringUTF(seq.data.c_str());

    return return_value;


}

JNIEXPORT jlong JNICALL Java_com_github_jmabuin_metacachespark_io_SequenceFileReaderNative_total (JNIEnv *env, jobject obj) {

    return 0;

}

JNIEXPORT jlong JNICALL Java_com_github_jmabuin_metacachespark_io_SequenceFileReaderNative_current (JNIEnv *env, jobject obj) {


    long number = fasta_reader_pointer->index();

    return number;

}

JNIEXPORT void JNICALL Java_com_github_jmabuin_metacachespark_io_SequenceFileReaderNative_skip (JNIEnv *env, jobject obj, jlong num_skiped) {

    fasta_reader_pointer->skip(num_skiped);
}

JNIEXPORT jstring JNICALL Java_com_github_jmabuin_metacachespark_io_SequenceFileReaderNative_get_1header (JNIEnv *env, jobject obj) {
    jstring return_value = env->NewStringUTF(fasta_reader_pointer->get_header().c_str());

    return return_value;
}


JNIEXPORT jstring JNICALL Java_com_github_jmabuin_metacachespark_io_SequenceFileReaderNative_get_1data (JNIEnv *env, jobject obj) {
    jstring return_value = env->NewStringUTF(fasta_reader_pointer->get_data().c_str());

    return return_value;
}


JNIEXPORT jstring JNICALL Java_com_github_jmabuin_metacachespark_io_SequenceFileReaderNative_get_1quality (JNIEnv *env, jobject obj) {
    jstring return_value = env->NewStringUTF(fasta_reader_pointer->get_qua().c_str());

    return return_value;
}

JNIEXPORT void JNICALL Java_com_github_jmabuin_metacachespark_io_SequenceFileReaderNative_close (JNIEnv *env, jobject obj) {

    delete(fasta_reader_pointer);
}
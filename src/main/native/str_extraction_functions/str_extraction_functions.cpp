#include "com_github_jmabuin_metacachespark_io_ExtractionFunctions.h"
#include "string_utils.h"

#include <string>

constexpr const char* accession_prefix[]
{
    "GCF_",
    "AC_",
    "NC_", "NG_", "NS_", "NT_", "NW_", "NZ_",
    "AE", "AJ", "AL", "AM", "AP", "AY",
    "BA", "BK", "BX",
    "CM", "CP", "CR", "CT", "CU",
    "FM", "FN", "FO", "FP", "FQ", "FR",
    "HE",
    "JH"
};

std::string::size_type end_of_accession_number(const std::string& text, std::string::size_type start = 0) {

    if(start >= text.size()) return text.size();

    auto k = text.find('|', start);
    if(k != std::string::npos) return k;

    k = text.find(' ', start);
    if(k != std::string::npos) return k;

    k = text.find('-', start);
    if(k != std::string::npos) return k;

    k = text.find('_', start);
    if(k != std::string::npos) return k;

    k = text.find(',', start);
    if(k != std::string::npos) return k;

    return text.size();
}

std::string extract_ncbi_accession_version_number(const std::string& prefix, const std::string& text) {

    if(text.empty()) return "";

    auto i = text.find(prefix);
    if(i == std::string::npos) return "";

    //find separator *after* prefix
    auto s = text.find('.', i + prefix.size());
    if(s == std::string::npos || (s-i) > 25) return "";

    auto k = end_of_accession_number(text,s+1);

    return mc::trimmed(text.substr(i, k-i));
}

//---------------------------------------------------------
std::string extract_ncbi_accession_version_number(std::string text) {

    if(text.size() < 2) return "";

    //remove leading dots
    while(text.size() < 2 && text[0] == '.') text.erase(0);

    if(text.size() < 2) return "";

    //try to find any known prefix + separator
    for(auto prefix : accession_prefix) {
        auto num = extract_ncbi_accession_version_number(prefix, text);
        if(!num.empty()) return num;
    }

    //try to find version speparator
    auto s = text.find('.', 1);
    if(s < 25) return mc::trimmed(text.substr(0, end_of_accession_number(text,s+1)));

    return "";
}

std::string extract_genbank_identifier(const std::string& text) {

    if(text.empty()) return "";

    auto i = text.find("gi|");
    if(i != std::string::npos) {
        //skip prefix
        i += 3;
        //find end of number
        auto j = text.find('|', i);
        if(j == std::string::npos) {
            j = text.find(' ', i);
            if(j == std::string::npos) j = text.size();
        }
        return mc::trimmed(text.substr(i, j-i));
    }
    return "";
}

std::string extract_ncbi_accession_number(const std::string& prefix, const std::string& text) {

    if(text.empty()) return "";

    auto i = text.find(prefix);
    if(i != std::string::npos) {
        auto j = i + prefix.size();
        auto k = end_of_accession_number(text,j);
        //version separator
        auto l = text.find('.', j);
        if(l < k) k = l;
        return mc::trimmed(text.substr(i, k-i));
    }
    return "";
}

std::string extract_ncbi_accession_number(const std::string& text) {

    if(text.empty()) return "";

    for(auto prefix : accession_prefix) {
        auto num = extract_ncbi_accession_number(prefix, text);

        if(!num.empty()) return num;
    }
    return "";
}

JNIEXPORT jstring JNICALL Java_com_github_jmabuin_metacachespark_io_ExtractionFunctions_extract_1ncbi_1accession_1version_1number (JNIEnv *env, jobject jobj, jstring text) {

    const char *nativeString = env->GetStringUTFChars(text, 0);

    std::string new_text(nativeString);

    std::string value = extract_ncbi_accession_version_number(new_text);

    jstring return_value = env->NewStringUTF(value.c_str());

    env->ReleaseStringUTFChars(text, nativeString);

    return return_value;

}


JNIEXPORT jstring JNICALL Java_com_github_jmabuin_metacachespark_io_ExtractionFunctions_extract_1genbank_1identifier (JNIEnv *env, jobject jobj, jstring text) {

    const char *nativeString = env->GetStringUTFChars(text, 0);

    std::string new_text(nativeString);

    std::string value = extract_genbank_identifier(new_text);

    jstring return_value = env->NewStringUTF(value.c_str());

    env->ReleaseStringUTFChars(text, nativeString);

    return return_value;

}

JNIEXPORT jstring JNICALL Java_com_github_jmabuin_metacachespark_io_ExtractionFunctions_extract_1ncbi_1accession_1number (JNIEnv *env, jobject jobj, jstring text) {

    const char *nativeString = env->GetStringUTFChars(text, 0);

    std::string new_text(nativeString);

    std::string value = extract_ncbi_accession_number(new_text);

    jstring return_value = env->NewStringUTF(value.c_str());

    env->ReleaseStringUTFChars(text, nativeString);

    return return_value;
}


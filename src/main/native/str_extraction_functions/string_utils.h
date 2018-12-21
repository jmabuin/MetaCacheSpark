
#ifndef MC_STRING_UTILS_H_
#define MC_STRING_UTILS_H_

#include <string>
#include <algorithm>


namespace mc {


/*****************************************************************************
 *
 * @brief removes trailing whitespace from string
 *
 *****************************************************************************/
template<class C, class T, class A>
inline void
trimr(std::basic_string<C,T,A>& s)
{
    s.erase(
        std::find_if_not(s.rbegin(), s.rend(),
                         [](char c) { return std::isspace(c);} ).base(),
        s.end() );
}

/*****************************************************************************
 *
 * @brief removes leading whitespace from string
 *
 *****************************************************************************/
template<class C, class T, class A>
inline void
triml(std::basic_string<C,T,A>& s)
{
    s.erase(
        s.begin(),
        std::find_if_not(s.begin(), s.end(),
                         [](char c) { return std::isspace(c);})
    );
}

/*****************************************************************************
 *
 * @brief removes leading and trailing whitespace from string
 *
 *****************************************************************************/
template<class C, class T, class A>
inline void
trim(std::basic_string<C,T,A>& s)
{
    triml(s);
    trimr(s);
}



/*****************************************************************************
 *
 * @return string with trailing whitespace removed
 *
 *****************************************************************************/
template<class C, class T, class A>
inline std::basic_string<C,T,A>
ltrimmed(std::basic_string<C,T,A> s)
{
    triml(s);
    return s;
}

/*****************************************************************************
 *
 * @return string with leading whitespace removed
 *
 *****************************************************************************/
template<class C, class T, class A>
inline std::basic_string<C,T,A>
rtrimmed(std::basic_string<C,T,A> s)
{
    trimr(s);
    return s;
}

/*****************************************************************************
 *
 * @return string with leading and trailing whitespace removed
 *
 *****************************************************************************/
template<class C, class T, class A>
inline std::basic_string<C,T,A>
trimmed(std::basic_string<C,T,A> s)
{
    triml(s);
    trimr(s);
    return s;
}

}  // namespace mc


#endif

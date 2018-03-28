#ifndef LOCATION_H
#define LOCATION_H

#include "io_serialize.h"

struct location {
        constexpr
        location(unsigned int g = 0, unsigned int w = 0) noexcept :
            tgt{g}, win{w}
        {}

        unsigned int tgt;
        unsigned int win;

        friend bool
        operator < (const location& a, const location& b) noexcept {
            if(a.tgt < b.tgt) return true;
            if(a.tgt > b.tgt) return false;
            return (a.win < b.win);
        }

        friend void read_binary(std::istream& is, location& p) {
                    mc::read_binary(is, p.tgt);
                    mc::read_binary(is, p.win);
                }
                friend void write_binary(std::ostream& os, const location& p) {
                    mc::write_binary(os, p.tgt);
                    mc::write_binary(os, p.win);
                }

};



#endif // LOCATION_H

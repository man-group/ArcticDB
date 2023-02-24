/*
* Copyright 2023 Man Group Operations Ltd.
* NO WARRANTY, EXPRESSED OR IMPLIED
*/

#pragma once

#include <string>
#include <ostream>
#include <fstream>
#include <fmt/format.h>
#include <stdexcept>
#include <typeinfo>

namespace arcticdb {

std::string get_stack();
std::string get_type_name(const std::type_info & );

class ArcticNativeCxxException : public std::exception {
public:

    explicit ArcticNativeCxxException(std::string_view m, bool capture_stack=true) : msg_{m} {
        if(capture_stack){
            msg_.append(".\n");
            msg_.append(get_stack());
        }
    }

    explicit ArcticNativeCxxException(const std::exception & exc): ArcticNativeCxxException(exc.what()){
        msg_.insert(0, " ");
        msg_.insert(0, arcticdb::get_type_name(typeid(exc)));

    }

    explicit ArcticNativeCxxException(std::string m, const std::exception & exc): ArcticNativeCxxException(m){
        msg_.insert(0, " ");
        msg_.insert(0, arcticdb::get_type_name(typeid(exc)));

        if(exc.what()) {
            msg_.append(".");
            msg_.append(exc.what());
        }
    }


    virtual const char * what() const noexcept override {
        return msg_.c_str();
    }
private:
    std::string msg_;
};

template<class...Args>
void throw_native_exception(const char* msg, Args&&...args){
    throw ArcticNativeCxxException(fmt::format(msg, args...));
}

inline void rethrow_exception(const std::exception & exc){
    throw ArcticNativeCxxException(exc);
}

}

// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <google/protobuf/stubs/callback.h>

#include <functional>
#include <utility>

#include "byte/base/closure.h"
#include "byte/include/macros.h"

namespace bcache2 {

class FunctionClosure : public Closure<void> {
 public:
    explicit FunctionClosure(std::function<void(void)>&& func) : func_(std::move(func)) {}
    FunctionClosure(std::function<void(void)>&& func, bool self_delete)
        : func_(std::move(func)), self_delete_(self_delete) {}
    ~FunctionClosure() {}

    void Run() override {
        func_();
        if (LIKELY(self_delete_)) {
            delete this;
        }
    }

    bool IsSelfDelete() const override { return self_delete_; }

 private:
    std::function<void(void)> func_;
    bool self_delete_ = true;
};

class GoogleFunctionClosure : public google::protobuf::Closure {
 public:
    explicit GoogleFunctionClosure(std::function<void(void)>&& func) : func_(std::move(func)) {}

    void Run() override {
        func_();
        delete this;
    }

 private:
    std::function<void(void)> func_;
};

inline Closure<void>* NewFuncClosure(std::function<void(void)> func) {
    return new FunctionClosure(std::move(func));
}

inline Closure<void>* NewPermanentFuncClosure(std::function<void(void)> func) {
    return new FunctionClosure(std::move(func), false);
}

inline GoogleFunctionClosure* NewGoogleClosure(std::function<void(void)> func) {
    return new GoogleFunctionClosure(std::move(func));
}

}  // namespace bcache2

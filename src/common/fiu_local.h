// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#ifndef FIU_ENABLE

// Disable api in fiu.h
#define fiu_init(flags) 0
#define fiu_fail(name) 0
#define fiu_failinfo() NULL
#define fiu_do_on(name, action)
#define fiu_exit_on(name)
#define fiu_return_on(name, retval)

// Disable api in fiu-control.h
#define fiu_enable(name, failnum, failinfo, flags) 0
#define fiu_enable_random(name, failnum, failinfo, flags, probability) 0
#define fiu_enable_external(name, failnum, failinfo, flags, external_cb) 0
#define fiu_enable_stack(name, failnum, failinfo, flags, func, func_pos_in_stack) 0
#define fiu_enable_stack_by_name(name, failnum, failinfo, flags, func_name, func_pos_in_stack) 0
#define fiu_disable(name) 0
#define fiu_rc_fifo(basename) 0
#define fiu_rc_string(cmd, error) 0

#else

#include <fiu-control.h>
#include <fiu.h>

#endif /* FIU_ENABLE */

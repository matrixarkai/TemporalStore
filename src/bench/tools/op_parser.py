#!/bin/env python3
# translate the Show Operations from the bench log into ConsistencyChecker unit test code

import re


def gen_operation(key, module, function, request, response, start_time_us, end_time_us, code):
    if module == "STRING":
        if function == "Set":
            function = "SET"
            value = re.search(r'value: "(.*?)"', request).group(1)
            ttl = 0
        elif function == "Setex":
            function = "SETEX"
            value = re.search(r'value: "(.*?)"', request).group(1)
            ttl = re.search(r"ttl_ms: (\d+)", request).group(1)
        elif function == "Get":
            function = "GET"
            ttl = 0
            value = ""
            if code == "OK":
                value = re.search(r'value: "(.*?)"', response).group(1)
        else:
            assert False, "function {} not support".format(function)
        print(f'GenOperation(Module::STRING, str2::Function::{function}, {start_time_us}, {end_time_us}, "{key}", "{value}", {ttl}, k{code}),')
    elif module == "COMMON":
        value = ""
        if function == "Expire":
            function = "EXPIRE"
            ttl = re.search(r"ttl_ms: (\d+)", request).group(1)
        elif function == "Ttl":
            function = "TTL"
            if code == "OK":
                ttl = re.search(r"ttl_ms: (\d+)", request).group(1)
            else:
                ttl = 0
        elif function == "DelObject":
            function = "DEL_OBJECT"
            ttl = 0
        else:
            assert False, "function {} not support".format(function)
        function = function.upper()
        print(f'GenOperation(Module::COMMON, common2::Function::{function}, {start_time_us}, {end_time_us}, "{key}", "{value}", {ttl}, k{code}),')


def gen_hash_operation():
    pass


if __name__ == "__main__":
    while True:
        try:
            line = input()
            if line.find("Show Operations") == -1:
                continue

            # parse operation
            key = re.search(r"Key:(.+?), ", line).group(1)
            module = re.search(r"Module=(.+?), ", line).group(1)
            function = re.search(r"Function=(.+?), ", line).group(1)
            request = re.search(r"Request=(.+?), ", line).group(1)
            response = re.search(r"Response=(.+?), ", line).group(1)
            start_time_us = re.search(r"StartTimeUs=(.+?), ", line).group(1)
            end_time_us = re.search(r"EndTimeUs=(.+?), ", line).group(1)
            code = re.search(r"Message=(\w+)", line).group(1)

            # translate
            if module == "STRING" or module == "COMMON":
                gen_operation(key, module, function, request, response, start_time_us, end_time_us, code)
            elif module == "HASH":
                gen_hash_operation()
            else:
                assert False, "module not support"
        except EOFError:
            break

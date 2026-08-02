#!/usr/bin/python3
import subprocess

if __name__ == "__main__":
    cache_bench = "./build/src/cache/tools/_build/cache_bench"
    args = {}
    args["workers"] = 12
    args["value_size_range"] = "256,256"
    args["read_ratio"] = 90
    args["read_inserted_ratio"] = 100
    args["capacity_mb"] = 10000  # 10GB capacity
    args["preload"] = 41940000  # Fully preload 10GB cache
    args["total_ops"] = 100000000
    args["policy"] = "fifo"
    args["engine"] = "dram"
    args["bench_type"] = "simple"
    path = "output/"
    numa_bind_prefix = "numactl --cpunodebind=1"
    pmem_path = "/mnt/pmem0"
    for mem_type in ["pmem", "dram"]:
        for po_ in ["fifo", "slru"]:
            # Current implmentation only consider two sockets (NUMA domains)
            for numa in ["local", "remote"]:
                for wks_ in range(12, 13):
                    if numa == "local":
                        if mem_type == "pmem":
                            pmem_path = "/mnt/pmem1"
                        numa_bind = numa_bind_prefix + " --membind=1 "
                    else:
                        if mem_type == "pmem":
                            pmem_path = "/mnt/pmem0"
                            numa_bind = numa_bind_prefix + " --membind=1 "
                        if mem_type == "dram":
                            numa_bind = numa_bind_prefix + " --membind=0 "
                    args["pmem_path"] = pmem_path
                    if mem_type == "pmem":
                        subprocess.run(["rm", "-rf", args["pmem_path"] + "/*"])
                    args["engine"] = mem_type
                    f = open(
                        path + "_".join([args["engine"], numa,
                                         str(wks_), po_]) + ".output", "w")
                    args["workers"] = wks_
                    args["policy"] = po_
                    args["total_ops"] = 1000000 * wks_

                    args_serial = cache_bench + " "
                    args_serial += " ".join(
                        ["-" + key + "=" + str(args[key]) for key in args])
                    subprocess.run([numa_bind + args_serial],
                                   shell=True,
                                   stdout=f)
                    f.close()
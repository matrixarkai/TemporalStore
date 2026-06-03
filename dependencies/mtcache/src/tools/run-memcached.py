#!/usr/bin/python3
import subprocess
import time

if __name__ == "__main__":
    mem_type = 'dram'
    # Current implmentation only consider two sockets (NUMA domains)
    numa_ = 'remote'
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
    args["bench_type"] = "memcached"
    path = "output/memcached/"
    numa_bind_prefix = "numactl --cpunodebind=1"
    pmem_path = "/mnt/pmem0"
    mem_args = [
        'numactl', '--cpunodebind=1', '--membind=1', 'memcached', '-l',
        '127.0.0.1', '-t', '12', '-c', '48000', '-o', 'no_lru_crawler', '-m',
        str(args['capacity_mb']), '-b', '4096', '-u', 'root', '-a', '755'
    ]
    if mem_type == 'pmem':
        if numa_ == 'local':
            mem_args += ['-e', '/mnt/pmem0/memc_mmap']
        else:
            mem_args += ['-e', '/mnt/pmem1/memc_mmap']
    for mem_type in ["dram", "pmem"]:
        for po_ in ["fifo", "slru"]:
            for numa in [numa_]:
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
                    if mem_type == "pmem":
                        subprocess.run(["rm", "-rf", pmem_path + "/*"])
                    args["engine"] = mem_type
                    f = open(
                        path + "_".join([args["engine"], numa,
                                         str(wks_), po_]) + ".output", "w")
                    args["workers"] = wks_
                    args["policy"] = po_
                    args["total_ops"] = 10000000 * wks_

                    args_serial = cache_bench + " "
                    args_serial += " ".join(
                        ["-" + key + "=" + str(args[key]) for key in args])
                    memcached_server = subprocess.Popen(mem_args,
                                                        stdout=subprocess.PIPE)
                    time.sleep(5)
                    subprocess.run([numa_bind + args_serial],
                                   shell=True,
                                   stdout=f)
                    f.close()
                    memcached_server.terminate()
                    time.sleep(5)
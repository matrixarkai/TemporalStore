import logging
import subprocess


def run_shell(command):
    try:
        stdout = subprocess.check_output(command, shell=True, stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as ex:
        stdout = ex.output
    logging.debug("run command '{}' finish, output: '{}'".format(command, stdout))
    return stdout


def bvc_pull(repo, version=None, target_path="./"):
    if version is not None:
        run_shell("bvc clone -f {} {} --version {}".format(repo, target_path, version))
    else:
        run_shell("bvc clone -f {} {}".format(repo, target_path))


def kill_process_by_name(name, signal="KILL"):
    run_shell("kill -{} `pidof {}`".format(signal, name))


def get_iplist_by_name(name):
    output = run_shell("gh -s {}".format(name)).decode().strip()
    return output.split("\n")


if __name__ == "__main__":
    get_iplist_by_name("bytedance.bcache2.chaos")

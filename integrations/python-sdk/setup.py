# SPDX-License-Identifier: Apache-2.0
from pathlib import Path
from shutil import copyfile

from grpc_tools import protoc
from setuptools import setup
from setuptools.command.build_py import build_py
from setuptools.command.sdist import sdist


proto_path = Path("temporalstore/v1/temporalstore.proto")


def schema_dir():
    project = Path(__file__).resolve().parent
    bundled = project / "proto"
    return bundled if (bundled / proto_path).is_file() else project.parents[1] / "proto"


class Build(build_py):
    def run(self):
        super().run()
        source = schema_dir()
        output = Path(self.build_lib).resolve()
        result = protoc.main([
            "protoc", f"-I{source}", f"--python_out={output}",
            f"--grpc_python_out={output}", str(source / proto_path),
        ])
        if result:
            raise RuntimeError(f"could not generate TemporalStore bindings: protoc returned {result}")


class SourceDistribution(sdist):
    def make_release_tree(self, base_dir, files):
        super().make_release_tree(base_dir, files)
        destination = Path(base_dir) / "proto" / proto_path
        destination.parent.mkdir(parents=True, exist_ok=True)
        copyfile(schema_dir() / proto_path, destination)


setup(cmdclass={"build_py": Build, "sdist": SourceDistribution})

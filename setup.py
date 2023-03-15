import os
import os.path as osp
import subprocess
import sys
from distutils.core import Command
from setuptools import setup
from setuptools import Extension, find_packages
from setuptools.command.build_ext import build_ext
from setuptools.command.build_py import build_py
from setuptools.command.develop import develop
from wheel.bdist_wheel import bdist_wheel
from pathlib import Path
from pkg_resources import packaging

class ProtobufFiles(object):
    def __init__(
        self,
        include_dir="cpp/proto",  # -I
        python_out_dir="python",  # --python_out
        grpc_out_dir=None,  # --grpc_python_out
        sources=[],  # arguments of proto
    ):
        self.include_dir = include_dir
        self.sources = sources
        self.python_out_dir = python_out_dir
        self.grpc_out_dir = grpc_out_dir

    def compile(self):
        # import deferred here to avoid blocking installation of dependencies
        cmd = ["-mgrpc_tools.protoc"]

        cmd.append("-I{}".format(self.include_dir))

        cmd.append("--python_out={}".format(self.python_out_dir))
        if self.grpc_out_dir:
            cmd.append("--grpc_python_out={}".format(self.grpc_out_dir))

        cmd.extend(self.sources)

        cmd_shell = "{} {}".format(sys.executable, " ".join(cmd))
        print('Running "{}"'.format(cmd_shell))
        subprocess.check_output(cmd_shell, shell=True)


proto_files = ProtobufFiles(sources=["cpp/proto/arcticc/pb2/*.proto"])


def compile_protos():
    print("\nProtoc compilation")
    proto_files.compile()
    if not os.path.exists("python/arcticc"):
        raise RuntimeError("Unable to locate Protobuf module during compilation.")
    else:
        open("python/arcticc/__init__.py", "a").close()
        open("python/arcticc/pb2/__init__.py", "a").close()


class CompileProto(Command):
    description = '"protoc" generate code _pb2.py from .proto files'

    user_options = []

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        compile_protos()


class CompileProtoAndBuild(build_py):
    def run(self):
        compile_protos()
        build_py.run(self)


class DevelopAndCompileProto(develop):
    def run(self):
        develop.run(self)
        compile_protos()  # compile after updating the deps
        if not os.path.islink("python/arcticdb_ext.so") and os.path.exists("python"):
            print("Creating symlink for compiled arcticdb module in python...")
            os.symlink("../arcticdb_ext.so", "python/arcticdb_ext.so")


class CMakeExtension(Extension):
    def __init__(self, name, sourcedir=""):
        Extension.__init__(self, name, sources=[])
        self.sourcedir = os.path.abspath(sourcedir)


class CMakeBuild(build_ext):
    def run(self):
        for ext in self.extensions:
            self.build_extension(ext)

    def build_extension(self, ext):
        self.compile(self.build_temp)

        so_file = f"{ext.name}.so"
        try:
            os.remove(so_file)
        except:
            pass

        if not osp.exists(so_file):
            extension_file = osp.join(os.path.dirname(self.get_ext_fullpath(ext.name)), so_file)

            print(f"Creating symlink from '{so_file}' to '{extension_file}'")
            os.symlink(extension_file, so_file)

    def compile(self, build_dir):
        env = {
            "TERM": "linux",  # to get colors
            "PATH": "/opt/gcc/8.2.0/bin:/opt/cmake/bin:/usr/bin:/usr/local/bin:/bin:/sbin:/usr/sbin:/usr/local/sbin",
            "CXX": "/opt/gcc/8.2.0/bin/g++",
            "CC": "/opt/gcc/8.2.0/bin/gcc",
            "CMAKE_ROOT": "/opt/cmake/share/cmake-3.12",
        }
        if "MB_PYTHON_PLATFORM_NAME" in os.environ:
            python_root_dir = "/default-pegasus-venv" if args.version != "medusa" else "/opt/man/releases/python-medusa/36-1"
        else:
            python_root_dir = Path(os.path.dirname(sys.executable)).parent

        python_version = "{}.{}".format(sys.version_info[:2][0], sys.version_info[:2][1])
        lib_path = osp.join(python_root_dir, "lib")
        # The include path in python3.8 is different - does not have "m" in the end (lol)
        include_path = osp.join(python_root_dir, "include")
        python_lib_path = osp.join(lib_path, "libpython{}{}.so".format(python_version, "" if packaging.version.parse(python_version) >= packaging.version.parse("3.8") else "m"))

        process_args = [
            "cmake",
            "-DCMAKE_DEPENDS_USE_COMPILER=FALSE",
            "-DPYBIND11_FINDPYTHON=ON",
            "-DENV_BASE_DIR={}".format(python_root_dir),
            "-DPython_ROOT_DIR={}".format(python_root_dir),
            "-DPYTHON_LIBRARIES={}".format(lib_path),
            "-DPYTHON_INCLUDE_DIRS={}".format(include_path),
            "-DBUILD_PYTHON_VERSION={}".format(python_version),
            "-DPYTHON_LIBRARY_SO={}".format(python_lib_path),
            "-G",
            "CodeBlocks - Unix Makefiles",
            "/opt/arcticdb/cpp"
        ]

        if not osp.exists(build_dir):
            os.makedirs(build_dir, mode=0o750)
        cwd = os.getcwd()
        try:
            os.chdir(build_dir)
            subprocess.check_call(process_args, env=env)
            subprocess.check_call(["make", "-j", "{:d}".format(os.cpu_count()), "install"], env=env)
        finally:
            os.chdir(cwd)


if __name__ == "__main__":
    setup(
        ext_modules=[CMakeExtension("arcticdb_ext")],
        package_dir={"": "python"},
        packages=find_packages(where="python", exclude=["tests", "tests.*"]),
        cmdclass=dict(
            build_ext=CMakeBuild,
            protoc=CompileProto,
            build_py=CompileProtoAndBuild,
            bdist_wheel=bdist_wheel,
            develop=DevelopAndCompileProto,
        ),
        zip_safe=False,
    )

import os
import subprocess
import sys
import glob
import platform
import shutil
from setuptools import setup, Command
from setuptools import Extension, find_packages
from setuptools.command.build_ext import build_ext
from setuptools.command.build_py import build_py
from setuptools.command.develop import develop
from wheel.bdist_wheel import bdist_wheel


class CompileProto(Command):
    description = '"protoc" generate code _pb2.py from .proto files'
    user_options = [("grpc_out_dir", None, "See the docs of grpc_tools.protoc")]

    def initialize_options(self):
        self.grpc_out_dir = None

    def finalize_options(self):
        pass

    def run(self):
        print("\nProtoc compilation")
        cmd = [sys.executable, "-mgrpc_tools.protoc", "-Icpp/proto", "--python_out=python"]

        if self.grpc_out_dir:
            cmd.append(f"--grpc_python_out={self.grpc_out_dir}")

        cmd.extend(glob.glob("cpp/proto/arcticc/pb2/*.proto"))

        print(f"Running {cmd}")
        subprocess.check_output(cmd)
        if not os.path.exists("python/arcticc"):
            raise RuntimeError("Unable to locate Protobuf module during compilation.")
        else:
            open("python/arcticc/__init__.py", "a").close()
            open("python/arcticc/pb2/__init__.py", "a").close()


class CompileProtoAndBuild(build_py):
    def run(self):
        self.run_command("protoc")
        build_py.run(self)


class DevelopAndCompileProto(develop):
    def run(self):
        develop.run(self)
        self.run_command("protoc")  # compile after updating the deps


class CMakeExtension(Extension):
    def __init__(self, name, sourcedir=""):
        Extension.__init__(self, name, sources=[])
        self.sourcedir = os.path.abspath(sourcedir)


class CMakeBuild(build_ext):
    def run(self):
        for ext in self.extensions:
            self.build_extension(ext)

    def build_extension(self, ext):
        dest = self.get_ext_fullpath(ext.name)
        print(f"Destination: {dest}")

        env_var = "ARCTIC_CMAKE_PRESET" # From CMakePresets.json
        preset = os.getenv(env_var, "*")
        if preset == "skip":
            return
        search = f"cpp/out/{preset}-build"
        candidates = glob.glob(search)
        if not candidates:
            if preset == "*":
                suffix = "-debug" if self.debug else "-release"
                preset = ("windows-cl" if platform.system() == "Windows" else platform.system().lower()) + suffix
            print(
                f"Did not find build directory with '{search}'. Will configure and build using cmake preset {preset}",
                file=sys.stderr,
            )
            subprocess.check_call(["cmake", "-P", "cpp/CMake/CheckSupportsPreset.cmake"])
            subprocess.check_call(["cmake", "--preset", preset, "-S", "cpp"])
            candidates = glob.glob(search)

        assert len(candidates) == 1, f"Specify {env_var} or use a single build directory. {search}={candidates}"
        subprocess.check_call(["cmake", "--install", candidates[0], "--component", "Python_Lib"])

        source = "cpp/out/install/" + os.path.basename(dest)
        print(f"Moving {source} -> {dest}")
        shutil.move(source, dest)


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

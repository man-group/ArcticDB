import os
import subprocess
import sys
import glob
import platform
import shutil
from pathlib import Path
from tempfile import mkdtemp
from setuptools import setup, Command, find_namespace_packages
from setuptools import Extension, find_packages
from setuptools.command.build_ext import build_ext
from setuptools.command.build_py import build_py
from setuptools.command.develop import develop
from wheel.bdist_wheel import bdist_wheel


class CompileProto(Command):
    # When adding new protobuf versions, also update: setup.cfg, python/arcticdb/__init__.py
    _PROTOBUF_TO_GRPC_VERSION = {"3": "<=1.48.*", "4": ">=1.49"}

    description = '"protoc" generate code _pb2.py from .proto files'
    user_options = [
        ("build-lib=", "b", "Create the arcticdb/proto/*/arcticc/pb2 subdirectories inside this base directory"),
        ("proto-vers=", "s", "Single string listing all the protobuf major versions to compile against"),
    ]

    def initialize_options(self):
        self.build_lib = None
        self.proto_vers = None

    def finalize_options(self):
        self.set_undefined_options("build", ("build_lib", "build_lib"))  # Default to the output of the build command
        if self.proto_vers is None:
            self.proto_vers = os.getenv("ARCTICDB_PROTOC_VERS", "".join(self._PROTOBUF_TO_GRPC_VERSION)).strip()

    def run(self):
        output_dir = os.path.join(self.build_lib, "arcticdb", "proto")
        python = sys.version_info[:2]
        print(f"\nProtoc compilation (into '{output_dir}') for versions '{self.proto_vers}':")
        for proto_ver in self.proto_vers:
            if (python <= (3, 6) and proto_ver >= "4") or (python >= (3, 11) and proto_ver == "3"):
                print(f"Python protobuf {proto_ver} do no run on Python {python}. Skipping...", file=sys.stderr)
            else:
                self._compile_one_version(proto_ver, os.path.join(output_dir, proto_ver))

    def _compile_one_version(self, proto_ver: str, version_output_dir: str):
        grpc_version = self._PROTOBUF_TO_GRPC_VERSION.get(proto_ver, None)
        assert grpc_version, "Supported proto-vers argumets are " + ", ".join(self._PROTOBUF_TO_GRPC_VERSION)

        # Manual virtualenv to avoid hard-coding Man internal locations:
        pythonpath = mkdtemp()
        subprocess.check_call(
            [
                sys.executable,
                "-mpip",
                "install",
                "--disable-pip-version-check",
                "--target=" + pythonpath,
                "grpcio-tools" + grpc_version,
            ]
        )
        env = {**os.environ, "PYTHONPATH": pythonpath}

        # Check the transitively install protobuf version:
        cmd = [sys.executable, "-c", "import google.protobuf ; print(google.protobuf.__version__)"]
        installed_proto_ver = subprocess.check_output(cmd, env=env, universal_newlines=True)
        match = installed_proto_ver.startswith(proto_ver + ".")
        assert match, f"grpc{grpc_version} installed protobuf=={installed_proto_ver} but we expect {proto_ver}"

        # Compile
        os.makedirs(version_output_dir, exist_ok=True)
        cmd = [sys.executable, "-mgrpc_tools.protoc", "-Icpp/proto", "--python_out=" + version_output_dir]
        cmd.extend(glob.glob(os.path.normpath("cpp/proto/arcticc/pb2/*.proto")))
        print("Running " + " ".join(cmd))
        installed_proto_ver = subprocess.check_call(cmd, env=env)
        open(version_output_dir + "/arcticc/__init__.py", "a").close()
        open(version_output_dir + "/arcticc/pb2/__init__.py", "a").close()

        shutil.rmtree(pythonpath)


class CompileProtoAndBuild(build_py):
    def run(self):
        self.run_command("protoc")
        build_py.run(self)


class DevelopAndCompileProto(develop):
    def install_for_development(self):
        super().install_for_development()
        self.reinitialize_command("protoc", build_lib=self.egg_base)
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
        dest = os.path.abspath(self.get_ext_fullpath(ext.name))
        print(f"Destination: {dest}")
        install_args = [f"-DCMAKE_INSTALL_PREFIX={os.path.dirname(dest)}"]

        cmake = shutil.which("cmake")  # Windows safe
        if not cmake and platform.system() != "Windows":
            print("Did not find cmake on the path. Will try to resolve from shell", file=sys.stderr)
            cmake = subprocess.check_output(["bash", "-lc", "which cmake | tail -n 1"], universal_newlines=True).strip()

        env_var = "ARCTIC_CMAKE_PRESET"  # From CMakePresets.json
        preset = os.getenv(env_var, "*")
        if preset == "skip":
            return
        search = f"cpp/out/{preset}-build"
        candidates = glob.glob(search)
        if not candidates:
            common_args = [
                "-DTEST=NO",
                f"-DBUILD_PYTHON_VERSION={sys.version_info[0]}.{sys.version_info[1]}",
                *install_args,
            ]
            cmake_check = subprocess.run([cmake, "-P", "cpp/CMake/CheckSupportsPreset.cmake"])
            if cmake_check.returncode == 0:
                self._configure_cmake_using_preset(cmake, common_args, preset, search)
            else:
                self._configure_for_legacy_image(common_args, "cpp/out/legacy-build")
            candidates = glob.glob(search)

        assert len(candidates) == 1, f"Specify {env_var} or use a single build directory. {search}={candidates}"
        subprocess.check_call([cmake, "--build", candidates[0], "--target", ext.name])
        subprocess.check_call(
            [cmake, *install_args, "-DCOMPONENT=Python_Lib", "-P", candidates[0] + "/cmake_install.cmake"]
        )

        assert os.path.exists(dest), f"No output at {dest}, but we didn't get a bad return code from CMake?"

    def _configure_cmake_using_preset(self, cmake, common_args, preset, search):
        if preset == "*":
            suffix = "-debug" if self.debug else "-release"
            preset = ("windows-cl" if platform.system() == "Windows" else platform.system().lower()) + suffix
        print(
            f"Did not find build directory with '{search}'. Will configure and build using cmake preset {preset}",
            file=sys.stderr,
        )
        subprocess.check_call([cmake, *common_args, "--preset", preset], cwd="cpp")

    def _configure_for_legacy_image(self, common_args, build_dir):
        print("Legacy cmake configuration")
        env = {
            "TERM": "linux",  # to get colors
            "PATH": "/opt/gcc/8.2.0/bin:/opt/cmake/bin:/usr/bin:/usr/local/bin:/bin:/sbin:/usr/sbin:/usr/local/sbin",
            "CXX": "/opt/gcc/8.2.0/bin/g++",
            "CC": "/opt/gcc/8.2.0/bin/gcc",
        }
        if "MB_PYTHON_PLATFORM_NAME" in os.environ:
            python_root_dir = "/default-pegasus-venv"
        else:
            python_root_dir = Path(os.path.dirname(sys.executable)).parent

        # If the cmake don't support presets, it also won't support FindPython Development.Module

        process_args = [
            "cmake",
            f"-DPython_ROOT_DIR={python_root_dir}",
            *common_args,
            "-G",
            "CodeBlocks - Unix Makefiles",
            os.path.join(os.getcwd(), "cpp"),
        ]

        if not os.path.exists(build_dir):
            os.makedirs(build_dir, mode=0o755)
        subprocess.check_call(process_args, env=env, cwd=build_dir)  # No shell=True here because cmake is in env.PATH


if __name__ == "__main__":
    setup(
        ext_modules=[CMakeExtension("arcticdb_ext")],
        package_dir={"": "python"},
        packages=find_packages(where="python", exclude=["tests", "tests.*"])
        + find_namespace_packages(where="python", include=["arcticdb.proto.*"]),
        cmdclass=dict(
            build_ext=CMakeBuild,
            protoc=CompileProto,
            build_py=CompileProtoAndBuild,
            bdist_wheel=bdist_wheel,
            develop=DevelopAndCompileProto,
        ),
        zip_safe=False,
    )

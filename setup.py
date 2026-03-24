import os
import subprocess
import sys
import glob
import platform
import shutil
import re
from tempfile import mkdtemp
from pathlib import Path
from setuptools import setup, Command, find_namespace_packages
from setuptools import Extension, find_packages
from setuptools.command.build_ext import build_ext
from setuptools.command.build_py import build_py
from setuptools.command.develop import develop
from wheel.bdist_wheel import bdist_wheel

# experimental flag to indicate that we want
# the dependencies from a conda
ARCTICDB_USING_CONDA = os.environ.get("ARCTICDB_USING_CONDA", "0")
ARCTICDB_USING_CONDA = ARCTICDB_USING_CONDA != "0"
print(f"ARCTICDB_USING_CONDA={ARCTICDB_USING_CONDA}")

# flag to indicate if we want to build the tests
ARCTICDB_BUILD_CPP_TESTS = os.environ.get("ARCTICDB_BUILD_CPP_TESTS", "0")
ARCTICDB_BUILD_CPP_TESTS = ARCTICDB_BUILD_CPP_TESTS != "0"
print(f"ARCTICDB_BUILD_CPP_TESTS={ARCTICDB_BUILD_CPP_TESTS}")


def _log_and_run(*cmd, **kwargs):
    print("Running " + " ".join(cmd))
    subprocess.check_call(cmd, **kwargs)


def cleanup_vcpkg_artifacts():
    if os.environ.get("ARCTICDB_KEEP_VCPKG_SOURCES", "0") != "0":
        print("ARCTICDB_KEEP_VCPKG_SOURCES is set, skipping vcpkg cleanup")
        return

    cpp_dir = Path("cpp")
    if not cpp_dir.exists():
        return

    vcpkg_dir = cpp_dir / "vcpkg"
    if not vcpkg_dir.exists():
        return

    def safe_remove_directory(dir_path):
        # To handle Windows symbolic links
        if not dir_path.exists():
            print(f"Directory not found (skipping): {dir_path}")
            return

        try:
            if os.path.islink(str(dir_path)):
                target = os.readlink(str(dir_path))
                if os.path.isabs(target):
                    target_path = Path(target)
                else:
                    target_path = dir_path.parent / target

                os.unlink(str(dir_path))
                print(f"Removed symbolic link: {dir_path}")

                if target_path.exists() and target_path.is_dir():
                    shutil.rmtree(str(target_path))
                    print(f"Removed target directory: {target_path}")
            else:
                shutil.rmtree(str(dir_path))
                print(f"Removed: {dir_path}")
        except Exception as e:
            print(f"Warning: Could not remove {dir_path}: {e}")

    safe_remove_directory(vcpkg_dir / "buildtrees")
    safe_remove_directory(vcpkg_dir / "downloads")
    safe_remove_directory(vcpkg_dir / "packages")


class CompileProto(Command):
    # When adding new protobuf versions, also update: setup.cfg, python/arcticdb/__init__.py
    _PROTOBUF_TO_GRPC_VERSION = {"3": "<1.31", "4": ">=1.49", "5": ">=1.68.1", "6": ">=1.73.0"}

    description = '"protoc" generate code _pb2.py from .proto files'
    user_options = [
        ("build-lib=", "b", "Create the arcticdb/proto/*/arcticc/pb2 subdirectories inside this base directory"),
        ("proto-vers=", "s", "Single string listing all the protobuf major versions to compile against"),
    ]

    def initialize_options(self):
        self.build_lib = None
        self.proto_vers = None

    def finalize_options(self):
        print(self.distribution.commands)
        if not self.build_lib:
            if "editable_wheel" in self.distribution.commands:
                self.build_lib = "python"
            else:
                # Default to the output location of the build command:
                self.set_undefined_options("build", ("build_lib", "build_lib"))
        if self.proto_vers is None:
            self.proto_vers = os.getenv("ARCTICDB_PROTOC_VERS", "".join(self._PROTOBUF_TO_GRPC_VERSION)).strip()

    def run(self):
        output_dir = os.path.join(self.build_lib, "arcticdb", "proto")
        python = sys.version_info[:2]
        print(f"\nProtoc compilation (into '{output_dir}') for versions '{self.proto_vers}':")
        for proto_ver in self.proto_vers:
            if not ARCTICDB_USING_CONDA and python >= (3, 11) and proto_ver == "3":
                # No available on PyPI for this configuration.
                # See the last release's: https://pypi.org/project/protobuf/3.20.3/#files
                print(f"Python protobuf {proto_ver} is not officially supported on Python {python}. Skipping...")
            elif not ARCTICDB_USING_CONDA and python >= (3, 13) and proto_ver < "5":
                # Not compatible with protobuf<5.26.1 as
                # Python 3.13 requires GRPCIO >= 1.66.2, which requires protobuf >=5.26.1 https://github.com/grpc/grpc/blob/6fa8043bf9befb070b846993b59a3348248e6566/requirements.txt#L4
                print(f"Python protobuf {proto_ver} does not run on Python {python}. Skipping...")
            elif not ARCTICDB_USING_CONDA and python >= (3, 14) and proto_ver < "6":
                # Not compatible with protobuf<6.31.1 as
                # Python 3.14 requires GRPCIO >= 1.75.1, which requires protobuf >=6.31.1 https://github.com/grpc/grpc/blob/v1.75.1/requirements.txt
                print(f"Python protobuf {proto_ver} does not run on Python {python}. Skipping...")
            elif not ARCTICDB_USING_CONDA and python <= (3, 8) and proto_ver == "6":
                # Python 3.8 is not compatible with protobuf 6
                print(f"Python protobuf {proto_ver} does not run on Python {python}. Skipping...")
            else:
                self._compile_one_version(proto_ver, os.path.join(output_dir, proto_ver))

    def _compile_one_version(self, proto_ver: str, version_output_dir: str):
        grpc_version = self._PROTOBUF_TO_GRPC_VERSION.get(proto_ver, None)
        assert grpc_version, "Supported proto-vers arguments are " + ", ".join(self._PROTOBUF_TO_GRPC_VERSION)

        # Manual virtualenv to avoid hard-coding Man internal locations
        pythonpath = mkdtemp()
        if not ARCTICDB_USING_CONDA:
            # Python protobuf 3 and 4 are incompatible and we do not want to dictate which version of protobuf
            # the user can have, so we compile the Python binding files with both versions and dynamically load
            # the correct version at run time.
            packages = ["grpcio-tools" + grpc_version, f"protobuf=={proto_ver}.*"]
            if proto_ver == "3":
                # grpcio-tools<1.31 has no pre-built wheels for py3.9+, so pip builds 
                # from tarball. Its setup.py uses pkg_resources, which was 
                # removed in setuptools>=82.
                # PIP_CONSTRAINT pins setuptools in pip's build isolation env.
                # where build constraints aren't propagated).
                constraints = os.path.join(pythonpath, "constraints.txt")
                with open(constraints, "w") as f:
                    f.write("setuptools<82\n")
                install_env = {**os.environ, "PIP_CONSTRAINT": constraints}
                packages.append("setuptools<82")
                # --use-pep517 to force parameters being passed to sub-build pipeline
                # CI still uses pip 25.1.1, which the option is OFF by default
                # It will be on by default in pip 25.3 anyway
                extra_pip_args = ["--use-pep517"]
            else:
                install_env = {**os.environ}
                extra_pip_args = []

            _log_and_run(
                sys.executable,
                "-mpip",
                "install",
                "--disable-pip-version-check",
                "--target=" + pythonpath,
                *extra_pip_args,
                *packages,
                env=install_env,
            )
            env = {**os.environ, "PYTHONPATH": pythonpath, "PYTHONNOUSERSITE": "1"}
        else:
            # grpcio-tools is already installed in the conda environment (see environment.yml)
            env = {**os.environ}

        # Compile
        os.makedirs(version_output_dir, exist_ok=True)
        cmd = [sys.executable, "-mgrpc_tools.protoc", "-Icpp/proto", "--python_out=" + version_output_dir]
        _log_and_run(*cmd, *glob.glob(os.path.normpath("cpp/proto/arcticc/pb2/*.proto")), env=env)

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

        cmake = shutil.which("cmake")  # Windows safe
        if not cmake and platform.system() != "Windows":
            print("Did not find cmake on the path. Will try to resolve from shell", file=sys.stderr)
            cmake = subprocess.check_output(["bash", "-lc", "which cmake | tail -n 1"], universal_newlines=True).strip()

        env_var = "ARCTIC_CMAKE_PRESET"  # From CMakePresets.json
        preset = os.getenv(env_var, "*")
        if preset == "skip":
            return
        if preset == "*":
            conda_suffix = "-conda" if ARCTICDB_USING_CONDA else ""
            suffix = "-debug" if self.debug else "-release"
            suffix = conda_suffix + suffix
            system_name = platform.system()
            if system_name == "Windows":
                preset = "windows-cl" + suffix
            elif system_name == "Darwin":
                preset = "macos" + suffix
            else:
                preset = system_name.lower() + suffix

        cmd = [
            cmake,
            f"-DTEST={ARCTICDB_BUILD_CPP_TESTS}",
            f"-DBUILD_PYTHON_VERSION={sys.version_info[0]}.{sys.version_info[1]}",
            f"-DCMAKE_INSTALL_PREFIX={os.path.dirname(dest)}",
            "--preset",
            preset,
        ]
        vcpkg_installed_dir = os.getenv("ARCTICDB_VCPKG_INSTALLED_DIR")
        print(f"ARCTICDB_VCPKG_INSTALLED_DIR={vcpkg_installed_dir}")
        if vcpkg_installed_dir:
            cmd.append(f"-DVCPKG_INSTALLED_DIR={vcpkg_installed_dir}")

        _log_and_run(*cmd, cwd="cpp")

        cleanup_vcpkg_artifacts()

        search = f"cpp/out/{preset}-build"
        candidates = glob.glob(search)
        assert len(candidates) == 1, f"Specify {env_var} or use a single build directory. {search}={candidates}"

        cmake_build_parallel_level = os.getenv("CMAKE_BUILD_PARALLEL_LEVEL", None)
        if not cmake_build_parallel_level:
            try:
                # Python API is not cgroups-aware yet, so use CMake:
                cpu_output = subprocess.check_output([cmake, "-P", "cpp/CMake/CpuCount.cmake"], universal_newlines=True)
                jobs = "-j", cpu_output.replace("-- CMAKE_BUILD_PARALLEL_LEVEL=", "").rstrip()
            except Exception as e:
                print("Failed to retrieve CPU count:", e)
                jobs = ()
        else:
            jobs = "-j", cmake_build_parallel_level

        try:
            _log_and_run(cmake, "--build", candidates[0], *jobs, "--target", "install_" + ext.name)
        finally:
            launcher = os.getenv("CMAKE_CXX_COMPILER_LAUNCHER", "")
            if "sccache" in launcher and "AUDITWHEEL_PLAT" in os.environ:
                subprocess.run([launcher, "--show-stats"])

        assert os.path.exists(dest), f"No output at {dest}, but we didn't get a bad return code from CMake?"


def readme():
    github_emoji = re.compile(r":[a-z_]+:")
    with open("README.md", encoding="utf-8") as f:
        return github_emoji.sub("", f.read())


if __name__ == "__main__":
    setup(
        ext_modules=[CMakeExtension("arcticdb_ext")],
        package_dir={"": "python"},
        packages=find_packages(where="python", exclude=["tests", "tests.*"])
        + find_namespace_packages(where="python", include=["arcticdb.proto.*"]),
        long_description=readme(),
        long_description_content_type="text/markdown",
        cmdclass=dict(
            build_ext=CMakeBuild,
            protoc=CompileProto,
            build_py=CompileProtoAndBuild,
            bdist_wheel=bdist_wheel,
            develop=DevelopAndCompileProto,
        ),
        zip_safe=False,
    )

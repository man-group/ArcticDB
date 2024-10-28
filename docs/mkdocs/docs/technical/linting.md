## Linting Tools

We use Black for Python and clang-format for C++.

The `.clang-format` file in the root of this repo is based on LLVM style with some minor tweaks.

### Running Linters

Activate a Python3 virtual environment and run the following from the project root to
install the linters:

```
python build_tooling/format.py --install-tools  # install the linters
```

Then check your formatting:

```
python build_tooling/format.py --check --type python
python build_tooling/format.py --check --type cpp
```

To reformat your working copy, run:

```
python build_tooling/format.py --in-place --type python
python build_tooling/format.py --in-place --type cpp

# Or just do everything at once,
python build_tooling/format.py --in-place --type all
```

#### CLion Integration

#### C++

Our formatting rules should be detected automatically, so you can use the CLion formatter on C++. See
[here](https://clang.llvm.org/docs/ClangFormat.html#clion-integration).

Just select "Enable ClangFormat" in Settings | Editor | Code Style.

Documentation on formatting in CLion is [here](https://www.jetbrains.com/help/clion/reformat-and-rearrange-code.html).

#### Python

Add Black as an external tool. After installing it with the steps above, run:

`which black`.

For me, this shows:

```
/home/alex/venvs/310/bin/black
```

Then follow the steps [here](https://black.readthedocs.io/en/stable/integrations/editors.html#as-external-tool) to add
that Black binary.

### CI

We run the formatting checks as the first step in `analysis_workflow.yml`, on an Ubuntu host, in a Python 3.10 venv.

If your checks pass locally then they should pass on the CI.

In the past we have had issues with compatibility of the linters across operating systems and Python versions, so for
the most stable results run the linting checks in Linux (eg WSL) in a Python 3.10 venv.

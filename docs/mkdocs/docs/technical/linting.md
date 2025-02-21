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

### Rebasing Old Work

You may have old work that isn't auto-formatted that you need to rebase and merge. Steps taken from [this blog](https://blog.scottlogic.com/2019/03/04/retroactively-applying-prettier-to-existing-branches.html)
can help.

First push a copy of your branch somewhere so you have a backup in case this goes horribly wrong.

Suppose that `master` has the formatting enforced.

With your feature branch checked out, rebase to just before the auto-formatting and squash commits:

```
git rebase -i --onto <code formatting commit SHA>~1 origin/master
# Now squash everything in to one commit
```

You will need to fix any genuine conflicts at this point.

Now rebase, applying the formatter. Run this from a venv with the linters installed. The "theirs" strategy means to
prefer the feature branch.

```
git rebase \
  --strategy-option=theirs \
  --exec '`which python` ./build_tooling/format.py --type all --in-place && git add cpp/ python/ && git commit --amend --no-edit' \
  --onto <code formatting commit SHA> origin/master
```

You should now have a single commit with the correct formatting, that you can then rebase as normal on to master,

```
git rebase origin/master
```

and resolve any genuine conflicts as usual.

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

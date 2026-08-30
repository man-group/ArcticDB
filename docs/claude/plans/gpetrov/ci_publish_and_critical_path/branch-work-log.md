# Branch work log: gpetrov/ci_publish_and_critical_path

Independent of the CI stack; branched straight off master. Tooling only - no test or production code.

## The publish job was slow and silently lossy

It took 21 minutes and processed 29 of 113 artifacts, so most test timings never reached the database - which
defeats the point of keeping the history. Two bugs: the artifact list was fetched without pagination, so only the
first page existed as far as the job was concerned, and the downloads were serial. Now paginated and pulled
through a thread pool. A third bug appeared once it worked: the job publishes the run it is part of, and that run
is not `completed` while it is still running, so the status check rejected it. 21 min -> 1 min, all 113
artifacts, verified locally by simulating the Actions environment (92 XML files in 21.7 s).

## Where the time actually goes

Wall time is not the sum of the work - with runners to spare it is the longest chain of jobs - and that was not
visible from the run page. `build_tooling/ci_critical_path.py` appends to `$GITHUB_STEP_SUMMARY`: when the run
became merge-ready, the last jobs to finish, a gantt of the tail, and "without X the run would end at Y". It also
runs standalone against any run id, which is how the numbers in this stack were measured.

The job is `continue-on-error`, so a reporting failure can never fail a build.

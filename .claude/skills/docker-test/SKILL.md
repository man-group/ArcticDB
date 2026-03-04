---
name: docker-test
description: Run commands or tests inside the arcticdb Docker container
argument-hint: "[command]"
---

Run commands or tests inside the arcticdb Docker container.

## Usage

- `/docker-test` — start the container if not running, then open an interactive prompt for what to run
- `/docker-test pytest python/tests/unit/some_test.py` — run a specific test
- `/docker-test python -c "import arcticdb"` — run arbitrary command
- `/docker-test --name mydev pytest ...` — use a named container `arc-dev-mydev` (reusable across sessions)
- `/docker-test --name mydev` — reconnect to an existing named container

## Instructions

Image name: `arcticdb-env`
Dockerfile: `docker/Dockerfile`

### 0. Determine the container name

First, check if `$ARGUMENTS` starts with `--name <name>`. If so:
- Use `arc-dev-<name>` as the container name.
- Strip `--name <name>` from `$ARGUMENTS` before executing in step 3.

If no `--name` was provided, generate a random name on **first invocation only**:
```bash
_ARC_CONTAINER="arc-dev-$(head -c4 /dev/urandom | xxd -p)"
```

**Important**: Once determined, reuse the same container name for all subsequent `/docker-test` invocations in this conversation. Do NOT re-derive.

Named containers (`--name`) survive across sessions so the user can reconnect later. Random containers are ephemeral.

### 1. Ensure the image is built

Check if the image exists:
```bash
docker image inspect arcticdb-env >/dev/null 2>&1
```

If it doesn't exist, build it from the repo root:
```bash
docker build -f docker/Dockerfile -t arcticdb-env .
```

### 2. Ensure the container is running

Check the container state with a single command:
```bash
docker inspect --format '{{.State.Status}}' "$_ARC_CONTAINER" 2>/dev/null
```

- If the command **fails** (container does not exist), create it:
  ```bash
  docker run -d --name "$_ARC_CONTAINER" \
    -v "$(git rev-parse --show-toplevel)":/workspace \
    -w /workspace \
    arcticdb-env sleep infinity
  ```

- If it returns **`exited`** or **`created`**, start it:
  ```bash
  docker start "$_ARC_CONTAINER"
  ```

- If it returns **`running`**, nothing to do.

### 3. Execute the command

Run the user's command inside the container. For simple commands pass them directly:
```bash
docker exec -w /workspace "$_ARC_CONTAINER" $ARGUMENTS
```

If the command uses shell operators (pipes `|`, redirects `>`, `&&`, etc.), wrap it with `bash -c`:
```bash
docker exec -w /workspace "$_ARC_CONTAINER" bash -c "$ARGUMENTS"
```
When using `bash -c`, escape any inner quotes appropriately.

If no command was provided, ask the user what they want to run.

### 4. Show results

Display the command output. If the command fails, help debug as usual.

### 5. Cleanup

When the user explicitly asks to clean up, remove the current session's container:
```bash
docker rm -f "$_ARC_CONTAINER"
```

To list all arc-dev containers from this skill:
```bash
docker ps -a --filter "name=^arc-dev-" --format "table {{.Names}}\t{{.Status}}"
```

To prune all **stopped** arc-dev containers (only do this if the user asks):
```bash
docker container ls -a --filter "name=^arc-dev-" --filter "status=exited" -q | xargs -r docker rm
```
Warn the user before pruning, as this will remove named containers they may want to reuse.

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

## Instructions

Container name: `arc-dev`
Image name: `arcticdb-env`
Dockerfile: `docker/Dockerfile`

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

Check if the container is running:
```bash
docker ps --filter name=arc-dev --format '{{.Names}}' | grep -q arc-dev
```

If not running, start it with the repo mounted:
```bash
docker run -d --name arc-dev -v "$(pwd)":/workspace -w /workspace arcticdb-env sleep infinity
```

If the container exists but is stopped, start it:
```bash
docker start arc-dev
```

### 3. Execute the command

Run the user's command inside the container:
```bash
docker exec -w /workspace arc-dev $ARGUMENTS
```

If no command was provided, ask the user what they want to run.

### 4. Show results

Display the command output. If the command fails, help debug as usual.

# Makefile for Tram Delay Reduction Management
# NOTE: Do not use chmod 777 or 666.
# Use proper ownership (chown) and safe permissions instead (chmod 755).

# Image names
BASE_IMAGE = tram-base:latest
INGEST_IMAGE = tram-ingest:latest
INGEST_REALTIME_IMAGE = tram-ingest-realtime:latest
SIM_IMAGE = tram-sim:latest
TRAIN_IMAGE = tram-train:latest
REALTIME_INTERVAL ?= 20

# Container runtime configuration (override with CONTAINER_RUNTIME=podman etc.)
CONTAINER_RUNTIME ?= docker
COMPOSE_CMD ?= $(CONTAINER_RUNTIME) compose

# Docker Compose configuration
COMPOSE_FILE = docker/docker-compose.yml
# Optional additional compose file (e.g. docker/docker-compose.dev.yml)
COMPOSE_FILE_DEV ?= docker/docker-compose.dev.yml

# Compose file flags (use multiple -f entries if needed)
COMPOSE_FILES ?= -f $(COMPOSE_FILE)

# Project and runtime flags
COMPOSE_PROJECT_NAME ?= headway
DETACH ?= -d
SERVICES ?= dev
LOGS_TAIL ?= 100

# Build flag for `docker compose up` (set to "--build" when you need to rebuild images)
# Example: `make jupyter-lab BUILD=--build`
BUILD ?=

# Host command to open VS Code (override if not available)
CODE_CMD ?= code
# Host command to open a browser (override if not available). Default to Chrome.
# On some systems the binary may be `google-chrome-stable` or `google-chrome`.
BROWSER_CMD ?= google-chrome


# Build targets
.PHONY: build-base build-ingest build-ingest-realtime build-sim build-train build-all
.PHONY: run-ingest-static
.PHONY: compose-ingest-realtime compose-ingest-realtime-loop compose-ingest-realtime-raw stop-realtime-loop
.PHONY: compose-sumo-tutorial compose-sim compose-train
.PHONY: clean help
.PHONY: vscode jupyter-lab dev-logs compose-stop compose-down


# Build base image (heavy dependencies once)
build-base:
	$(CONTAINER_RUNTIME) build --build-arg APP_UID=1000 --build-arg APP_GID=1000 -f docker/Dockerfile.base -t $(BASE_IMAGE) .

# Build job-specific images (lightweight & fast)
build-ingest: build-base
	$(CONTAINER_RUNTIME) build --build-arg APP_UID=1000 --build-arg APP_GID=1000 -f docker/Dockerfile.ingest -t $(INGEST_IMAGE) .

build-ingest-realtime: build-base
	$(CONTAINER_RUNTIME) build --build-arg APP_UID=1000 --build-arg APP_GID=1000 -f docker/Dockerfile.ingest-realtime -t $(INGEST_REALTIME_IMAGE) .

build-sim: build-base
	$(CONTAINER_RUNTIME) build -f docker/Dockerfile.sim -t $(SIM_IMAGE) .

build-train: build-base
	$(CONTAINER_RUNTIME) build -f docker/Dockerfile.train -t $(TRAIN_IMAGE) .

# Build all images
build-all: build-ingest build-ingest-realtime build-sim build-train

# Run GTFS static data ingestion once (cleans previous snapshots)
run-ingest-static:
	CONTAINER_RUNTIME=$(CONTAINER_RUNTIME) COMPOSE_CMD="$(COMPOSE_CMD)" ./scripts/ingest_static_once.sh

# Run with Docker Compose (short-lived tasks)
compose-ingest-realtime:
	$(COMPOSE_CMD) -f $(COMPOSE_FILE) run --rm gtfs-ingest-realtime

compose-ingest-realtime-loop:
	$(COMPOSE_CMD) -f $(COMPOSE_FILE) run --rm gtfs-ingest-realtime --feed-type realtime --interval $(REALTIME_INTERVAL) 

stop-realtime-loop:
	@echo "🛑 Stopping GTFS-RT realtime loop containers..."
	@docker ps -q --filter "name=gtfs-ingest-realtime" | xargs -r docker stop
	@echo "✅ All GTFS-RT realtime loop containers stopped."


compose-ingest-realtime-raw:
	GTFS_RT_SAVE_PROTO=1 GTFS_STATIC_SAVE_ZIP=1 $(COMPOSE_CMD) -f $(COMPOSE_FILE) run --rm gtfs-ingest-realtime

compose-sumo-tutorial:
	$(COMPOSE_CMD) -f $(COMPOSE_FILE) up -d --build SUMO-tutorial

compose-sim:
	$(COMPOSE_CMD) -f $(COMPOSE_FILE) run --rm simulation

compose-train:
	$(COMPOSE_CMD) -f $(COMPOSE_FILE) run --rm training

# Open VS Code after starting compose in background
# Usage examples:
#  make vscode                    # start compose (background) and run `code .`
#  make COMPOSE_FILES="-f docker/docker-compose.yml -f docker/docker-compose.dev.yml" vscode


vscode:
	@echo "⏳ Starting docker compose in background and opening VS Code..."
	$(COMPOSE_CMD) $(COMPOSE_FILES) up $(DETACH) $(BUILD) $(SERVICES)
	@# small delay to let containers start (tweak if necessary)
	@sleep 1
	@$(CODE_CMD) . || echo "⚠️ '$(CODE_CMD)' failed - running on headless host?"

# Start the jupyter-lab service and open JupyterLab in the host browser
# Usage: make jupyter-lab
#        make COMPOSE_FILES="-f docker/docker-compose.yml -f docker/docker-compose.dev.yml" jupyter-lab


jupyter-lab:
	@echo "🔄 Starting docker compose (no specific jupyter service required)..."
	$(COMPOSE_CMD) $(COMPOSE_FILES) up $(DETACH) $(BUILD) $(SERVICES)
	@# Give services a moment to start
	@sleep 1
	@URL="http://localhost:8888/lab"; \
	echo "➡️ Opening JupyterLab at $$URL"; \
	$(BROWSER_CMD) "$$URL" || echo "⚠️ Could not open browser. Access JupyterLab at $$URL"

# Stop running services (does not remove containers)
compose-stop:
	@echo "🛑 Stopping docker compose services..."
	$(COMPOSE_CMD) $(COMPOSE_FILES) stop $(SERVICES)

# Stop and remove containers, networks, volumes created by up
compose-down:
	@echo "🧹 Bringing docker compose down (removing containers/networks)..."
	$(COMPOSE_CMD) $(COMPOSE_FILES) down $(SERVICES)

# Real-time scheduler (short-lived tasks)
scheduler-realtime:
	CONTAINER_RUNTIME=$(CONTAINER_RUNTIME) COMPOSE_CMD="$(COMPOSE_CMD)" ./scripts/scheduler-realtime.sh

scheduler-realtime-once:
	CONTAINER_RUNTIME=$(CONTAINER_RUNTIME) COMPOSE_CMD="$(COMPOSE_CMD)" ./scripts/scheduler-realtime.sh --once

# Cron-based real-time data collection
cron-setup:
	CONTAINER_RUNTIME=$(CONTAINER_RUNTIME) COMPOSE_CMD="$(COMPOSE_CMD)" ./scripts/setup-cron.sh setup

cron-remove:
	CONTAINER_RUNTIME=$(CONTAINER_RUNTIME) COMPOSE_CMD="$(COMPOSE_CMD)" ./scripts/setup-cron.sh remove

cron-show:
	./scripts/setup-cron.sh show

# Cleanup
clean:
	$(CONTAINER_RUNTIME) rmi $(BASE_IMAGE) $(INGEST_IMAGE) $(INGEST_REALTIME_IMAGE) $(SIM_IMAGE) $(TRAIN_IMAGE) 2>/dev/null || true
	$(CONTAINER_RUNTIME) system prune -f

# Help
help:
	@echo "Available targets:"
	@echo "  build-base   - Build base image (heavy dependencies)"
	@echo "  build-ingest - Build GTFS Static ingestion image (short-lived task)"
	@echo "  build-ingest-realtime - Build GTFS-RT real-time ingestion image (continuous)"
	@echo "  build-sim    - Build simulation image"
	@echo "  build-train  - Build training image"
	@echo "  build-all    - Build all images"
	@echo "  run-ingest-static - Run GTFS static ingestion once (ensures a single snapshot)"
	@echo "  compose-ingest-realtime - Run GTFS-RT real-time ingestion with compose (single execution)"
	@echo "  compose-ingest-realtime-loop - Run continuous GTFS-RT ingestion with compose"
	@echo "  stop-realtime-loop - for cron configuration"
	@echo "  compose-ingest-realtime-raw - Same as above with raw protobuf/ZIP archiving enabled"
	@echo "  compose-sumo-tutorial - Run SUMO tutorial via compose with GUI"
	@echo "  compose-sim  - Run simulation with compose"
	@echo "  compose-train - Run training with compose"
	@echo "  vscode       - Start compose (dev) and open VS Code"
	@echo "  jupyter-lab  - Start compose (dev) and open JupyterLab in browser"
	@echo "  compose-stop - Stop specified services (uses $(SERVICES))"
	@echo "  compose-down - Stop and remove containers/networks created by compose"
	@echo "  scheduler-realtime - Run real-time scheduler (RT data every 20s)"
	@echo "  scheduler-realtime-once - Run real-time scheduler once"
	@echo "  cron-setup   - Setup system cron for real-time data collection"
	@echo "  cron-remove  - Remove system cron jobs"
	@echo "  cron-show    - Show current cron jobs"
	@echo "  clean        - Clean up images"
	@echo "  help         - Show this help"

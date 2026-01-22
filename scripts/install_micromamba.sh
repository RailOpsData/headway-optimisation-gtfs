#!/bin/bash
set -e

# Install micromamba into scripts/bin and initialize shell integration.
DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$DIR/.." && pwd)"

echo "Installing micromamba (1–2 min)..."

# Ensure target bin exists in repo scripts
mkdir -p "$DIR/bin"

# Download and extract micromamba binary into scripts/bin
curl -Ls https://micro.mamba.pm/api/micromamba/linux-64/latest | tar -xvj -C "$DIR" bin/micromamba

# Initialize shell integration for bash with custom root prefix
"$DIR/bin/micromamba" shell init --shell bash --root-prefix ~/micromamba

# Optional: create project environment from environment.yml in repo
# micromamba env create -f "$PROJECT_ROOT/requirements/environment.yml"

echo "Installation complete. Run a new shell or source your shell rc to use micromamba."
#!/bin/bash
# install_micromamba.sh - Install Micromamba and create gtfs-sumo-rl environment

set -e  # Exit immediately on error

echo "Installing micromamba (1–2 min)..."

# 1. Download and extract micromamba binary
curl -Ls https://micro.mamba.pm/api/micromamba/linux-64/latest | tar -xvj bin/micromamba

# 2. Initialize shell integration for bash with custom root prefix
./bin/micromamba shell init --shell bash --root-prefix ~/micromamba

# # 3. Reload shell configuration to enable micromamba in this session
# source ~/.bashrc

# # 4. Create project environment from environment.yml
# echo "Creating gtfs-sumo-rl environment..."
# micromamba env create -f ~/adaptive-signal-open-data/requirements/environment.yml

# # 5. Activate environment and run a basic check
# micromamba activate gtfs-sumo-rl
# micromamba list | grep -E 'pandas|numpy' || echo "Warning: core packages not found in environment."

# echo "Installation complete."
# echo "Next steps:"
# echo "  micromamba activate gtfs-sumo-rl"
# echo "  jupyter lab  # start development"


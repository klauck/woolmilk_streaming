#!/bin/bash
#with help of llm

set -e

echo "Building for Raspberry Pi using zigbuild..."
TARGET=${1:-aarch64-unknown-linux-gnu}

echo "Target: $TARGET"

find_project_root() {
    local current_dir="$(pwd)"
    while [[ "$current_dir" != "/" ]]; do
        if [[ -f "$current_dir/Cargo.toml" ]]; then
            echo "$current_dir"
            return 0
        fi
        current_dir="$(dirname "$current_dir")"
    done
    echo "Error: Could not find Cargo.toml. Are you in a Rust project?" >&2
    exit 1
}

PROJECT_ROOT=$(find_project_root)
echo "Project root: $PROJECT_ROOT"

# Check if zig is installed
if ! command -v zig &> /dev/null; then
    echo "Installing zig..."
    brew install zig
fi

# Check if cargo-zigbuild is installed
if ! command -v cargo-zigbuild &> /dev/null; then
    echo "Installing cargo-zigbuild..."
    cargo install cargo-zigbuild
fi

# Install target if not already installed
if ! rustup target list --installed | grep -q "$TARGET"; then
    echo "Installing target $TARGET..."
    rustup target add "$TARGET"
fi

# Change to project root for building
cd "$PROJECT_ROOT"

# Build using zigbuild
echo "Building with zigbuild..."
cargo zigbuild --target "$TARGET" --release

BINARY_PATH="$PROJECT_ROOT/target/$TARGET/release/flight-with-nexmark"

if [ ! -f "$BINARY_PATH" ]; then
    echo "Error: Binary not found at $BINARY_PATH"
    exit 1
fi

echo "Build complete!"
echo "Binary location: $BINARY_PATH"

# Deploy to SSH server
SSH_HOST="duck-2.dima.tu-berlin.de"
REMOTE_PATH="~/flight-with-nexmark"

echo ""
echo "Deploying to $SSH_HOST..."

if [ -z "$DUCK_PASSWORD" ]; then
    echo -n "DUCK_PASSWORD not found in environment. Enter password for $SSH_HOST: "
    read -s DUCK_PASSWORD
    echo ""
fi

# Copy binary to remote server using sshpass
if ! command -v sshpass &> /dev/null; then
    echo "Installing sshpass..."
    brew install hudochenkov/sshpass/sshpass
fi

echo "Copying binary to $SSH_HOST..."
sshpass -p "$DUCK_PASSWORD" scp "$BINARY_PATH" "$SSH_HOST:$REMOTE_PATH"

if [ $? -ne 0 ]; then
    echo "❌ Failed to deploy to $SSH_HOST"
    exit 1
fi

echo "✅ Successfully deployed to $SSH_HOST"


# scp ./flight-with-nexmark picocluster@192.168.2.42:~/
# scp ./flight-with-nexmark picocluster@192.168.2.43:~/

# ssh picocluster@192.168.2.42
# ssh picocluster@192.168.2.43

#  ./flight-with-nexmark exit --bind-address "[::]:8816"
# ./flight-with-nexmark processor --bind-address "[::]:8815" --exit-address "192.168.2.42:8816"
# ./flight-with-nexmark entry --records-per-chunk 100000 --no-records 10000000 --server-address "192.168.2.43:8815" pre-generated

# processo node: ./flight-with-nexmark 
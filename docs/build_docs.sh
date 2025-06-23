#!/bin/bash
# Script to build the Sphinx documentation for Astra

# Ensure we're in the docs directory
cd "$(dirname "$0")"

# Install documentation dependencies if needed
# echo "Checking documentation dependencies..."
# pip install -q -e "..[docs]" || { echo "Failed to install dependencies"; exit 1; }

# Clean previous build
echo "Cleaning previous build..."
rm -rf build/

# Build the documentation
echo "Building documentation..."
make html || { echo "Documentation build failed"; exit 1; }

echo "Documentation built successfully!"
echo "You can view it by opening: build/html/index.html"
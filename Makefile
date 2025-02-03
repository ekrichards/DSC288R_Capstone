.PHONY: all data clean setup lint test help

# Default target
all: data

# Run the DVC pipeline
data:
	dvc repro

# Install dependencies
setup:
	pip install -r requirements.txt

# Lint the codebase
lint:
	flake8 src/

# Show help information
help:
	@echo "Available commands:"
	@echo "  make        Run the entire pipeline (default)"
	@echo "  make data   Run the DVC pipeline"
	@echo "  make setup  Install project dependencies"
	@echo "  make lint   Lint the codebase"

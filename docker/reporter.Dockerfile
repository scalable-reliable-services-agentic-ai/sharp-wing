# Stage 1: Build the base environment
FROM python:3.12-slim AS base

WORKDIR /app

# Install uv, a fast Python package installer
RUN pip install uv

# Copy the project definition file and install dependencies
COPY pyproject.toml pyproject.toml
RUN uv pip install --system .

# Stage 2: Create the final image
FROM base AS final

COPY src/reporter/ /app/src/reporter
COPY config/ /app/config

# Set the python path to the root of the app
ENV PYTHONPATH=/app

CMD ["python", "-u", "src/reporter/main.py"]

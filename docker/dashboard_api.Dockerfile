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

COPY config/ /app/config/
COPY src/ /app/src

# Set the python path to the root of the app
ENV PYTHONPATH=/app

EXPOSE 8000

# Uruchamiamy FastAPI za pomocą uvicorn
# CMD ["uvicorn", "src.api:app", "--host", "0.0.0.0", "--port", "8000"]
CMD ["uvicorn", "src.dashboard.api:app", "--host", "0.0.0.0", "--port", "8000"]
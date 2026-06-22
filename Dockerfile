FROM python:3.12-slim

# rasterio, pyproj and shapely install as manylinux wheels that bundle their own
# GDAL/PROJ/GEOS, so no system GDAL or build toolchain is needed. Keeping this
# lean keeps the cold Codespace build fast (participants fork + build on the day).
RUN apt-get update && apt-get install -y --no-install-recommends \
    git curl && \
    rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /workspace

# Build the venv against the base image's Python and never re-resolve at runtime.
# The base is pinned to match .python-version (3.12), and that file is copied
# *before* the sync, so the venv baked here is exactly what `uv run` expects.
# Otherwise every container recreates the env and re-downloads everything on
# first start — which is what was timing out Codespace creation.
# Put the venv outside /workspace so the `.:/workspace` bind mount can't hide it.
# This lets every service share the one baked venv from the read-only image layer
# (no per-service venv volume, no 1.2 GB copy per container at first start).
ENV UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never \
    UV_PYTHON_PREFERENCE=only-system \
    UV_PROJECT_ENVIRONMENT=/opt/venv

COPY .python-version pyproject.toml uv.lock* ./
RUN uv sync --frozen --no-dev 2>/dev/null || uv sync

COPY . .

EXPOSE 8888

CMD ["uv", "run", "jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root", \
     "--NotebookApp.token=''", "--NotebookApp.password=''"]

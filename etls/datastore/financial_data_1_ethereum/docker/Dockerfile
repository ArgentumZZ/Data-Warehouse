# -----------------------------
# 0. Use an official Python base image
# -----------------------------
# ARG PY_VERSION=3.11-slim
# FROM python:${PY_VERSION}
FROM python:3.11-slim


# Update package lists and install system dependencies required
# for building Python packages that compile C extensions
RUN \
    # Update the package index so apt knows about the latest package versions
    apt-get update && \
    # Install system-level build tools and headers needed to compile Python C extensions
    apt-get install -y \
        # C compiler required for building Python packages with C extensions
        gcc \
        # Core build tools: make, g++, dpkg-dev, etc.
        build-essential \
        # Foreign Function Interface headers (needed for cffi, cryptography)
        libffi-dev \
        # PostgreSQL client headers (required for psycopg2)
        libpq-dev \
        # Python headers needed to compile Python C extensions
        python3-dev \
    # Remove cached package lists to keep the image size smaller
    && rm -rf /var/lib/apt/lists/*



RUN pip install --upgrade pip
# -----------------------------
# 1. Build-time arguments (ARG)
# -----------------------------
# ARG exists ONLY during docker build
# ARG: defines a build‑time variable
# Can be used for installation paths, build flags, temporary directories
# It does not exist when the container runs
# You can override it at build time: docker build --build-arg basedir=/custom/path -t my-image .
ARG NUMPY_VERSION=1.26.0
ARG basedir=/app
# Good practice: echo ARGs for debugging
RUN echo "Using basedir: $basedir"
RUN echo "Using numpy version: $NUMPY_VERSION"
# WORKDIR $basedir

# -----------------------------
# 2. Runtime environment variables (ENV)
# -----------------------------
# ENV defines a run‑time environment variable
# It exists inside the running container and needed when your script needs to read the value (e.g. mode, API keys, config, log level)
# Your Python code can read it using os.getenv()
# You can override it at runtime: docker run -e APP_ENV=dev my-image
# ENV BASEDIR=$basedir
# This allows import utilities to work because Python will look in /app and find the utilities/ folder.
ENV PYTHONPATH="/app"
ENV DATA_DIR=/app/data
ENV CONFIG_DIR=/app/config/db_config.cfg
# ENV LOG_DIR=/app/logs

# -----------------------------
# 3. Create working directory
# -----------------------------
# Use ARG here — valid during build
RUN mkdir -p $basedir
# Set working directory inside the container
WORKDIR $basedir

# -----------------------------
# 4. Install individual libraries
# -----------------------------
RUN pip install numpy==$NUMPY_VERSION

# -----------------------------
# 5. Install dependencies (cached layer)
# -----------------------------
# Copy dependency list first (better for caching)
COPY etls/datastore/financial_data_1_ethereum/docker/requirements.txt .
# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# -----------------------------
# 6. Copy config file (IMPORTANT)
# -----------------------------
RUN mkdir -p /app/config
COPY config/local/db_config.cfg /app/config/db_config.cfg

# -----------------------------
# 6. Copy project files
# -----------------------------
# Copy only the ethereum project subfolder into /app
COPY etls/datastore/financial_data_1_ethereum/ /app/

# -----------------------------
# 7. Default command
# -----------------------------
# Command to run your script
# CMD ["python", "docker_test.py"]

# the executable that always runs
ENTRYPOINT ["python", "/app/script_runner/script_runner.py"]

# In the terminal
# docker build -t my-python-test .
# docker run --rm my-python-test

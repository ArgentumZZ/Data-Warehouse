# 1. Use an official Python base image
FROM python:3.12-slim-bookworm

# 2. Update package lists and install system dependencies required
# for building Python packages that compile C extensions
RUN \
    # Update the package index, so apt knows about the latest package versions
    apt-get update && \
    # Install system-level build tools and headers needed to compile Python C extensions
    apt-get install -y \
    # avoid installing unnecessary "suggested" packages
    --no-install-recommends \
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

# 3. Environment Setup
# - Build-time arguments (ARG) do not exist at runtime.
# - Can override them at build time: docker build --build-arg basedir=/custom/path -t my-image .

# - Runtime environment variables (ENV)  inside the running container and are needed when the script needs to read values.
# - Python code can read ENV variables using os.getenv()
# - Can override them at runtime: docker run -e APP_ENV=dev my-image

# PYTHONDONTWRITEBYTECODE: Prevents Python from writing .pyc files (saves space/noise)
# PYTHONUNBUFFERED: Ensures logs are sent straight to the terminal without buffering
# PROJECT_NAME will be used in ENTRYPOINT, because it needs runtime variables
ARG project_name=financial_data_1_ethereum
ENV PROJECT_NAME=${project_name} \
    PYTHONPATH="/app:/app/${project_name}" \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# 4. Create a working directory inside the container (even if it doesn't exist, every command runs from there)
# Python treats /app as the base for all imports
WORKDIR /app

# 5. Dependency installations
# python -m pip ensures that pip is updated for the correct interpreter.
# --no-cache-dir keeps the image smaller
RUN python -m pip install --upgrade --no-cache-dir pip

# Copy only requirements first so code changes don't trigger a full reinstall.
COPY etls/datastore/${PROJECT_NAME}/docker/requirements.txt ./requirements.txt

# Install individual libraries for the project, they shouldn't exist in the requirements.txt
RUN pip install --no-cache-dir numpy==2.4.1 -r requirements.txt

# 6. Copy application files (e.g., config, shared modules, project files and environment file)
# COPY <file_path_1> <file_path_2>
COPY config/local/db_config.cfg /app/config/db_config.cfg
COPY config/local/setenv.sh /app/config/setenv.sh
COPY etls/utilities /app/utilities
COPY etls/connectors /app/connectors
COPY etls/datastore/${PROJECT_NAME} /app/${PROJECT_NAME}

# 7. Run the script

# sh -> shell, command runner, expands variables
# -c -> execute the following string as a shell command
# $@ -> forward all arguments
# -- -> prevent arguments from being interpreted as options to sh
ENTRYPOINT ["sh", "-c", ". /app/config/setenv.sh && python /app/${PROJECT_NAME}/script_runner/run_script.py \"$@\"", "--"]
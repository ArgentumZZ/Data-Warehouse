# 1. Use an official Python base image
FROM python:3.12-slim-bookworm

# 2. Update package lists and install system dependencies required
# for building Python packages that compile C extensions
RUN \
    # Update the package index, so apt knows about the latest package versions
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

# 3. Upgrade pip
# python -m pip ensures that pip is updated for the correct interpreter.
# --no-cache-dir keeps the image smaller
RUN python -m pip install --upgrade --no-cache-dir pip

# 4. Build-time arguments (ARG)
# ARG values do not exist at runtime
# Can override them at build time: docker build --build-arg basedir=/custom/path -t my-image .

# name of the project
ARG project_name=financial_data_1_ethereum

# library versions
ARG numpy_version=1.26.0

# root directory inside the container
# Python treats /app as the base for all imports
ARG basedir=/app

# 5. Runtime environment variables (ENV)

# They exist inside the running container and are needed when the script needs to read values
# They exist at runtime
# (e.g. mode, API keys, config, log level)
# Python code can read ENV variables using os.getenv()
# Can override them at runtime: docker run -e APP_ENV=dev my-image

ENV PYTHONPATH="${basedir}:${basedir}/${project_name}"
ENV CONFIG_DIR=${basedir}/config/db_config.cfg

# Will be used in ENTRYPOINT, because it needs runtime variables
ENV BASEDIR=${basedir}
ENV PROJECT_NAME=${project_name}
# ENV DATA_DIR=${basedir}/data

# 6. Create a working directory
RUN mkdir -p ${basedir}

# Set the working directory inside the container (every command runs from there)
WORKDIR ${basedir}

# 7. Install individual libraries for the project
RUN echo "Installing individual libraries"
RUN pip install numpy==$numpy_version

# 8. Install dependencies
# Libraries from point 7 shouldn't exist in the requirements.txt

# Copy dependency list first
COPY etls/datastore/${project_name}/docker/requirements.txt .

# Install dependencies
RUN echo "Installing project dependencies"
RUN pip install --no-cache-dir -r requirements.txt

# 9. Copy config file
RUN mkdir -p ${basedir}/config

# COPY <file_path_1> to <file_path_2>
COPY config/local/db_config.cfg ${basedir}/config/db_config.cfg

# 10. Copy Shared Modules
# Copy these so they are available at the same level as the project
# COPY <file_path_1> to <file_path_2>
COPY etls/utilities ${basedir}/utilities
COPY etls/connectors ${basedir}/connectors

# 11. Copy Project Files
# Copy the specific project folder directly into app
COPY etls/datastore/${project_name} ${basedir}/${project_name}

# 12. Run the script

# sh -> shell, command runner, expands variables
# -c -> execute the following string as a shell command

# ENTRYPOINT python ${BASEDIR}/${PROJECT_NAME}/script_runner/run_script.py
# ENTRYPOINT ["python", "/app/financial_data_1_ethereum/script_runner/run_script.py"]
ENTRYPOINT ["sh", "-c", "python ${BASEDIR}/${PROJECT_NAME}/script_runner/run_script.py"]

# Command to run the script
# CMD ["python", "script_runner.py"]

# In the terminal:
# docker build -t my-python-test .
# docker run --rm my-python-test
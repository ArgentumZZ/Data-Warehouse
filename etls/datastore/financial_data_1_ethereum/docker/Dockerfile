# 1. Use an official Python base image
FROM python:3.11-slim

# 2. Update package lists and install system dependencies required
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


# 3. Upgrade pip
RUN pip install --upgrade pip

# 4. Build-time arguments (ARG)
# Can override them at build time: docker build --build-arg basedir=/custom/path -t my-image .

# name of the project
ARG project_name=financial_data_1_ethereum
RUN echo "The project_name is set to: $project_name"

# library versions
ARG numpy_version=1.26.0
RUN echo "Using numpy version: $numpy_version"

 # root directory inside the container
ARG basedir=/app
RUN echo "Using basedir: $basedir"

# 5. Runtime environment variables (ENV)

# They exist inside the running container and are needed when the script needs to read the value (e.g. mode, API keys, config, log level)
# Python code can read tehem using os.getenv()
# Can override them at runtime: docker run -e APP_ENV=dev my-image

# Python treats /app as the base for all imports
ENV PYTHONPATH="${basedir}:${basedir}/${project_name}"
RUN echo "The PYTHONPATH ENV is set to: $PYTHONPATH"

ENV DATA_DIR=${basedir}/data
RUN echo "The DATA_DIR ENV is set to: $DATA_DIR"

ENV CONFIG_DIR=${basedir}/config/db_config.cfg
RUN echo "The CONFIG_DIR ENV is set to: $CONFIG_DIR"

ENV BASEDIR=${basedir}
RUN echo "The BASEDIR ENV is set to: $BASEDIR"

ENV PROJECT_NAME=${project_name}
RUN echo "The PROJECT_NAME ENV is set to: $PROJECT_NAME"
# 6. Create working directory
RUN mkdir -p ${basedir}

# Set working directory inside the container (every command runs from there.)
WORKDIR ${basedir}

# 7. Install individual libraries for the project
RUN echo "Installing individual libraries"
RUN pip install numpy==$numpy_version

# 8. Install dependencies (cached layer)

# Copy dependency list first (better for caching)
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
# Copy the specific project folder into a subfolder or directly into app
# Recommendation: Copy it into its own folder to keep the structure clean
COPY etls/datastore/${project_name} ${basedir}/${project_name}

# 12. Default command

# Command to run the script
# CMD ["python", "script_runner.py"]

# the executable that always runs
ENTRYPOINT ["python", "/app/financial_data_1_ethereum/script_runner/run_script.py"]
# ENTRYPOINT python ${BASEDIR}/${PROJECT_NAME}/script_runner/run_script.py


# In the terminal:
# docker build -t my-python-test .
# docker run --rm my-python-test

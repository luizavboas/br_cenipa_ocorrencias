# Python version: 3.10
FROM python:3.10-slim

# Environment
ENV DEBIAN_FRONTEND=noninteractive
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV VIRTUAL_ENV=.venv
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

# Install gcc, Google Chrome, CLI tools, git
RUN apt-get update && \
    apt-get install --no-install-recommends -y \
        wget \
        curl \
        gnupg \
        build-essential \
        git \
        libglib2.0-0 \
        libnss3 \
        libgconf-2-4 \
        libfontconfig1 \
        libxtst6 \
        libgtk-3-0 \
        libx11-xcb-dev \
        libdbus-glib-1-2 \
        libxt6 \
        libpci-dev \
        bzip2 \
    && wget -q -O - https://dl.google.com/linux/linux_signing_key.pub | apt-key add - \
    && echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" > /etc/apt/sources.list.d/google-chrome.list \
    && apt-get update \
    && apt-get install --no-install-recommends -y google-chrome-stable \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Virtualenv creation and activation
RUN python3 -m venv $VIRTUAL_ENV
RUN echo "Creating Virtualenv in $VIRTUAL_ENV"

# Instal Poetry and pip
RUN pip install --no-cache-dir --upgrade pip poetry

# Define working dir
WORKDIR /app

COPY . .

# Install Python dependencies (depends on the method: poetry or via requirements.txt)
RUN if [ -f "pyproject.toml" ]; then poetry install --no-root; fi
RUN if [ -f "requirements.txt" ]; then pip install --no-cache-dir -r requirements.txt; fi

RUN mkdir -p .prefect/app/bases
RUN mkdir -p .venv

# Prefect Server
EXPOSE 4200

# Start command
CMD ["bash", "startup.sh"]

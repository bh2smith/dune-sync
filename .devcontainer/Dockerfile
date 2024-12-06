FROM python:3.13-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
  git \
  curl \
  make \
  docker-compose \
  && rm -rf /var/lib/apt/lists/*

# Create non-root user
ARG USERNAME=vscode
ARG USER_UID=1000
ARG USER_GID=$USER_UID

RUN groupadd --gid $USER_GID $USERNAME \
  && useradd --uid $USER_UID --gid $USER_GID -m $USERNAME \
  && apt-get update \
  && apt-get install -y sudo \
  && echo $USERNAME ALL=\(root\) NOPASSWD:ALL > /etc/sudoers.d/$USERNAME \
  && chmod 0440 /etc/sudoers.d/$USERNAME

# Install Poetry and configure it
RUN python -m pip install poetry
ENV POETRY_VIRTUALENVS_CREATE=false


# Install Python dependencies
RUN pip install --no-cache-dir \
  poetry \
  black \
  pylint \
  isort \
  pytest \
  pytest-cov \
  ruff

# Switch to non-root user
USER $USERNAME

# Set working directory
WORKDIR /workspace
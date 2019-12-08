#-------------------------------------------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See https://go.microsoft.com/fwlink/?linkid=2090316 for license information.
#-------------------------------------------------------------------------------------------------------------

FROM python:3

# Avoid warnings by switching to noninteractive
ENV DEBIAN_FRONTEND=noninteractive

# This Dockerfile adds a non-root 'vscode' user with sudo access. However, for Linux,
# this user's GID/UID must match your local user UID/GID to avoid permission issues
# with bind mounts. Update USER_UID / USER_GID if yours is not 1000. See
# https://aka.ms/vscode-remote/containers/non-root-user for details.
ARG USERNAME=vscode
ARG USER_UID=1000
ARG USER_GID=$USER_UID
ENV DCC_HOME=/etc/devcontainer-common

# Uncomment the following COPY line and the corresponding lines in the `RUN` command if you wish to
# include your requirements in the image itself. It is suggested that you only do this if your
# requirements rarely (if ever) change.
COPY requirements.txt /tmp/pip-tmp/

RUN apt-get update \
    && apt-get -y install --no-install-recommends apt-utils dialog 2>&1 \
    #
    # Install the files that help prepare common devcontainer artifacts into $DCC_HOME
    && apt-get -y install curl \
    && curl https://raw.githubusercontent.com/shah/devcontainer-common/master/bootstrap.sh | /bin/bash \
    #
    # Verify git, process tools, lsb-release (common in install instructions for CLIs) installed
    && apt-get -y install git iproute2 procps lsb-release \
    #
    # We're using SQLite so let's grab database CLI tools
    && apt-get -y install sqlite3 \
    #
    # Install Java and Graphviz so we can run SchemaSpy db documentation utilities
    && apt-get -y install default-jdk graphviz \
    && wget -O "/usr/local/bin/schemaspy.jar" 'https://github.com/schemaspy/schemaspy/releases/download/v6.1.0/schemaspy-6.1.0.jar' \
    && wget -O "/usr/local/bin/sqlite-jdbc.jar" 'https://bitbucket.org/xerial/sqlite-jdbc/downloads/sqlite-jdbc-3.27.2.1.jar' \
    #
    # Setup common devcontainer shell
    && /bin/bash $DCC_HOME/setup-shell.sh \
    #
    # Update Python environment based on requirements.txt
    && pip --disable-pip-version-check --no-cache-dir install -r /tmp/pip-tmp/requirements.txt \
    && rm -rf /tmp/pip-tmp \
    #
    # Create a non-root user to use if preferred - see https://aka.ms/vscode-remote/containers/non-root-user.
    && groupadd --gid $USER_GID $USERNAME \
    && useradd -s /bin/bash --uid $USER_UID --gid $USER_GID -m $USERNAME \
    # [Optional] Add sudo support for the non-root user
    && apt-get install -y sudo \
    && echo $USERNAME ALL=\(root\) NOPASSWD:ALL > /etc/sudoers.d/$USERNAME\
    && chmod 0440 /etc/sudoers.d/$USERNAME \
    #
    # Clean up
    && apt-get autoremove -y \
    && apt-get clean -y \
    && rm -rf /var/lib/apt/lists/*

# Switch back to dialog for any ad-hoc use of apt-get
ENV DEBIAN_FRONTEND=

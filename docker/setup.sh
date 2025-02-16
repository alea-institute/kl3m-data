#!/usr/bin/env bash
# ubuntu 24.04

sudo apt update -y --fix-missing
sudo apt dist-upgrade -y

# instal prereqs
sudo apt install -y build-essential pkg-config libssl-dev cmake uidmap poppler-utils tesseract-ocr-eng tesseract-ocr unzip zip git

# install docker
curl -fsSL https://get.docker.com -o get-docker.sh
sudo sh ./get-docker.sh
dockerd-rootless-setuptool.sh install

# install rust with nightly and no prompts
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- --default-toolchain nightly -y

# install uv
curl -LsSf https://astral.sh/uv/install.sh | sh

# clone repo
git clone https://github.com/alea-institute/kl3m-data.git
cd kl3m-data
git checkout v0.1.2
uv sync
uv run python3 -V
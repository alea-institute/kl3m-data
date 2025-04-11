FROM ubuntu:24.04

# Avoid prompts from apt
ENV DEBIAN_FRONTEND=noninteractive

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 \
    python3-pip \
    python3-venv \
    pipx \
    curl \
    build-essential \
    && \
    pipx install poetry \
    && \
    pipx ensurepath

# create /app directory
WORKDIR /app
RUN cd /app

# copy the poetry configuration \
COPY pyproject.toml poetry.lock ./

# build the poetry environment
RUN bash -l -c "poetry install --no-root" \
    && \
    bash -l -c "poetry run playwright install-deps" \
    && \
    bash -l -c "poetry run playwright install"

# copy the python api code
COPY kl3m_data kl3m_data
COPY config.json config.json

CMD ["bash", "-l", "-c", "PYTHONPATH=. poetry run python3 kl3m_data/cli/main.py fdlp download_all"]
# docker exec alea-web-survey tail -f alea_web_survey.log

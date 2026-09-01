FROM ubuntu:noble AS build

SHELL ["/bin/sh", "-exc"]

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update -qy && \
  apt-get install -qyy \
  -o APT::Install-Recommends=false \
  -o APT::Install-Suggests=false \
  build-essential \
  ca-certificates \
  python3-setuptools \
  python3.12-dev

COPY --from=ghcr.io/astral-sh/uv:0.10.2 /uv /usr/local/bin/uv

ENV UV_LINK_MODE=copy \
  UV_COMPILE_BYTECODE=1 \
  UV_PYTHON_DOWNLOADS=never \
  UV_PYTHON=python3.12 \
  UV_PROJECT_ENVIRONMENT=/app

COPY . /src
WORKDIR /src

RUN uv sync \
  --locked \
  --no-dev \
  --no-editable

##########################################################################

FROM ubuntu:noble

SHELL ["/bin/sh", "-exc"]

ARG PYTHON_MODULE_NAME
ENV PYTHON_MODULE_NAME=$PYTHON_MODULE_NAME

ENV PATH=/app/bin:$PATH
ENV PYTHONUNBUFFERED=1

RUN groupadd -r app && \
  useradd -r -d /app -g app -N app

COPY docker-entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]
#STOPSIGNAL SIGINT

RUN apt-get update -qy && \
  apt-get install -qyy \
  -o APT::Install-Recommends=false \
  -o APT::Install-Suggests=false \
  python3.12 \
  libpython3.12 \
  ca-certificates \
  libpcre3 \
  libxml2 && \
  apt-get clean && \
  rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

COPY --from=build --chown=app:app /app /app

USER app
WORKDIR /src

RUN python -V && \
  python -Im site && \
  python -Ic "import $PYTHON_MODULE_NAME"

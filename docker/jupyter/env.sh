#!/bin/sh

export JUPYTERHUB_CRYPT_KEY=$(openssl rand -hex 32)

jupyterhub
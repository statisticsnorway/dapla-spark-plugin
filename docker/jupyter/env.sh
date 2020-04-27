#!/bin/sh

export JUPYTERHUB_CRYPT_KEY=$(openssl rand -hex 32)

#echo "/jupyter/check-git-config.sh" > $HOME/.bashrc

jupyterhub
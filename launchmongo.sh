#!/bin/sh
# If you are not on ubuntu 18.04 you need to change the version (don't use bionic)
docker run -p 27017:27017 -d mongo:bionic

# you can connect to it with the mongo shell by using `mongo`

# to destroy it use docker ps -a and kill that container with docker kill

# to use jupyter:
# install:
# sudo python3 -m pip install jupyter

# launch:
# jupyter notebook

#!/bin/bash

sudo snap install --classic certbot -y
sudo ln -s /snap/bin/certbot /usr/bin/certbot
sudo certbot certonly --standalone
# copy certs; 
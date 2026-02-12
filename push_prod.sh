#!/usr/bin/env bash
set -e
cd /home/developer/padifood-backend
systemctl --user daemon-reload
systemctl --user restart padifood-backend
systemctl --user status padifood-backend

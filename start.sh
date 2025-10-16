#!/bin/bash

# containerにssh出来るように
if [ ! -d "/run/sshd" ]; then
  mkdir -p /run/sshd
fi

/usr/sbin/sshd

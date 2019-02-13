#!/usr/bin/env bash
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
myip=
while IFS=$': \t' read -a line ;do
    [ -z "${line%inet}" ] && ip=${line[${#line[1]}>4?1:2]} &&
        [ "${ip#127.0.0.1}" ] && myip=$ip
  done< <(LANG=C /sbin/ifconfig)

if [ $# -eq 0 ]
  then
    $DIR/edas-ssh --hostfile $DIR/../resources/hosts --scheduler $myip:8786 $PKEY_OPTS
  else
    $DIR/edas-ssh --nprocs $1 --hostfile $DIR/../resources/hosts --scheduler $myip:8786 $PKEY_OPTS
fi

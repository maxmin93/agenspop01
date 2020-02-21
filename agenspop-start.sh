#!/bin/bash

target_path=./target
jarfile=( "$target_path"/agenspop*.jar )
echo $jarfile
[[ -e $jarfile ]] || {
  echo "ERROR: not exist agenspop jar file\nTry build and start again.." >&2;
  exit 1;
  }

echo "Run target jar: $jarfile"
java -jar $jarfile --spring.config.name=es-config

#!/bin/bash -e

# Extract dump into a temporary directory.
TMPFILE=`mktemp -d /tmp/dump.XXXXXXXXXX`
mkdir -p $TMPFILE
tar -x -C $TMPFILE <&0

# Restore from temporary directory into database.
/usr/bin/mongorestore $@ $TMPFILE/*

# Remove dump directory.
rm -rf $TMPFILE

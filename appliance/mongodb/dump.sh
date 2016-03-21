#!/bin/bash

# Passthrough arguments and dump to temporary path.
TMPFILE=`mktemp -d /tmp/dump.XXXXXXXXXX`
/usr/bin/mongodump $@ -o $TMPFILE

# Archive dump directory, gzip, & write to STDOUT.
cd $TMPFILE
tar -cf - *

# Remove dump directory.
rm -rf $TMPFILE

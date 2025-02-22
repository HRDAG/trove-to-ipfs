#!/bin/bash

# precede by
# psql -U $USER -d $DB  -c "select distinct car_url from fs where tsize is NULL;" > car_did_missed.txt

NJOBS=50

get1csv() {
    jfile="/var/tmp/pescados/car-did-csv/$1.csv"
    if [ ! -f "$jfile" ]; then
        echo "$jfile"
        ipfs ls "$1" >"$jfile"
    fi
}

export -f get1csv
find /var/tmp/pescados/car-did-csv -type f -size -5c -delete

cat /var/tmp/pescados/missing-cids.txt | parallel --timeout 120 -j $NJOBS get1csv

find /var/tmp/pescados/car-did-csv -type f -size -5c -delete
# done.

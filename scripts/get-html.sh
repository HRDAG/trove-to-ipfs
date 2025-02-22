#!/bin/bash

# precede by
# psql -U $USER -d $DB  -c "select distinct car_url from fs where tsize is NULL;" > car_did_missed.txt

NJOBS=20

get1html() {
    jfile="/var/tmp/pescados/car-did-html/$1.html"
    #--- note remove next line when necessary
    # rm "$jfile"
    #---
    url="https://w3s.link/ipfs/$1"
    echo "$url" >&2
    if [ ! -f "$jfile" ]; then
        curl -sS -L -o "$jfile" "$url"
    else
        echo "found $jfile, skipping" >&2
    fi
}

export -f get1html
find /var/tmp/pescados/car-did-html -type f -size -5c -delete

cat /var/tmp/pescados/car-did-html.txt | parallel --timeout 1200 -j $NJOBS get1html

find /var/tmp/pescados/car-did-html -type f -size -5c -delete
# done.

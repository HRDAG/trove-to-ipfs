#!/bin/bash

# precede with "w3 ls > car_did.txt"

my_func() {
    jfile="/var/tmp/pescados/car-did-json/$1.json"
    if [ ! -f "$jfile" ]; then
        ipfs dag get "$1" > "$jfile"
    fi
}

export -f my_func
cat  /var/tmp/pescados/car_did.txt | parallel -j 50 my_func


# done.

#! /bin/bash

CSV_FILE="$@"
echo "$CSV_FILE"

HASH_FILE="$CSV_FILE"".hash" 
echo ${HASH_FILE}

LOAD_FILE="${CSV_FILE}"".detail"
echo ${LOAD_FILE}

cat /dev/null > "${HASH_FILE}"

cat "$@" | while read -r line; do
    printf %s "$line" | md5sum | cut -f1 -d' '  >> "${HASH_FILE}"
done

sed -i '1s/^.*$/line_hash/' "${HASH_FILE}"

paste -d"," "${CSV_FILE}" "${HASH_FILE}" > "${LOAD_FILE}"

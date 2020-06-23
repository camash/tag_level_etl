#! /bin/bash


if [ $# != 1 ] ; then
  echo "USAGE: $0 csv_file_name"
  echo " e.g.: $0 xxx.csv"
  exit 1;
fi

# get local csv location
CSV_FOLDER=`grep "csv_base" connection.cfg | cut -d= -f2`

cd "${CSV_FOLDER}"

CSV_FILE="$@"
echo "$CSV_FILE"

HASH_FILE="$CSV_FILE"".hash"
echo ${HASH_FILE}

SUFFIX=".detail"
LOAD_FILE="${CSV_FILE}""${SUFFIX}"
echo ${LOAD_FILE}

cat /dev/null > "${HASH_FILE}"

cat "$@" | while read -r line; do
    printf %s "$line" | md5sum | cut -f1 -d' '  >> "${HASH_FILE}"
done

sed -i '1s/^.*$/line_hash/' "${HASH_FILE}"

paste -d"," "${CSV_FILE}" "${HASH_FILE}" > "${LOAD_FILE}"

if [ $? -eq 0 ]; then
  echo "${SUFFIX}"
else
  return 1
fi

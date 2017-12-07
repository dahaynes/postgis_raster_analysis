#!/bin/bash
#Script for batch loading data into Postgresql

while getopts s:b:f:t: option
do
    case "${option}"
    in
    s) srid=${OPTARG};;
    b) binary=${OPTARG};;
    f) shape=${OPTARG};;
    t) table=${OPTARG};;
    esac
done

#args=(" -s ${srid}" " > ${binary}" )
shp2pgsql -s ${srid} -I ${shape} ${table} > ${binary}  

for database in master node1 node2 node3 node4 node5 node6 node7 node8 node9 node10 node11 node12; do
    psql -d ${database} -c "DROP TABLE IF EXISTS "${table}
    psql -d ${database} -f ${binary}
done




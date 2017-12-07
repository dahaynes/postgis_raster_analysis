#!/bin/bash
#Script for batch loading data into Postgresql

while getopts s:r:t: option
do
    case "${option}"
    in
    s) srid=${OPTARG};;
    r) raster=${OPTARG};;
    t) table=${OPTARG};;
    esac
done

#args=(" -s ${srid}" " > ${binary}" )


for tilesize in 50 100 200 300 400 500 600 700 800 900 1000 1500 2000 2500 3000 3500 4000; do
    
    #args=("-s ${srid}" "-b /data/04489/dhaynes/${table}_${tilesize}.sql" "-t ${tilesize}" "-f ${raster}" "-n ${table}_${tilesize}" )

    raster2pgsql -C -x -I -Y -F -s ${srid} -t ${tilesize}"x"${tilesize} ${raster} ${table}"_"${tilesize} > "/data/04489/dhaynes/"${table}"_"${tilesize}".sql"
    
    #raster2pgsql -C -x -I -Y -F -t 50x50 -s 4326 ../glc2000_clipped.tif glc_clipped_50_50 > /data/04489/dhaynes/glc_clipped_50.sql

    for database in master node1 node2 node3 node4 node5 node6 node7 node8 node9 node10 node11 node12; do
        psql -d ${database} -c "DROP TABLE IF EXISTS "${table}"_"${tilesize}
        psql -d ${database} -f "/data/04489/dhaynes/"${table}"_"${tilesize}".sql" &
    
    done
    wait 
done

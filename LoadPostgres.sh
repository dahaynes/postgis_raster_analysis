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



      
#python3 

for tilesize in 50 100 200 300 400 500 600 700 800 900 1000 1500 2000 2500 3000 3500 4000; do
    args=("-s ${srid}" "-b /data/04489/dhaynes/${table}_${tilesize}.sql" "-t ${tilesize}" "-f ${raster}" "-n ${table}_${tilesize}" )
    #LoadArray="python3 postgresql_loading.py 
    python3 postgresql_loading.py ${args[@]}
    echo ${args[@]}
done


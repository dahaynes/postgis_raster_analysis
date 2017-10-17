-- select (st_valuecount(rast)).* from glc_250 limit 10


with rasterdataset as
(
SELECT array_agg(rid) as ids
FROM glc_250
)
SELECT ids[4:8]
FROM rasterdataset

Select ST_SummaryStatsAgg

SELECT statefp, name, (ST_SummaryStatsAgg(ST_Clip(r.rast, p.geom  ), 1, True )).*
FROM states p inner join glc_250 r on ST_Intersects(r.rast, p.geom)
GROUP by p.statefp, name


select *  from states limit 5

create or replace view mybqml.bqml.tlc_yellow_trips_2015_fields
as
SELECT
  *,
  ST_X(pickup_geo_snap) pickup_geo_snap_longitude,
  ST_Y(pickup_geo_snap) pickup_geo_snap_lattitude,
  ST_X(dropoff_geo_snap) dropoff_geo_snap_longitude,
  ST_Y(dropoff_geo_snap) dropoff_geo_snap_lattitude,
  EXTRACT(TIME FROM DATETIME_ADD(pickup_datetime, INTERVAL CAST(geo_distance_miles / 11 * 3600 as INT64) SECOND)) as dropoff_time_est
from
(
  SELECT 
    *,
    ST_SnapToGrid(pickup_geo, 0.03) pickup_geo_snap,
    ST_SnapToGrid(dropoff_geo, 0.03) dropoff_geo_snap,
    
    ST_Distance(
      pickup_geo,
      dropoff_geo
    ) as geo_distance,

    ST_Distance(
      pickup_geo,
      dropoff_geo
    ) / 1609 as geo_distance_miles,

    ST_MakeLine( 
      pickup_geo,
      dropoff_geo
    ) as geo_route,
    
    (fare_amount + tolls_amount + extra) AS trip_total_fare,
    (trip_dayofweek-1) * 24 + trip_hourofday as trip_weekhour_fc,
    
    case 
      when trip_dayofweek in (1, 7) THEN 1
      ELSE 0 
    END as trip_weekend,

    trip_distance / trip_duration * 3600 as trip_mph    
  FROM
    (select 
      case MOD(ABS(FARM_FINGERPRINT(CAST(pickup_datetime AS STRING))), 2)
        when 0 then 'TRAIN'
        when 1 then 'TEST'
        ELSE 'NA'
      end as class,

      CONCAT( CAST(pickup_datetime AS STRING), CAST(pickup_longitude  AS STRING) ) as trip_key,

      *,

      EXTRACT(DAYOFWEEK FROM pickup_datetime) AS trip_dayofweek,
      EXTRACT(HOUR FROM pickup_datetime) AS trip_hourofday,
      EXTRACT(TIME FROM pickup_datetime) AS pickup_time,
      DATETIME_DIFF(dropoff_datetime, pickup_datetime, SECOND) as trip_duration,

      ST_GeogPoint(pickup_longitude, pickup_latitude) as pickup_geo,
      ST_GeogPoint(dropoff_longitude, dropoff_latitude) as dropoff_geo
    from 
      `bigquery-public-data.new_york_taxi_trips.tlc_yellow_trips_2015`
    where
        pickup_datetime < dropoff_datetime
      AND
        pickup_longitude * pickup_latitude * dropoff_longitude * dropoff_latitude > 0
      AND
        fare_amount >= 1 and trip_distance > 0
      AND
        MOD(ABS(FARM_FINGERPRINT(CAST(dropoff_datetime AS STRING))), 10000) = 1
    )
)
where
  geo_distance > 0
AND
  trip_distance < 2 * geo_distance_miles
 
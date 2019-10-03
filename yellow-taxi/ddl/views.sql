create or replace table bqml.data_staging
    as 
    select
    *
    from 
    bqml.tlc_yellow_trips_2015_fields
;

create or replace view bqml.data_stream
    as
    select
    *
    from mybqml.bqml.data_staging
    where
    trip_distance < 2 * geo_distance_miles
;

create or replace view bqml.data_stream_train
    as
    select
    *
    from mybqml.bqml.data_stream
    where
    class = 'TRAIN'
;

create or replace view bqml.data_stream_test
as
    select
    *
    from mybqml.bqml.data_stream
    where
    class <> 'TEST'


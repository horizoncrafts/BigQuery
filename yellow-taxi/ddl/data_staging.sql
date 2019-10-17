create or replace table bqml.data_staging
    as 
    select
        *
    from 
        bqml.tlc_yellow_trips_2015_fields
;
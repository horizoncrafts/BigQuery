select
    ROUND(T.trip_total, 0) tot,
    ABS(trip_total - T.predicted_total) as avarage_error,
    POW(ABS(trip_total - T.predicted_total), 2) as MSE,
    T.*
from
    bqml.predicted_total T

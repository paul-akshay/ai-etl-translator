graph hotel_booking_workflow

source read_bookings
{
    type: input_file
    file: bookings.csv
    schema: booking_schema.dml
}

transform filter_confirmed
{
    type: filter_by_expression
    condition: booking_status == "CONFIRMED"
}

transform filter_city
{
    type: filter_by_expression
    condition: city == "Vancouver"
}

transform filter_min_stay
{
    type: filter_by_expression
    condition: nights >= 2
}

transform aggregate_revenue
{
    type: rollup
    group_by: hotel_id
    aggregate: sum(total_price)
}

target write_summary
{
    type: output_file
    file: hotel_revenue_summary.csv
}

flow
read_bookings -> filter_confirmed -> filter_city -> filter_min_stay -> aggregate_revenue -> write_summary
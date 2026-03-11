graph sales_workflow

source read_sales
{
    type: input_file
    file: sales.csv
    schema: sales_schema.dml
}

transform filter_sales
{
    type: filter_by_expression
    condition: category == "Electronics"
}

transform aggregate_sales
{
    type: rollup
    group_by: product_id
    aggregate: sum(units_sold)
}

target write_summary
{
    type: output_file
    file: sales_summary.csv
}

flow
read_sales -> filter_sales -> aggregate_sales -> write_summary
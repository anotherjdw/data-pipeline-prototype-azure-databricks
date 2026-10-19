import data_pipeline_prototype.transformations.format_orders_transformations as job
import data_pipeline_prototype.utils.schema.schema_definitions as schema


def run() -> None:

    args = job.parse_arguments()

    read = job.read(args=args)

    transform = job.transform(
        orders=read["orders"],
    )

    job.write(
        output_dataframe=transform,
        table_name=args.OUTPUT_TABLE,
        expected_schema=schema.preprocessed_orders_schema,
    )


if __name__ == "__main__":
    run()

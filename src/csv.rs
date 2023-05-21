use polars::prelude::*;

pub fn read_service_csv(path: &str) -> DataFrame {
    let df = LazyCsvReader::new(path).has_header(true).finish().unwrap();

    let df = df
        .select([col("*").interpolate(InterpolationMethod::Linear)])
        .with_column(
            (col("timestamp") * lit(1000)).cast(DataType::Datetime(TimeUnit::Milliseconds, None)),
        )
        .with_column(col("timestamp").dt().time().alias("time"))
        .collect()
        .unwrap();
    df
}
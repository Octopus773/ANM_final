use polars::export::chrono::{DateTime, NaiveTime, Utc};
use polars::prelude::*;
use std::collections::HashMap;
use std::env;
use std::path::Path;

#[path = "csv.rs"]
mod my_csv;

use my_csv::read_service_csv;

fn main() {
    let args: Vec<String> = env::args().take(3).collect();
    assert!(
        args.len() == 3,
        "Usage: {} <train_data_folder> <error_data_folder>",
        args[0]
    );
    let train_data_folder = Path::new(&args[1]).join("processed");
    let error_data_folder = Path::new(&args[2]).join("processed");

    let mut train_data = HashMap::new();

    for file in train_data_folder.read_dir().unwrap() {
        let file = file.unwrap();
        if !file.file_name().to_str().unwrap().ends_with(".csv") {
            continue;
        }
        train_data.insert(
            file.file_name().to_str().unwrap().to_string(),
            read_service_csv(file.path().to_str().unwrap()),
        );
    }

    let df = train_data.get("adservice2-0.csv").unwrap();

    let out = df
        .clone()
        .lazy()
        .filter(
            col("timestamp")
                .dt()
                .hour()
                .eq(16)
                .and(col("timestamp").dt().minute().gt(2))
                .and(col("timestamp").dt().minute().lt(10)),
        )
        .collect()
        .unwrap();

    println!("{:?}", out);

    for case in error_data_folder.read_dir().unwrap() {
        let case = case.unwrap();
        for file in case.path().read_dir().unwrap() {
            let file = file.unwrap();
            if !file.file_name().to_str().unwrap().ends_with(".csv") {
                continue;
            }
            let services = read_service_csv(&file.path().to_str().unwrap());
            let file_name = file.file_name();

            let start = services
                .column("timestamp")
                .unwrap()
                .time()
                .unwrap()
                .into_iter()
                .next()
                .unwrap()
                .unwrap();

            let filter_expr = col("timestamp")
                .dt()
                .hour()
                .eq(16)
                .and(col("timestamp").dt().minute().gt(2))
                .and(col("timestamp").dt().minute().lt(10));

            let service_train_data = train_data.get(file_name.to_str().unwrap()).unwrap();

            let service_train_data = service_train_data
                .clone()
                .lazy()
                .filter(filter_expr)
                .collect()
                .unwrap();

            for (test_serie, train_serie) in services.iter().zip(service_train_data.iter()) {
                println!("{:?}", test_serie);
                println!("{:?}", train_serie);
                break;
            }
            break;
        }
    }
}

use polars::export::chrono::{DateTime, NaiveTime, Utc};
use polars::prelude::*;
use std::collections::HashMap;
use std::env;
use std::path::Path;

#[path = "csv.rs"]
mod my_csv;

use my_csv::read_service_csv;

fn get_datas_from_series(series: &[&Series]) -> Vec<Vec<f64>> {
    let dtype = series[1].dtype();

    let datas: Vec<Vec<f64>> = match dtype {
        DataType::Float64 => {
            series
                .iter()
                .map(|s| s.f64().unwrap())
                .into_iter()
                .map(|it| it.into_iter().map(|el| el.unwrap()).collect::<Vec<_>>())
                .collect::<Vec<_>>()
        },
        DataType::Int64 => {
            series
                .iter()
                .map(|s| s.i64().unwrap())
                .into_iter()
                .map(|it| it.into_iter().map(|el| el.unwrap() as f64).collect::<Vec<_>>())
                .collect::<Vec<_>>()
        },
        _ => panic!("Not implemented, {:?}", dtype),
    };
    datas
}

fn e_diagnosis(s_a: &[f64], s_n: &[f64]) -> f64 {
    let mean_a = s_a.iter().sum::<f64>() / s_a.len() as f64;
    let mean_n = s_n.iter().sum::<f64>() / s_n.len() as f64;

    0.0
}

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

    for case in error_data_folder.read_dir().unwrap() {
        let case = case.unwrap();
        for file in case.path().read_dir().unwrap() {
            let file = file.unwrap();
            if !file.file_name().to_str().unwrap().ends_with(".csv") {
                continue;
            }
            let services = read_service_csv(&file.path().to_str().unwrap());
            let file_name = file.file_name();

            let service_train_data = train_data.get(file_name.to_str().unwrap()).unwrap();

            let data = services
                .join(
                    &service_train_data,
                    ["time"],
                    ["time"],
                    JoinType::Left,
                    Some("_train".to_string()),
                )
                .unwrap()
                .lazy()
                .select([col("*").exclude(["timestamp", "time"])])
                .collect()
                .unwrap();

            let columns = data.get_column_names();
            let columns = columns.iter().take(columns.len() / 2);


            for column in columns {
                let series = data
                    .columns(&[*column, &format!("{}_train", column)])
                    .unwrap();

                let datas = get_datas_from_series(&series);
                

                
            }
            break;
        }
    }
}

use polars::prelude::*;
use rayon::prelude::*;
use serde_json::json;
use std::collections::HashMap;
use std::env;
use std::path::Path;

#[path = "csv.rs"]
mod my_csv;

use my_csv::read_service_csv;

fn get_datas_from_series(series: &[&Series]) -> Vec<Vec<f64>> {
    let datas = series
        .iter()
        .map(|s| {
            s.cast(&DataType::Float64)
                .unwrap()
                .f64()
                .unwrap()
                .to_owned()
        })
        .into_iter()
        .map(|it| {
            it.into_iter()
                .map(|el| el.unwrap_or(0.0))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();

    datas
}

fn e_diagnosis(s_a: &[f64], s_n: &[f64]) -> f64 {
    let mean_a = s_a.iter().sum::<f64>() / s_a.len() as f64;
    let mean_n = s_n.iter().sum::<f64>() / s_n.len() as f64;

    let variance_a = s_a.iter().map(|el| (el - mean_a).powi(2)).sum::<f64>() / s_a.len() as f64;
    let variance_n = s_n.iter().map(|el| (el - mean_n).powi(2)).sum::<f64>() / s_n.len() as f64;

    if variance_a == 0.0 || variance_n == 0.0 {
        return 0.0;
    }

    let covariance_a_n = s_a
        .iter()
        .zip(s_n.iter())
        .map(|(a, n)| (a - mean_a) * (n - mean_n))
        .sum::<f64>()
        / s_a.len() as f64;

    covariance_a_n.powi(2) / (variance_a * variance_n).sqrt()
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

    let root_causes = error_data_folder
        .read_dir()
        .unwrap()
        .into_iter()
        .filter_map(|dir| dir.ok())
        .filter(|dir| dir.path().is_dir())
        .par_bridge()
        .map(|case| {
            let mut nb_root_causes = case
                .path()
                .read_dir()
                .unwrap()
                .into_iter()
                .filter_map(|file| file.ok())
                .filter(|file| file.file_name().to_str().unwrap().ends_with(".csv"))
                .par_bridge()
                .map(|file| {
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

                    let file_root_causes = columns
                        .map(|col| {
                            let series = data.columns(&[*col, &format!("{}_train", col)]).unwrap();

                            let datas = get_datas_from_series(&series);

                            let d = e_diagnosis(&datas[0], &datas[1]);
                            (col, d)
                        })
                        .filter(|(_, d)| *d < 0.05);

                    (
                        file_name.to_str().unwrap().to_owned(),
                        file_root_causes.count(),
                    )
                })
                .collect::<Vec<_>>();

            nb_root_causes.sort_by_key(|el| el.1);
            let nb_root_causes = nb_root_causes
                .iter()
                .rev()
                .map(|x| x.0.to_owned())
                .collect::<Vec<_>>();

            nb_root_causes
        })
        .collect::<Vec<_>>();

    let json = json!(root_causes);

    std::fs::write("/tmp/anm.json", json.to_string()).unwrap();
}

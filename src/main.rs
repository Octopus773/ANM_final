use polars::prelude::*;
use rayon::prelude::*;
use serde_json::json;
use std::collections::HashMap;
use std::env;
use std::path::Path;

mod graphs;
#[path = "csv.rs"]
mod my_csv;
mod tweaks;

use my_csv::read_service_csv;
use tweaks::service_peers_check;

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

fn e_diagnosis(s_a: &[f64], s_n: &[f64]) -> (f64, f64, f64, f64, f64, f64) {
    let mean_a = s_a.iter().sum::<f64>() / s_a.len() as f64;
    let mean_n = s_n.iter().sum::<f64>() / s_n.len() as f64;

    let variance_a = s_a.iter().map(|el| (el - mean_a).powi(2)).sum::<f64>() / s_a.len() as f64;
    let variance_n = s_n.iter().map(|el| (el - mean_n).powi(2)).sum::<f64>() / s_n.len() as f64;

    let std_a = variance_a.sqrt();
    let std_n = variance_n.sqrt();


    // if variance_a == 0.0 || variance_n == 0.0 {
    //     return 0.0; // big value to not be considered as a root cause
    // }


    // let anomaly_score = s_a
    //     .iter()
    //     .map(|a| ((a - mean_n) / std_n).abs())
    //     .sum::<f64>() / s_a.len() as f64;

    // anomaly_score

    if variance_a == 0.0 && variance_n == 0.0 && mean_a == mean_n {
        return (10000.0, mean_a, mean_n, std_a, std_n, -1.0); // big value to not be considered as a root cause
    } else if (mean_a <= mean_n * 1.3 && mean_a >= mean_n * 0.7) || (mean_n - mean_a).abs() <= 1.0 {
        if  std_a <= std_n * 1.5 && std_a >= std_n * 0.5 {
            return (5000.0, mean_a, mean_n, std_a, std_n, -1.0);
        } else if (std_n - std_a).abs() <= 0.08 {
            return (4000.0, mean_a, mean_n, std_a, std_n, -1.0);
        }
    } else if variance_a == 0.0 || variance_n == 0.0 {
        return (0.0, mean_a, mean_n, std_a, std_n, -1.0);
    }

    let covariance_a_n = s_a
        .iter()
        .zip(s_n.iter())
        .map(|(a, n)| (a - mean_a) * (n - mean_n))
        .sum::<f64>()
        / (s_a.len() - 1) as f64;

    let d = covariance_a_n / variance_a.max(variance_n);
    // (variance_a * variance_n).sqrt();

    // println!(
    //     "mean_a: {}, mean_n: {}, variance_a: {}, variance_n: {}, covariance_a_n: {} d: {}",
    //     mean_a, mean_n, variance_a, variance_n, covariance_a_n, d
    // );
    (d, mean_a, mean_n, std_a, std_n, covariance_a_n)
}

fn main() {
    let args: Vec<String> = env::args().take(4).collect();
    assert!(
        args.len() == 3 || args.len() == 4,
        "Usage: {} <train_data_folder> <error_data_folder> [<service_debug_graphs>]",
        args[0]
    );
    let train_data_folder = Path::new(&args[1]).join("processed");
    let error_data_folder = Path::new(&args[2]).join("processed");
    let case_debug_graphs = args.get(3).map(|s| s.as_str());

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

    let mut root_causes = error_data_folder
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
                // .par_bridge()
                .map(|file| {
                    let services = read_service_csv(&file.path().to_str().unwrap());
                    let file_name = file.file_name();
                    let file_name = file_name.to_str().unwrap();

                    let service_train_data = train_data.get(file_name).unwrap();

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
                    let nb_columns = columns.len();
                    let columns = columns.iter().take(nb_columns / 2);

                    let mut series_data = HashMap::new();

                    let file_root_causes = columns
                        // .par_bridge()
                        .map(|col| {
                            let series = data.columns(&[*col, &format!("{}_train", col)]).unwrap();

                            let datas = get_datas_from_series(&series);
                            series_data.insert(col, datas.clone());

                            let d = e_diagnosis(&datas[0], &datas[1]);
                            (col, d)
                        })
                        .filter(|(_, d)| (*d).0 < 10000.0)
                        .collect::<Vec<_>>();

                    if case_debug_graphs.is_some() && case_debug_graphs.unwrap() == case.file_name() {
                    graphs::plot_file_graph(
                        file_name,
                        &series_data,
                        file_root_causes.as_slice(),
                    );
                    }

                    let file_root_causes = file_root_causes.iter().filter(|(_, d)| (*d).0 < 0.1).map(|rc| (rc.0, rc.1.0));

                    // if file_name == "currencyservice-1.csv" {
                    //     println!("Root cause details:");
                    //     println!("{:?}", file_root_causes.clone().collect::<Vec<_>>());
                    // }

                    (file_name.to_owned(), file_root_causes.count())
                })
                .collect::<Vec<_>>();

            // match service_peers_check(&nb_root_causes) {
            //     Some(peer) => {
            //         println!(
            //             "{} has {} peer root causes",
            //             case.file_name().to_str().unwrap(),
            //             peer
            //         );
            //         let idx = nb_root_causes.iter().position(|el| el.0 == peer).unwrap();
            //         nb_root_causes[idx].1 += 50;
            //     }
            //     None => (),
            // }

            nb_root_causes.sort_by_key(|el| el.1);

            // println!("list of root causes: {:?}", nb_root_causes);

            nb_root_causes.sort_by_key(|el| el.1);
            if case.file_name().to_str() == case_debug_graphs {
                println!("list of root causes: {:?}", nb_root_causes);
            }
            let list_root_causes = nb_root_causes
                .iter()
                .rev()
                .map(|x| x.0.to_owned())
                .collect::<Vec<_>>();

            (
                case.file_name().to_str().unwrap().parse::<i32>().unwrap(),
                list_root_causes,
            )
        })
        .collect::<Vec<_>>();

    root_causes.sort_by_key(|el| el.0);

    let root_causes = root_causes
        .iter()
        .map(|el| el.1.to_owned())
        .collect::<Vec<_>>();

    let json = json!(root_causes);

    std::fs::write("/tmp/anm.json", json.to_string()).unwrap();
}

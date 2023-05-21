use plotters::prelude::*;
use std::collections::HashMap;

pub fn plot_file_graph(
    file_name: &str,
    point_data: &HashMap<&&str, Vec<Vec<f64>>>,
    result_data: &[(&&str, f64)],
) {
    let graph_file_name = format!("./graphs/{}.png", file_name);
    let root = BitMapBackend::new(&graph_file_name, (4444, 2500)).into_drawing_area();
    root.fill(&WHITE).unwrap();

    let mut result_data = result_data.to_vec();

    result_data.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Less));

    let mut areas = root.split_evenly(match result_data.len() {
        1 => (1, 1),
        2 => (1, 2),
        3 => (1, 3),
        4 => (2, 2),
        5 => (2, 3),
        6 => (2, 3),
        7 => (2, 4),
        8 => (2, 4),
        9 => (3, 3),
        10 => (3, 4),
        11 => (3, 4),
        12 => (3, 4),
        13 => (3, 5),
        14 => (3, 5),
        15 => (3, 5),
        16 => (4, 4),
        17 => (4, 5),
        18 => (4, 5),
        19 => (4, 5),
        20 => (4, 5),
        21 => (4, 6),
        22 => (4, 6),
        23 => (4, 6),
        24 => (4, 6),
        25 => (5, 5),
        26 => (5, 6),
        27 => (5, 6),
        28 => (5, 6),
        29 => (5, 6),
        30 => (5, 6),
        31 => (5, 7),
        32 => (5, 7),
        33 => (5, 7),
        34 => (5, 7),
        35 => (5, 7),
        36 => (6, 6),
        37 => (6, 7),
        38 => (6, 7),
        39 => (6, 7),
        40 => (6, 7),
        41 => (6, 7),
        42 => (6, 7),
        43 => (6, 8),
        44 => (6, 8),
        45 => (6, 8),
        46 => (6, 8),
        47 => (6, 8),
        48 => (6, 8),
        49 => (7, 7),
        50 => (7, 8),
        _ => {
            println!("{} has {} root causes", file_name, result_data.len());
            (7, 8)
        }
    });

    for (area, metrics) in areas.iter_mut().zip(result_data.iter()) {
        let data = point_data.get(metrics.0).unwrap();
        let min = data
            .iter()
            .map(|v| v.iter().copied().reduce(f64::min).unwrap())
            .reduce(f64::min)
            .unwrap();
        let max = data
            .iter()
            .map(|v| v.iter().copied().reduce(f64::max).unwrap())
            .reduce(f64::max)
            .unwrap();
        let area = area.titled(metrics.0, ("sans-serif", 40)).unwrap();
        let mut chart = ChartBuilder::on(&area)
            .margin(10)
            .caption(metrics.1.to_string(), ("sans-serif", 30))
            .y_label_area_size(30)
            .build_cartesian_2d(0..30, min..max)
            .unwrap();

        chart.configure_mesh().draw().unwrap();
        chart
            .draw_series(LineSeries::new(
                data[0].iter().enumerate().map(|(i, v)| (i as i32, *v)),
                &RED,
            ))
            .unwrap();
        chart
            .draw_series(LineSeries::new(
                data[1].iter().enumerate().map(|(i, v)| (i as i32, *v)),
                &BLUE,
            ))
            .unwrap();
    }
}

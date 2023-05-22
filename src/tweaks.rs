use itertools::Itertools;

pub fn get_service_name(file_name: &str) -> String {
    if file_name.starts_with("frontend") {
        return "frontend".to_string();
    }
    file_name.split("service").next().unwrap().to_string()
}

pub fn service_peers_check(services: &[(String, usize)]) -> Option<String> {
    let mut services = services.to_vec();
    services.sort_by(|a, b| b.0.cmp(&a.0));
    let abnormal_peer = services
        .iter()
        .group_by(|(service, _)| get_service_name(service))
        .into_iter()
        .map(|(_, group)| {
            let elems = group.collect_vec();
            let mean = elems.iter().map(|e| e.1).sum::<usize>() / elems.len();
            let max = elems.iter().max_by_key(|e| e.1).unwrap();

            (max.0.to_owned(), max.1, mean)
        })
        .filter(|e| e.1 as f64 > e.2 as f64 * 1.5)
        .max_by_key(|e| e.1);

    match abnormal_peer {
        Some(peer) => Some(peer.0.clone()),
        None => None,
    }
}

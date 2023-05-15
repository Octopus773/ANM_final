use polars::{
    export::chrono::{DateTime, NaiveDateTime, Utc},
    prelude::*,
};
use serde::Deserialize;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

// #[derive(Debug, Deserialize)]
// pub struct ServiceRecord {
//     timestamp: i64,
//     #[serde(rename = "container_network_receive_packets_dropped.eth0")]
//     container_network_receive_packets_dropped_eth0: f64,
//     container_spec_cpu_shares: f64,
//     container_spec_memory_swap_limit_MB: f64,
//     #[serde(rename = "container_fs_writes_MB./dev/vda")]
//     container_fs_writes_MB_dev_vda: f64,
//     #[serde(rename = "container_network_transmit_packets_dropped.eth0")]
//     container_network_transmit_packets_dropped_eth0: f64,
//     #[serde(rename = "container_network_receive_errors.eth0")]
//     container_network_receive_errors_eth0: f64,
//     #[serde(rename = "container_fs_writes_merged./dev/vda1")]
//     container_fs_writes_merged_dev_vda1: f64,
//     container_threads_max: f64,
//     #[serde(rename = "container_fs_write_seconds./dev/vda1")]
//     container_fs_write_seconds_dev_vda1: f64,
//     #[serde(rename = "container_fs_inodes_free./dev/vda1")]
//     container_fs_inodes_free_dev_vda1: f64,
//     #[serde(rename = "container_fs_sector_reads./dev/vda1")]
//     container_fs_sector_reads_dev_vda1: f64,
//     #[serde(rename = "container_network_transmit_packets.eth0")]
//     container_network_transmit_packets_eth0: f64,
//     container_cpu_usage_seconds: f64,
//     container_cpu_cfs_throttled_periods: f64,
//     container_file_descriptors: f64,
//     container_cpu_cfs_throttled_seconds: f64,
//     #[serde(rename = "container_fs_reads./dev/vda")]
//     container_fs_reads_dev_vda: f64,
//     #[serde(rename = "container_fs_reads./dev/vda1")]
//     container_fs_reads_dev_vda1: f64,
//     #[serde(rename = "container_memory_failures.container.pgfault")]
//     container_memory_failures_container_pgfault: f64,
//     #[serde(rename = "container_memory_failures.container.pgmajfault")]
//     container_memory_failures_container_pgmajfault: f64,
//     #[serde(rename = "container_memory_failures.hierarchy.pgfault")]
//     container_memory_failures_hierarchy_pgfault: f64,
//     #[serde(rename = "container_memory_failures.hierarchy.pgmajfault")]
//     container_memory_failures_hierarchy_pgmajfault: f64,
//     container_memory_max_usage_MB: f64,
//     #[serde(rename = "container_fs_inodes./dev/vda1")]
//     container_fs_inodes_dev_vda1: f64,
//     #[serde(rename = "container_fs_reads_MB./dev/vda")]
//     container_fs_reads_MB_dev_vda: f64,
//     container_last_seen: f64,
//     container_cpu_user_seconds: f64,
//     #[serde(rename = "container_fs_limit_MB./dev/vda1")]
//     container_fs_limit_MB_dev_vda1: f64,
//     #[serde(rename = "container_fs_io_time_weighted_seconds./dev/vda1")]
//     container_fs_io_time_weighted_seconds_dev_vda1: f64,
//     #[serde(rename = "container_fs_sector_writes./dev/vda1")]
//     container_fs_sector_writes_dev_vda1: f64,
//     #[serde(rename = "container_network_receive_packets.eth0")]
//     container_network_receive_packets_eth0: f64,
//     container_memory_working_set_MB: f64,
//     #[serde(rename = "container_fs_io_current./dev/vda1")]
//     container_fs_io_current_dev_vda1: f64,
//     container_memory_usage_MB: f64,
//     #[serde(rename = "container_fs_usage_MB./dev/vda1")]
//     container_fs_usage_MB_dev_vda1: f64,
//     container_sockets: f64,
//     #[serde(rename = "container_fs_io_time_seconds./dev/vda1")]
//     container_fs_io_time_seconds_dev_vda1: f64,
//     container_memory_failcnt: f64,
//     container_spec_cpu_quota: f64,
//     #[serde(rename = "container_fs_writes./dev/vda")]
//     container_fs_writes_dev_vda: f64,
//     #[serde(rename = "container_fs_writes./dev/vda1")]
//     container_fs_writes_dev_vda1: f64,
//     #[serde(rename = "container_ulimits_soft.max_open_files")]
//     container_ulimits_soft_max_open_files: f64,
//     #[serde(rename = "container_network_receive_MB.eth0")]
//     container_network_receive_MB_eth0: f64,
//     #[serde(rename = "container_tasks_state.iowaiting")]
//     container_tasks_state_iowaiting: f64,
//     #[serde(rename = "container_tasks_state.running")]
//     container_tasks_state_running: f64,
//     #[serde(rename = "container_tasks_state.sleeping")]
//     container_tasks_state_sleeping: f64,
//     #[serde(rename = "container_tasks_state.stopped")]
//     container_tasks_state_stopped: f64,
//     #[serde(rename = "container_tasks_state.uninterruptible")]
//     container_tasks_state_uninterruptible: f64,
//     #[serde(rename = "container_fs_read_seconds./dev/vda1")]
//     container_fs_read_seconds_dev_vda1: f64,
//     container_memory_rss: f64,
//     container_start_time_seconds: f64,
//     container_memory_mapped_file: f64,
//     container_spec_memory_reservation_limit_MB: f64,
//     container_memory_cache: f64,
//     container_cpu_cfs_periods: f64,
//     container_spec_cpu_period: f64,
//     container_memory_swap: f64,
//     container_threads: f64,
//     #[serde(rename = "container_network_transmit_errors.eth0")]
//     container_network_transmit_errors_eth0: f64,
//     container_spec_memory_limit_MB: f64,
//     #[serde(rename = "container_fs_reads_merged./dev/vda1")]
//     container_fs_reads_merged_dev_vda1: f64,
//     #[serde(rename = "container_network_transmit_MB.eth0")]
//     container_network_transmit_MB_eth0: f64,
//     container_cpu_system_seconds: f64,
//     container_cpu_load_average_10s: f64,
// }

// fn timestamp_to_datetime(timestamp: Series) -> PolarsResult<Series> {
//     let x = timestamp
//         .u64()?
//         .into_iter()
//         .map(|ts| {
//             if ts.is_none() {
//                 return None;
//             }
//             let datetime = NaiveDateTime::from_timestamp_opt(ts.unwrap() as i64, 0);
//             Some(datetime.unwrap())
//         }).collect::<DatetimeChunked>();
//     Ok(x.into_series())
// }

// .with_schema(SchemaRef::new(Schema::from_iter(vec![
//     Field::new("timestamp", DataType::Int64),
//     Field::new(
//         "container_network_receive_packets_dropped.eth0",
//         DataType::Float64,
//     ),
//     Field::new("container_spec_cpu_shares", DataType::Float64),
//     Field::new("container_spec_memory_swap_limit_MB", DataType::Float64),
//     Field::new("container_fs_writes_MB./dev/vda", DataType::Float64),
//     Field::new(
//         "container_network_transmit_packets_dropped.eth0",
//         DataType::Float64,
//     ),
//     Field::new("container_network_receive_errors.eth0", DataType::Float64),
//     Field::new("container_fs_writes_merged./dev/vda1", DataType::Float64),
//     Field::new("container_threads_max", DataType::Float64),
//     Field::new("container_fs_write_seconds./dev/vda1", DataType::Float64),
//     Field::new("container_fs_inodes_free./dev/vda1", DataType::Float64),
//     Field::new("container_fs_sector_reads./dev/vda1", DataType::Float64),
//     Field::new("container_network_transmit_packets.eth0", DataType::Float64),
//     Field::new("container_cpu_usage_seconds", DataType::Float64),
//     Field::new("container_cpu_cfs_throttled_periods", DataType::Float64),
//     Field::new("container_file_descriptors", DataType::Float64),
//     Field::new("container_cpu_cfs_throttled_seconds", DataType::Float64),
//     Field::new("container_fs_reads./dev/vda", DataType::Float64),
//     Field::new("container_fs_reads./dev/vda1", DataType::Float64),
//     Field::new(
//         "container_memory_failures.container.pgfault",
//         DataType::Float64,
//     ),
//     Field::new(
//         "container_memory_failures.container.pgmajfault",
//         DataType::Float64,
//     ),
//     Field::new(
//         "container_memory_failures.hierarchy.pgfault",
//         DataType::Float64,
//     ),
//     Field::new(
//         "container_memory_failures.hierarchy.pgmajfault",
//         DataType::Float64,
//     ),
//     Field::new("container_memory_max_usage_MB", DataType::Float64),
//     Field::new("container_fs_inodes./dev/vda1", DataType::Float64),
//     Field::new("container_fs_reads_MB./dev/vda", DataType::Float64),
//     Field::new("container_last_seen", DataType::Float64),
//     Field::new("container_cpu_user_seconds", DataType::Float64),
//     Field::new("container_fs_limit_MB./dev/vda1", DataType::Float64),
//     Field::new(
//         "container_fs_io_time_weighted_seconds./dev/vda1",
//         DataType::Float64,
//     ),
//     Field::new("container_fs_sector_writes./dev/vda1", DataType::Float64),
//     Field::new("container_network_receive_packets.eth0", DataType::Float64),
//     Field::new("container_memory_working_set_MB", DataType::Float64),
//     Field::new("container_fs_io_current./dev/vda1", DataType::Float64),
//     Field::new("container_memory_usage_MB", DataType::Float64),
//     Field::new("container_fs_usage_MB./dev/vda1", DataType::Float64),
//     Field::new("container_sockets", DataType::Float64),
//     Field::new("container_fs_io_time_seconds./dev/vda1", DataType::Float64),
//     Field::new("container_memory_failcnt", DataType::Float64),
//     Field::new("container_spec_cpu_quota", DataType::Float64),
//     Field::new("container_fs_writes./dev/vda", DataType::Float64),
//     Field::new("container_fs_writes./dev/vda1", DataType::Float64),
//     Field::new("container_ulimits_soft.max_open_files", DataType::Float64),
//     Field::new("container_network_receive_MB.eth0", DataType::Float64),
//     Field::new("container_tasks_state.iowaiting", DataType::Float64),
//     Field::new("container_tasks_state.running", DataType::Float64),
//     Field::new("container_tasks_state.sleeping", DataType::Float64),
//     Field::new("container_tasks_state.stopped", DataType::Float64),
//     Field::new("container_tasks_state.uninterruptible", DataType::Float64),
//     Field::new("container_fs_read_seconds./dev/vda1", DataType::Float64),
//     Field::new("container_memory_rss", DataType::Float64),
//     Field::new("container_start_time_seconds", DataType::Float64),
//     Field::new("container_memory_mapped_file", DataType::Float64),
//     Field::new(
//         "container_spec_memory_reservation_limit_MB",
//         DataType::Float64,
//     ),
//     Field::new("container_memory_cache", DataType::Float64),
//     Field::new("container_cpu_cfs_periods", DataType::Float64),
//     Field::new("container_spec_cpu_period", DataType::Float64),
//     Field::new("container_memory_swap", DataType::Float64),
//     Field::new("container_threads", DataType::Float64),
//     Field::new("container_network_transmit_errors.eth0", DataType::Float64),
//     Field::new("container_spec_memory_limit_MB", DataType::Float64),
//     Field::new("container_fs_reads_merged./dev/vda1", DataType::Float64),
//     Field::new("container_network_transmit_MB.eth0", DataType::Float64),
//     Field::new("container_cpu_system_seconds", DataType::Float64),
//     Field::new("container_cpu_load_average_10s", DataType::Float64),
// ])))

pub fn read_service_csv(path: &str) -> DataFrame {
    // load csv with polars
    let df = LazyCsvReader::new(path).has_header(true).finish().unwrap();

    let df = df
        .select([col("*").interpolate(InterpolationMethod::Linear)])
        .with_column(
            (col("timestamp") * lit(1000)).cast(DataType::Datetime(TimeUnit::Milliseconds, None)),
        )
        .collect()
        .unwrap();
    df
}

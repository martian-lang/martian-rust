use crate::common::NodeType;
use crate::PipestanceFile;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Perf(pub Vec<PerfElement>);

impl PipestanceFile for Perf {
    fn filename() -> &'static str {
        "_perf"
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObservedMemory {
    pub rss: i64,
    pub shared: i64,
    pub vmem: i64,
    pub text: i64,
    pub stack: i64,
    pub proc_count: i64,
}

/// Performance data for a stage or pipeline execution.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PerfElement {
    /// Name of the execution (stage name or pipeline name)
    pub name: String,
    /// Fully qualified name (e.g. ID.INSITU_DECODING_CS.DECODE_POINTS.fork0)
    pub fqname: String,
    /// Information about memory usage. Only used in pipelines.
    pub highmem: Option<ObservedMemory>,
    /// Information about all forks executed by this node
    pub forks: Vec<ForkPerfInfo>,
    /// History of byte allocations and deletions
    pub bytehist: Option<Vec<NodeByteStamp>>,
    /// Maximum number of bytes allocated
    pub maxbytes: i64,
    /// Type of the node ("stage" or "pipeline")
    #[serde(rename(deserialize = "type"))]
    pub ty: NodeType,
}

/// Core type of a `_perf` file. Contains all information about a stage execution.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PerfStats {
    /// The start time of this node, or the earliest start time of any of its
    /// child nodes. Format is YYYY-mm-ddTHH:MM:SS.nano+-HH:MM, e.g.
    /// 2022-01-01T01:01:01.123456789-08:00
    pub start: String,
    /// The end time of this node, or the latest end time of any of its child
    /// nodes.
    pub end: String,
    /// The total time elapsed during event execution [seconds]
    pub walltime: f64,
    /// Number of jobs. For a stage, this is 1.
    pub num_jobs: i32,
    /// Number of threads used during execution
    pub num_threads: i32,
    /// For split/main/join nodes, this should be the same as walltime. For
    /// other nodes, it is the sum of the durations for child nodes.
    pub duration: f64,
    /// Number of threads * duration / 3600 [hours]
    pub core_hours: f64,
    /// Maximum resident set size (portion of memory occupied by process)
    /// [kilobytes]
    pub maxrss: i64,
    /// Maximum number of virtual memory used [kilobytes]
    pub maxvmem: i64,
    /// Number of kilobytes requested [kilobytes]
    pub maxrss_requested: i64,
    /// Number of kilobytes of virtual memory requested [kilobytes]
    pub maxvmem_requested: i64,
    /// The number of times the filesystem had to perform input.
    pub in_blocks: i64,
    /// The number of times the filesystem had to perform output.
    pub out_blocks: i64,
    /// The number of times the filesystem had to perform input or output.
    pub total_blocks: i64,
    /// The average number of times the filesystem had to perform input per
    /// second [events / second].
    pub in_blocks_rate: f64,
    /// The average number of times the filesystem had to perform output per
    /// second [events / second].
    pub out_blocks_rate: f64,
    /// The average number of times the filesystem had to perform input/output
    /// per second [events / second]
    pub total_blocks_rate: f64,
    /// Total number of bytes read in by the stage [bytes]
    pub in_bytes: i64,
    /// Total number of bytes written out by the stage [bytes]
    pub out_bytes: i64,
    /// Average number of bytes read [bytes/second]
    pub in_bytes_rate: f64,
    /// Average number of bytes written [bytes/second]
    pub out_bytes_rate: f64,
    /// Maximum read rate [bytes / second]
    pub in_bytes_peak: f64,
    /// Maximum write rate in [bytes / second]
    pub out_bytes_peak: f64,
    /// The total CPU time used by the user [seconds]
    pub usertime: f64,
    /// The total CPU time used by the system [seconds]
    pub systemtime: f64,
    /// The number of output files and intermediate files (removed by VDR).
    pub total_files: u32,
    /// The number of bytes of disk space used for output files and intermediate
    /// files (removed by VDR).
    pub total_bytes: u64,
    /// The number of output files.
    pub output_files: u32,
    /// The number of bytes of disk space used for output files.
    pub output_bytes: u64,
    /// The total number of intermediate files that were removed by VDR.
    pub vdr_files: u32,
    /// The total number bytes used by intermediate files that were removed by
    /// VDR.
    pub vdr_bytes: u64,
    /// Standard deviation of the read rate [bytes / second]
    pub in_bytes_dev: f64,
    /// Standard deviation of the write rate [bytes / second]
    pub out_bytes_dev: f64,
}

/// pub structure holding information about data flow throughout pipeline.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct NodeByteStamp {
    /// Timestamp of the measurement
    pub ts: String,
    /// Description of the measurement
    pub desc: String,
    /// Number of bytes used for action described in `desc`. E.g., an allocation.
    pub bytes: i64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ForkPerfInfo {
    /// Statistics for the split portion for this fork
    pub split_stats: Option<PerfStats>,
    /// Statistics for the join portion for this fork
    pub join_stats: Option<PerfStats>,
    /// Statistics for the whole fork
    pub fork_stats: PerfStats,
    /// List of the stages executed in this fork
    pub stages: Vec<StagePerfInfo>,
    /// Performance information about each chunk execution
    pub chunks: Vec<ChunkPerfInfo>,
    /// Index of the fork
    pub index: i32,
}

/// Information about a single chunk (executed during main)
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ChunkPerfInfo {
    /// Performance information for the chunk
    pub chunk_stats: PerfStats,
    /// Index identifying the chunk. Each chunk gets a unique index.
    pub index: i32,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct StagePerfInfo {
    /// Name of the stage.
    pub name: String,
    /// Fully qualified name of the stage.
    pub fqname: String,
    /// Index of the fork for this stage. Doesn't need to be the same as it's
    /// parent fork index.
    pub forki: i32,
}

#[cfg(test)]
mod tests {
    use super::Perf;
    use crate::common::read_zst;
    use crate::PipestanceFile;
    use anyhow::Result;

    #[test]
    fn test_perf_deserialize() -> Result<()> {
        let _perf = Perf::from_string(read_zst("test_data/_perf.zst")?)?;
        Ok(())
    }
}

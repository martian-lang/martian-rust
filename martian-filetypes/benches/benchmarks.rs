use criterion::{criterion_group, criterion_main, Benchmark, Criterion, Throughput};
use martian::MartianFileType;
use martian_filetypes::{FileTypeIO, JsonFile, LazyFileTypeIO};

fn run_json_lazy_benchmark(c: &mut Criterion) {
    let elements = 100_000;
    let dir = tempfile::tempdir().unwrap();
    let json_file_full = JsonFile::new(dir.path(), "benchmark_full");
    let json_file_lazy = JsonFile::new(dir.path(), "benchmark_lazy");
    let data: Vec<_> = (0..elements).into_iter().collect();
    json_file_full.write(&data).unwrap();
    json_file_lazy.write(&data).unwrap();

    c.bench(
        "bench-json-lazy-read",
        Benchmark::new("full-read", move |b| {
            b.iter(|| {
                let decoded: Vec<i32> = json_file_full.read().unwrap();
                decoded
            })
        })
        .with_function("lazy-read", move |b| {
            b.iter(|| {
                let decoded: Vec<i32> = json_file_lazy
                    .lazy_reader()
                    .unwrap()
                    .map(|x| x.unwrap())
                    .collect();
                decoded
            })
        })
        .sample_size(10)
        .throughput(Throughput::Elements(elements)),
    );
}

criterion_group!(benches, run_json_lazy_benchmark);

criterion_main!(benches);

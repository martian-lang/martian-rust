use criterion::{criterion_group, criterion_main, Benchmark, Criterion, Throughput};
use martian::MartianFileType;
use martian_filetypes::{FileTypeIO, JsonFile, LazyFileTypeIO, LazyWrite};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Default, Clone)]
struct Foo {
    a: i32,
    b: usize,
    c: String,
    d: Vec<bool>,
}

fn json_lazy_read_bench(c: &mut Criterion) {
    let elements = 100_000;
    let dir = tempfile::tempdir().unwrap();
    let json_file_full = JsonFile::new(dir.path(), "benchmark_full");
    let json_file_lazy = JsonFile::new(dir.path(), "benchmark_lazy");
    let data: Vec<_> = vec![Foo::default(); elements as usize];
    json_file_full.write(&data).unwrap();
    json_file_lazy.write(&data).unwrap();

    c.bench(
        "bench-json-lazy-read",
        Benchmark::new("full-read", move |b| {
            b.iter(|| {
                let decoded: Vec<Foo> = json_file_full.read().unwrap();
                decoded
            })
        })
        .with_function("lazy-read", move |b| {
            b.iter(|| {
                let decoded: Vec<Foo> = json_file_lazy
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

fn json_lazy_write_bench(c: &mut Criterion) {
    let elements = 100_000;
    let dir = tempfile::tempdir().unwrap();
    let json_file_full = JsonFile::new(dir.path(), "benchmark_full");
    let json_file_lazy = JsonFile::new(dir.path(), "benchmark_lazy");
    let data = vec![Foo::default(); elements as usize];
    let foo = Foo::default();

    c.bench(
        "bench-json-lazy-write",
        Benchmark::new("full-write", move |b| {
            b.iter(|| json_file_full.write(&data))
        })
        .with_function("lazy-write", move |b| {
            b.iter(|| {
                let mut writer = json_file_lazy.lazy_writer().unwrap();
                for _ in 0..elements {
                    writer.write_item(&foo).unwrap();
                }
                writer.finish()
            })
        })
        .sample_size(10)
        .throughput(Throughput::Elements(elements)),
    );
}

criterion_group!(benches, json_lazy_read_bench, json_lazy_write_bench);

criterion_main!(benches);

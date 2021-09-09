use criterion::{criterion_group, criterion_main, Benchmark, Criterion, Throughput};
use martian::MartianFileType;
use martian_filetypes::bin_file::BincodeFile;
use martian_filetypes::json_file::JsonFile;
use martian_filetypes::lz4_file::Lz4;
use martian_filetypes::{FileTypeIO, LazyFileTypeIO, LazyWrite};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Default, Clone)]
struct Foo {
    a: i32,
    b: usize,
    c: String,
    d: Vec<bool>,
}

fn lazy_read_bench<F, T>(c: &mut Criterion, data: Vec<T>, key: &'static str)
where
    F: FileTypeIO<Vec<T>> + LazyFileTypeIO<T> + 'static,
    Lz4<F>: FileTypeIO<Vec<T>> + LazyFileTypeIO<T>,
{
    let dir = tempfile::tempdir().unwrap();
    let file_full = F::new(dir.path(), "benchmark_full");
    let file_lazy = F::new(dir.path(), "benchmark_lazy");
    let file_lz4 = Lz4::<F>::new(dir.path(), "benchmark_lz4");
    let file_lz4_lazy = Lz4::<F>::new(dir.path(), "benchmark_lz4_lazy");
    file_full.write(&data).unwrap();
    file_lazy.write(&data).unwrap();
    file_lz4.write(&data).unwrap();
    file_lz4_lazy.write(&data).unwrap();

    c.bench(
        key,
        Benchmark::new("full-read", move |b| {
            b.iter(|| {
                let decoded: Vec<T> = file_full.read().unwrap();
                decoded
            })
        })
        .with_function("lazy-read", move |b| {
            b.iter(|| {
                let decoded: Vec<T> = file_lazy.read_all().unwrap();
                decoded
            })
        })
        .with_function("lz4-read", move |b| {
            b.iter(|| {
                let decoded: Vec<T> = file_lz4.read().unwrap();
                decoded
            })
        })
        .with_function("lz4-lazy-read", move |b| {
            b.iter(|| {
                let decoded: Vec<T> = file_lz4_lazy.read_all().unwrap();
                decoded
            })
        })
        .sample_size(10)
        .throughput(Throughput::Elements(data.len() as u64)),
    );
}

fn lazy_write_bench<F, T>(c: &mut Criterion, data: Vec<T>, key: &'static str)
where
    F: FileTypeIO<Vec<T>> + LazyFileTypeIO<T> + 'static,
    Lz4<F>: FileTypeIO<Vec<T>> + LazyFileTypeIO<T>,
    T: Clone + 'static,
{
    let dir = tempfile::tempdir().unwrap();
    let file_full = F::new(dir.path(), "benchmark_full");
    let file_lazy = F::new(dir.path(), "benchmark_lazy");
    let file_lz4 = Lz4::<F>::new(dir.path(), "benchmark_lz4");
    let file_lz4_lazy = Lz4::<F>::new(dir.path(), "benchmark_lz4_lazy");
    let data_copy1 = data.clone();
    let data_copy2 = data.clone();
    let data_copy3 = data.clone();
    let elements = data.len() as u32;

    c.bench(
        key,
        Benchmark::new("full-write", move |b| b.iter(|| file_full.write(&data)))
            .with_function("lazy-write", move |b| {
                b.iter(|| {
                    let mut writer = file_lazy.lazy_writer().unwrap();
                    for d in &data_copy1 {
                        writer.write_item(d).unwrap();
                    }
                    writer.finish()
                })
            })
            .with_function("lz4-write", move |b| b.iter(|| file_lz4.write(&data_copy2)))
            .with_function("lz4-lazy-write", move |b| {
                b.iter(|| {
                    let mut writer = file_lz4_lazy.lazy_writer().unwrap();
                    for d in &data_copy3 {
                        writer.write_item(d).unwrap();
                    }
                    writer.finish()
                })
            })
            .sample_size(10)
            .throughput(Throughput::Elements(elements.into())),
    );
}

fn json_lazy_read_bench(c: &mut Criterion) {
    let data: Vec<_> = vec![Foo::default(); 100_000];
    lazy_read_bench::<JsonFile, _>(c, data, "bench-json-lazy-read");
}

fn bincode_lazy_read_bench(c: &mut Criterion) {
    let data: Vec<_> = vec![Foo::default(); 100_000];
    lazy_read_bench::<BincodeFile, _>(c, data, "bench-bincode-lazy-read");
}

fn json_lazy_write_bench(c: &mut Criterion) {
    let data: Vec<_> = vec![Foo::default(); 100_000];
    lazy_write_bench::<JsonFile, _>(c, data, "bench-json-lazy-write");
}

fn bincode_lazy_write_bench(c: &mut Criterion) {
    let data: Vec<_> = vec![Foo::default(); 100_000];
    lazy_write_bench::<BincodeFile, _>(c, data, "bench-bincode-lazy-write");
}

criterion_group!(
    benches,
    json_lazy_read_bench,
    json_lazy_write_bench,
    bincode_lazy_read_bench,
    bincode_lazy_write_bench
);

criterion_main!(benches);

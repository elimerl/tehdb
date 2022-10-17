use criterion::{black_box, criterion_group, criterion_main, Criterion};
use smallvec::smallvec;
use tehdb::database::{Database, ElementType, Row, Schema};

pub fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("insert 1000 rows", |b| {
        b.iter(|| {
            let mut db = Database::new(Schema::new(vec![("id".into(), ElementType::I32)], 0));

            for i in 0..1000 {
                let row = black_box(Row::new(smallvec![i.into()]));

                db.insert(row).unwrap();
            }
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);

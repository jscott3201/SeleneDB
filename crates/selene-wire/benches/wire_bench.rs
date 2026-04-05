//! Benchmarks for selene-wire: frame encode/decode, serialization, compression, datagrams.

use std::collections::HashMap;

use bytes::Bytes;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use selene_core::Value;
use selene_testing::bench_profiles::bench_profile;
use selene_wire::datagram::TelemetryDatagram;
use selene_wire::dto::entity::NodeDto;
use selene_wire::frame::Frame;
use selene_wire::serialize::{deserialize_payload, serialize_payload};
use selene_wire::{MsgType, WireFlags};
use smol_str::SmolStr;

const MESSAGE_SIZES: &[u64] = &[64, 256, 1024, 4096, 16_384, 65_536, 262_144];

fn profile_criterion() -> Criterion {
    bench_profile().into_criterion()
}

// ── Frame Encode/Decode ─────────────────────────────────────────────────

fn bench_frame_encode(c: &mut Criterion) {
    let mut group = c.benchmark_group("frame_encode");
    for &size in MESSAGE_SIZES {
        group.throughput(Throughput::Bytes(size));
        let payload = Bytes::from(vec![0xABu8; size as usize]);
        let frame = Frame::new(MsgType::GqlQuery, WireFlags::empty(), payload).unwrap();
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, _s| {
            b.iter(|| {
                let encoded = frame.encode();
                std::hint::black_box(encoded.len());
            });
        });
    }
    group.finish();
}

fn bench_frame_decode(c: &mut Criterion) {
    let mut group = c.benchmark_group("frame_decode_header");
    for &size in MESSAGE_SIZES {
        group.throughput(Throughput::Bytes(size));
        let payload = Bytes::from(vec![0xABu8; size as usize]);
        let frame = Frame::new(MsgType::GqlQuery, WireFlags::empty(), payload).unwrap();
        let encoded = frame.encode();
        let header: [u8; 6] = encoded[..6].try_into().unwrap();
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, _s| {
            b.iter(|| {
                let (msg_type, flags, _len) = Frame::decode_header(&header).unwrap();
                std::hint::black_box((msg_type, flags));
            });
        });
    }
    group.finish();
}

// ── Postcard Serialization ──────────────────────────────────────────────

fn bench_serialize_node_dto(c: &mut Criterion) {
    let mut group = c.benchmark_group("serialize_node_dto");
    let node = NodeDto {
        id: 42,
        labels: vec!["sensor".into(), "temperature".into(), "building_a".into()],
        properties: {
            let mut p = HashMap::new();
            p.insert("unit".to_string(), Value::String(SmolStr::new("°F")));
            p.insert("value".to_string(), Value::Float(72.5));
            p.insert("floor".to_string(), Value::Int(3));
            p.insert(
                "location".to_string(),
                Value::String(SmolStr::new("Room 301A")),
            );
            p
        },
        created_at: 1_000_000_000,
        updated_at: 2_000_000_000,
        version: 5,
    };

    group.bench_function("postcard", |b| {
        b.iter(|| {
            let bytes = serialize_payload(&node, WireFlags::empty()).unwrap();
            std::hint::black_box(bytes.len());
        });
    });

    group.bench_function("json", |b| {
        b.iter(|| {
            let bytes = serialize_payload(&node, WireFlags::JSON_FORMAT).unwrap();
            std::hint::black_box(bytes.len());
        });
    });

    group.finish();
}

fn bench_deserialize_node_dto(c: &mut Criterion) {
    let mut group = c.benchmark_group("deserialize_node_dto");
    let node = NodeDto {
        id: 42,
        labels: vec!["sensor".into(), "temperature".into()],
        properties: {
            let mut p = HashMap::new();
            p.insert("unit".to_string(), Value::String(SmolStr::new("°F")));
            p.insert("value".to_string(), Value::Float(72.5));
            p
        },
        created_at: 1_000_000_000,
        updated_at: 2_000_000_000,
        version: 5,
    };

    let postcard_bytes = serialize_payload(&node, WireFlags::empty()).unwrap();
    let json_bytes = serialize_payload(&node, WireFlags::JSON_FORMAT).unwrap();

    group.bench_function("postcard", |b| {
        b.iter(|| {
            let dto: NodeDto = deserialize_payload(&postcard_bytes, WireFlags::empty()).unwrap();
            std::hint::black_box(dto.id);
        });
    });

    group.bench_function("json", |b| {
        b.iter(|| {
            let dto: NodeDto = deserialize_payload(&json_bytes, WireFlags::JSON_FORMAT).unwrap();
            std::hint::black_box(dto.id);
        });
    });

    group.finish();
}

// ── Zstd Compression ────────────────────────────────────────────────────

fn bench_zstd_compress(c: &mut Criterion) {
    let mut group = c.benchmark_group("zstd_compress");
    // Use realistic compressible payloads (repeated JSON-like patterns)
    for &size in MESSAGE_SIZES {
        group.throughput(Throughput::Bytes(size));
        let pattern = r#"{"id":42,"labels":["sensor"],"properties":{"value":72.5}}"#;
        let payload: Vec<u8> = pattern
            .as_bytes()
            .iter()
            .cycle()
            .take(size as usize)
            .copied()
            .collect();

        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, _s| {
            b.iter(|| {
                let compressed = zstd::bulk::compress(&payload, 1).unwrap();
                std::hint::black_box(compressed.len());
            });
        });
    }
    group.finish();
}

fn bench_zstd_decompress(c: &mut Criterion) {
    let mut group = c.benchmark_group("zstd_decompress");
    for &size in MESSAGE_SIZES {
        group.throughput(Throughput::Bytes(size));
        let pattern = r#"{"id":42,"labels":["sensor"],"properties":{"value":72.5}}"#;
        let payload: Vec<u8> = pattern
            .as_bytes()
            .iter()
            .cycle()
            .take(size as usize)
            .copied()
            .collect();
        let compressed = zstd::bulk::compress(&payload, 1).unwrap();

        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, _s| {
            b.iter(|| {
                let decompressed = zstd::bulk::decompress(&compressed, size as usize * 2).unwrap();
                std::hint::black_box(decompressed.len());
            });
        });
    }
    group.finish();
}

// ── Datagram Codec ──────────────────────────────────────────────────────

fn bench_datagram_codec(c: &mut Criterion) {
    let mut group = c.benchmark_group("datagram_codec");
    let dg = TelemetryDatagram {
        entity_id: 42,
        property: "temperature".into(),
        timestamp_nanos: 1_000_000_000_000,
        value: 72.5,
    };
    let encoded = dg.encode();

    group.bench_function("encode_single", |b| {
        b.iter(|| {
            let bytes = dg.encode();
            std::hint::black_box(bytes.len());
        });
    });

    group.bench_function("decode_single", |b| {
        b.iter(|| {
            let decoded = TelemetryDatagram::decode(&encoded).unwrap();
            std::hint::black_box(decoded.entity_id);
        });
    });

    group.bench_function("encode_batch_64", |b| {
        let batch: Vec<TelemetryDatagram> = (0..64)
            .map(|i| TelemetryDatagram {
                entity_id: i,
                property: "temp".into(),
                timestamp_nanos: 1_000_000_000_000 + i as i64 * 1_000_000,
                value: 72.0 + i as f64 * 0.1,
            })
            .collect();
        b.iter(|| {
            let total: usize = batch.iter().map(|d| d.encode().len()).sum();
            std::hint::black_box(total);
        });
    });

    group.bench_function("roundtrip_single", |b| {
        b.iter(|| {
            let bytes = dg.encode();
            let decoded = TelemetryDatagram::decode(&bytes).unwrap();
            std::hint::black_box(decoded.value);
        });
    });

    // Small DTO serialization (common hot path)
    let dto = NodeDto {
        id: 42,
        labels: vec!["sensor".into()],
        properties: std::collections::HashMap::new(),
        created_at: 1000,
        updated_at: 2000,
        version: 1,
    };
    group.bench_function("serialize_node_dto", |b| {
        b.iter(|| {
            let bytes = serialize_payload(&dto, WireFlags::empty()).unwrap();
            std::hint::black_box(bytes.len());
        });
    });

    group.finish();
}

criterion_group! {
    name = framing;
    config = profile_criterion();
    targets = bench_frame_encode, bench_frame_decode
}
criterion_group! {
    name = serialization;
    config = profile_criterion();
    targets = bench_serialize_node_dto, bench_deserialize_node_dto
}
criterion_group! {
    name = compression;
    config = profile_criterion();
    targets = bench_zstd_compress, bench_zstd_decompress
}
criterion_group! {
    name = datagrams;
    config = profile_criterion();
    targets = bench_datagram_codec
}
criterion_main!(framing, serialization, compression, datagrams);

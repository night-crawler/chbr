use bloch::parse::block::parse_single;
use bloch::value::MapIterator;
use testresult::TestResult;

const _SQL: &str = r#"
set session_timezone = 'UTC';

drop table if exists activity_hw;

create table activity_hw
(
    first_seen     DateTime,
    last_seen      DateTime,
    name           LowCardinality(String),
    resource_attrs Map(LowCardinality(String), LowCardinality(String)),
    scope_attrs    Map(LowCardinality(String), LowCardinality(String)),
    attrs          Map(LowCardinality(String), LowCardinality(String)),
    type           LowCardinality(String),
    temporality    LowCardinality(String),
    is_monotonic   Bool
) engine = MergeTree order by tuple();

insert into activity_hw values
    ('2025-07-02 15:28:01', '2025-07-02 15:31:02', 'hw.gpu.temp',
     {'profiling.host.name': 'gpu-devbox', 'profiling.host.tags': '', 'container.name': '', 'zymtrace.project': 'default', 'service.name': 'zymtrace-profiler', 'k8s.pod.name': '', 'device.id': 'GPU-d1d3515f-be46-1445-5748-cc2e226b719f', 'k8s.namespace.name': '', 'service.version': '25.6.10'},
     {'profiler.revision': 'unknown', 'profiler.build_timestamp': 'unknown'},
     {}, 'gauge', 'unspecified', false),
    ('2025-07-02 15:28:01', '2025-07-02 15:31:02', 'hw.gpu.utilization',
     {'profiling.host.name': 'gpu-devbox', 'profiling.host.tags': '', 'container.name': '', 'zymtrace.project': 'default', 'service.name': 'zymtrace-profiler', 'k8s.pod.name': '', 'device.id': 'GPU-d1d3515f-be46-1445-5748-cc2e226b719f', 'k8s.namespace.name': '', 'service.version': '25.6.10'},
     {'profiler.revision': 'unknown', 'profiler.build_timestamp': 'unknown'},
     {}, 'gauge', 'unspecified', false),
    ('2025-07-02 15:28:01', '2025-07-02 15:31:02', 'hw.gpu.memory.utilization',
     {'profiling.host.name': 'gpu-devbox', 'profiling.host.tags': '', 'container.name': '', 'zymtrace.project': 'default', 'service.name': 'zymtrace-profiler', 'k8s.pod.name': '', 'device.id': 'GPU-d1d3515f-be46-1445-5748-cc2e226b719f', 'k8s.namespace.name': '', 'service.version': '25.6.10'},
     {'profiler.revision': 'unknown', 'profiler.build_timestamp': 'unknown'},
     {}, 'gauge', 'unspecified', false),
    ('2025-07-02 15:28:01', '2025-07-02 15:31:02', 'hw.gpu.pcie.rx',
     {'profiling.host.name': 'gpu-devbox', 'profiling.host.tags': '', 'container.name': '', 'zymtrace.project': 'default', 'service.name': 'zymtrace-profiler', 'k8s.pod.name': '', 'device.id': 'GPU-d1d3515f-be46-1445-5748-cc2e226b719f', 'k8s.namespace.name': '', 'service.version': '25.6.10'},
     {'profiler.revision': 'unknown', 'profiler.build_timestamp': 'unknown'},
     {}, 'gauge', 'unspecified', false),
    ('2025-07-02 15:28:01', '2025-07-02 15:31:02', 'hw.gpu.power_limit',
     {'profiling.host.name': 'gpu-devbox', 'profiling.host.tags': '', 'container.name': '', 'zymtrace.project': 'default', 'service.name': 'zymtrace-profiler', 'k8s.pod.name': '', 'device.id': 'GPU-d1d3515f-be46-1445-5748-cc2e226b719f', 'k8s.namespace.name': '', 'service.version': '25.6.10'},
     {'profiler.revision': 'unknown', 'profiler.build_timestamp': 'unknown'},
     {}, 'gauge', 'unspecified', false),
    ('2025-07-02 15:28:01', '2025-07-02 15:31:02', 'hw.gpu.power',
     {'profiling.host.name': 'gpu-devbox', 'profiling.host.tags': '', 'container.name': '', 'zymtrace.project': 'default', 'service.name': 'zymtrace-profiler', 'k8s.pod.name': '', 'device.id': 'GPU-d1d3515f-be46-1445-5748-cc2e226b719f', 'k8s.namespace.name': '', 'service.version': '25.6.10'},
     {'profiler.revision': 'unknown', 'profiler.build_timestamp': 'unknown'},
     {}, 'gauge', 'unspecified', false),
    ('2025-07-02 15:28:01', '2025-07-02 15:31:02', 'hw.gpu.pcie.tx',
     {'profiling.host.name': 'gpu-devbox', 'profiling.host.tags': '', 'container.name': '', 'zymtrace.project': 'default', 'service.name': 'zymtrace-profiler', 'k8s.pod.name': '', 'device.id': 'GPU-d1d3515f-be46-1445-5748-cc2e226b719f', 'k8s.namespace.name': '', 'service.version': '25.6.10'},
     {'profiler.revision': 'unknown', 'profiler.build_timestamp': 'unknown'},
     {}, 'gauge', 'unspecified', false);

select * from activity_hw format Native;
"#;

#[test]
fn lc_empty_string_bug() -> TestResult {
    let data = std::fs::read(crate::common::fixture("activity_hw.native"))?;
    let (rem, block) = parse_single(&data)?;
    assert!(rem.is_empty());

    let marker = block.mark("resource_attrs")?;
    for i in 0..block.num_rows {
        let map_it: MapIterator<&str, &str> = marker.get(i)?.unwrap().try_into()?;
        for kv in map_it {
            assert!(kv.is_ok(), "empty strings should not be Value::Empty");
        }
    }

    Ok(())
}

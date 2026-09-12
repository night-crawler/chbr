use bloch::FromBlock;
use bloch::parse::block::parse_single;
use bloch::reader::{LcStr, Map};

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

#[derive(FromBlock, Copy, Clone)]
struct Row<'a> {
    resource_attrs: Map<'a, LcStr<'a>, LcStr<'a>>,
}

#[test]
fn preserves_empty_strings_in_low_cardinality_maps() -> Result<(), Box<dyn std::error::Error>> {
    let data = std::fs::read(crate::common::fixture("activity_hw.native"))?;
    let (remaining, block) = parse_single(&data)?;
    assert!(remaining.is_empty());
    for row in Row::rows(&block)? {
        row?.resource_attrs.collect::<bloch::Result<Vec<_>>>()?;
    }
    Ok(())
}

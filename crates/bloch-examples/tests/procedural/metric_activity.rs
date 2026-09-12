use std::collections::HashMap;

use bloch::parse::block::parse_single;
use bloch::value::MapIterator;
use testresult::TestResult;

const _SQL: &str = r#"
drop table if exists metric_activity;

create table metric_activity
(
    first_seen      DateTime,
    last_seen       DateTime,
    name            LowCardinality(String),
    resource_attrs  Map(LowCardinality(String), LowCardinality(String)),
    scope_attrs     Map(LowCardinality(String), LowCardinality(String)),
    attrs           Map(LowCardinality(String), LowCardinality(String)),
    type            LowCardinality(String),
    temporality     LowCardinality(String),
    is_monotonic    Bool
) engine = MergeTree order by tuple();

insert into metric_activity values
    (toDateTime(1749729493),toDateTime(1749741163),'system.cpu.time',{},{},{'state':'wait','cpu':'cpu15'},'sum','cumulative',true),
    (toDateTime(1749729493),toDateTime(1749741163),'system.filesystem.inodes.usage',{},{},{'device':'/dev/dm-3','state':'used','mountpoint':'/home','mode':'rw','type':'ext4'},'sum','cumulative',false),
    (toDateTime(1749729493),toDateTime(1749741163),'system.network.connections',{},{},{'protocol':'tcp','state':'LISTEN'},'sum','cumulative',false),
    (toDateTime(1749729493),toDateTime(1749741163),'system.network.io',{},{},{'device':'vethd73fd86','direction':'transmit'},'sum','cumulative',true),
    (toDateTime(1749729493),toDateTime(1749741163),'system.cpu.time',{},{},{'state':'wait','cpu':'cpu10'},'sum','cumulative',true),
    (toDateTime(1749729493),toDateTime(1749741163),'system.disk.merged',{},{},{'direction':'write','device':'dm-3'},'sum','cumulative',true),
    (toDateTime(1749729493),toDateTime(1749741163),'system.network.packets',{},{},{'direction':'transmit','device':'veth8473ec4'},'sum','cumulative',true),
    (toDateTime(1749729493),toDateTime(1749741163),'system.cpu.time',{},{},{'state':'wait','cpu':'cpu13'},'sum','cumulative',true),
    (toDateTime(1749729493),toDateTime(1749741163),'system.cpu.time',{},{},{'state':'wait','cpu':'cpu0'},'sum','cumulative',true),
    (toDateTime(1749729493),toDateTime(1749741163),'system.disk.pending_operations',{},{},{'device':'dm-3'},'sum','cumulative',false),
    (toDateTime(1749729493),toDateTime(1749741163),'system.filesystem.usage',{},{},{'device':'/dev/nvme0n1p2','mountpoint':'/efi','type':'vfat','mode':'rw','state':'reserved'},'sum','cumulative',false),
    (toDateTime(1749729493),toDateTime(1749741163),'system.cpu.time',{},{},{'state':'nice','cpu':'cpu14'},'sum','cumulative',true),
    (toDateTime(1749729493),toDateTime(1749741163),'system.cpu.time',{},{},{'cpu':'cpu12','state':'nice'},'sum','cumulative',true),
    (toDateTime(1749729493),toDateTime(1749741163),'system.cpu.time',{},{},{'state':'system','cpu':'cpu2'},'sum','cumulative',true),
    (toDateTime(1749729493),toDateTime(1749741163),'system.disk.operations',{},{},{'direction':'write','device':'nvme0n1'},'sum','cumulative',true),
    (toDateTime(1749729493),toDateTime(1749741163),'system.disk.weighted_io_time',{},{},{'device':'nvme0n1p1'},'sum','cumulative',true),
    (toDateTime(1749729493),toDateTime(1749741163),'system.cpu.time',{},{},{'state':'system','cpu':'cpu11'},'sum','cumulative',true),
    (toDateTime(1749729493),toDateTime(1749741163),'system.network.io',{},{},{'direction':'receive','device':'docker0'},'sum','cumulative',true),
    (toDateTime(1749729493),toDateTime(1749741163),'system.cpu.time',{},{},{'cpu':'cpu0','state':'system'},'sum','cumulative',true),
    (toDateTime(1749729493),toDateTime(1749741163),'system.disk.operations',{},{},{'direction':'read','device':'dm-0'},'sum','cumulative',true),
    (toDateTime(1749729493),toDateTime(1749741163),'system.network.errors',{},{},{'device':'wlp1s0','direction':'receive'},'sum','cumulative',true),
    (toDateTime(1749729493),toDateTime(1749741163),'system.cpu.time',{},{},{'cpu':'cpu12','state':'softirq'},'sum','cumulative',true),
    (toDateTime(1749729493),toDateTime(1749741163),'system.cpu.time',{},{},{'cpu':'cpu8','state':'system'},'sum','cumulative',true),
    (toDateTime(1749729493),toDateTime(1749741163),'system.disk.io',{},{},{'device':'dm-3','direction':'read'},'sum','cumulative',true),
    (toDateTime(1749729493),toDateTime(1749741163),'system.cpu.time',{},{},{'cpu':'cpu5','state':'wait'},'sum','cumulative',true),
    (toDateTime(1749729493),toDateTime(1749741163),'system.cpu.time',{},{},{'cpu':'cpu3','state':'nice'},'sum','cumulative',true),
    (toDateTime(1749729493),toDateTime(1749741163),'system.network.connections',{},{},{'protocol':'tcp','state':'SYN_SENT'},'sum','cumulative',false),
    (toDateTime(1749729493),toDateTime(1749741163),'system.disk.merged',{},{},{'device':'dm-0','direction':'read'},'sum','cumulative',true),
    (toDateTime(1749729493),toDateTime(1749741163),'system.network.connections',{},{},{'state':'CLOSE','protocol':'tcp'},'sum','cumulative',false),
    (toDateTime(1749729493),toDateTime(1749741163),'system.paging.faults',{},{},{'type':'major'},'sum','cumulative',true),
    (toDateTime(1749729493),toDateTime(1749741163),'system.network.io',{},{},{'device':'veth8473ec4','direction':'transmit'},'sum','cumulative',true),
    (toDateTime(1749729493),toDateTime(1749741163),'system.network.errors',{},{},{'device':'lo','direction':'receive'},'sum','cumulative',true),
    (toDateTime(1749729493),toDateTime(1749741163),'system.paging.operations',{},{},{'direction':'page_in','type':'minor'},'sum','cumulative',true),
    (toDateTime(1749729493),toDateTime(1749741163),'system.filesystem.usage',{},{},{'device':'/dev/dm-2','mode':'rw','state':'used','mountpoint':'/','type':'ext4'},'sum','cumulative',false),
    (toDateTime(1749729493),toDateTime(1749741163),'system.disk.merged',{},{},{'device':'dm-3','direction':'read'},'sum','cumulative',true),
    (toDateTime(1749729493),toDateTime(1749741163),'system.cpu.load_average.5m',{},{},{},'gauge','unspecified',false),
    (toDateTime(1749729493),toDateTime(1749741163),'system.disk.weighted_io_time',{},{},{'device':'nvme0n1p2'},'sum','cumulative',true),
    (toDateTime(1749729493),toDateTime(1749741163),'system.cpu.time',{},{},{'cpu':'cpu6','state':'steal'},'sum','cumulative',true),
    (toDateTime(1749729493),toDateTime(1749741163),'system.cpu.time',{},{},{'state':'interrupt','cpu':'cpu4'},'sum','cumulative',true),
    (toDateTime(1749729493),toDateTime(1749741163),'system.cpu.time',{},{},{'state':'steal','cpu':'cpu8'},'sum','cumulative',true),
    (toDateTime(1749729493),toDateTime(1749741163),'system.cpu.time',{},{},{'cpu':'cpu8','state':'nice'},'sum','cumulative',true),
    (toDateTime(1749729493),toDateTime(1749741163),'system.cpu.time',{},{},{'cpu':'cpu0','state':'nice'},'sum','cumulative',true),
    (toDateTime(1749729493),toDateTime(1749741163),'system.cpu.time',{},{},{'cpu':'cpu4','state':'softirq'},'sum','cumulative',true),
    (toDateTime(1749729493),toDateTime(1749741163),'system.disk.pending_operations',{},{},{'device':'nvme0n1p3'},'sum','cumulative',false),
    (toDateTime(1749729493),toDateTime(1749741163),'system.cpu.time',{},{},{'state':'system','cpu':'cpu15'},'sum','cumulative',true),
    (toDateTime(1749729493),toDateTime(1749741163),'system.cpu.time',{},{},{'state':'idle','cpu':'cpu14'},'sum','cumulative',true),
    (toDateTime(1749729493),toDateTime(1749741163),'system.memory.usage',{},{},{'state':'buffered'},'sum','cumulative',false),
    (toDateTime(1749729493),toDateTime(1749741163),'system.network.io',{},{},{'direction':'receive','device':'tailscale0'},'sum','cumulative',true),
    (toDateTime(1749729493),toDateTime(1749741163),'system.cpu.time',{},{},{'state':'steal','cpu':'cpu7'},'sum','cumulative',true),
    (toDateTime(1749729493),toDateTime(1749741163),'system.cpu.time',{},{},{'state':'wait','cpu':'cpu8'},'sum','cumulative',true),
    (toDateTime(1749729493),toDateTime(1749741163),'system.network.io',{},{},{'direction':'transmit','device':'veth09f13de'},'sum','cumulative',true),
    (toDateTime(1749729493),toDateTime(1749741163),'system.disk.operations',{},{},{'direction':'read','device':'nvme0n1p3'},'sum','cumulative',true),
    (toDateTime(1749729493),toDateTime(1749741163),'system.cpu.time',{},{},{'cpu':'cpu12','state':'interrupt'},'sum','cumulative',true),
    (toDateTime(1749729493),toDateTime(1749741163),'system.disk.operations',{},{},{'direction':'read','device':'dm-2'},'sum','cumulative',true),
    (toDateTime(1749729493),toDateTime(1749741163),'system.cpu.time',{},{},{'state':'steal','cpu':'cpu2'},'sum','cumulative',true),
    (toDateTime(1749729493),toDateTime(1749741163),'system.cpu.time',{},{},{'state':'user','cpu':'cpu7'},'sum','cumulative',true),
    (toDateTime(1749729493),toDateTime(1749741163),'system.cpu.time',{},{},{'state':'idle','cpu':'cpu11'},'sum','cumulative',true),
    (toDateTime(1749729493),toDateTime(1749741163),'system.network.errors',{},{},{'device':'tailscale0','direction':'transmit'},'sum','cumulative',true),
    (toDateTime(1749729493),toDateTime(1749741163),'system.cpu.time',{},{},{'cpu':'cpu8','state':'idle'},'sum','cumulative',true),
    (toDateTime(1749729493),toDateTime(1749741163),'system.network.dropped',{},{},{'direction':'receive','device':'veth8473ec4'},'sum','cumulative',true),
    (toDateTime(1749729493),toDateTime(1749741163),'system.network.errors',{},{},{'direction':'transmit','device':'br-478b197d5cec'},'sum','cumulative',true),
    (toDateTime(1749729493),toDateTime(1749741163),'system.network.dropped',{},{},{'device':'docker0','direction':'transmit'},'sum','cumulative',true),
    (toDateTime(1749729493),toDateTime(1749741163),'system.cpu.time',{},{},{'cpu':'cpu6','state':'nice'},'sum','cumulative',true),
    (toDateTime(1749729493),toDateTime(1749741163),'system.cpu.time',{},{},{'state':'idle','cpu':'cpu2'},'sum','cumulative',true),
    (toDateTime(1749729493),toDateTime(1749741163),'system.cpu.time',{},{},{'cpu':'cpu12','state':'idle'},'sum','cumulative',true),
    (toDateTime(1749729493),toDateTime(1749741163),'system.cpu.time',{},{},{'cpu':'cpu4','state':'user'},'sum','cumulative',true),
    (toDateTime(1749729493),toDateTime(1749741163),'system.network.dropped',{},{},{'device':'vethd73fd86','direction':'receive'},'sum','cumulative',true),
    (toDateTime(1749729493),toDateTime(1749741163),'system.cpu.time',{},{},{'state':'system','cpu':'cpu7'},'sum','cumulative',true),
    (toDateTime(1749729493),toDateTime(1749741163),'system.disk.io',{},{},{'direction':'read','device':'nvme0n1'},'sum','cumulative',true),
    (toDateTime(1749729493),toDateTime(1749741163),'system.network.packets',{},{},{'device':'tailscale0','direction':'receive'},'sum','cumulative',true),
    (toDateTime(1749729493),toDateTime(1749741163),'system.disk.operation_time',{},{},{'device':'nvme0n1p3','direction':'write'},'sum','cumulative',true),
    (toDateTime(1749729493),toDateTime(1749741163),'system.disk.io_time',{},{},{'device':'dm-3'},'sum','cumulative',true),
    (toDateTime(1749729493),toDateTime(1749741163),'system.network.dropped',{},{},{'device':'lo','direction':'receive'},'sum','cumulative',true),
    (toDateTime(1749729493),toDateTime(1749741163),'system.cpu.time',{},{},{'state':'softirq','cpu':'cpu7'},'sum','cumulative',true),
    (toDateTime(1749729493),toDateTime(1749741163),'system.disk.io',{},{},{'device':'dm-0','direction':'write'},'sum','cumulative',true),
    (toDateTime(1749729493),toDateTime(1749741163),'system.network.packets',{},{},{'direction':'transmit','device':'tailscale0'},'sum','cumulative',true),
    (toDateTime(1749729493),toDateTime(1749741163),'system.cpu.time',{},{},{'state':'idle','cpu':'cpu3'},'sum','cumulative',true),
    (toDateTime(1749729493),toDateTime(1749741163),'system.cpu.time',{},{},{'cpu':'cpu0','state':'steal'},'sum','cumulative',true),
    (toDateTime(1749729493),toDateTime(1749741163),'system.network.packets',{},{},{'device':'wlp1s0','direction':'transmit'},'sum','cumulative',true),
    (toDateTime(1749729493),toDateTime(1749741163),'system.cpu.time',{},{},{'cpu':'cpu5','state':'system'},'sum','cumulative',true),
    (toDateTime(1749729493),toDateTime(1749741163),'system.memory.usage',{},{},{'state':'slab_reclaimable'},'sum','cumulative',false),
    (toDateTime(1749729493),toDateTime(1749741163),'system.disk.merged',{},{},{'direction':'write','device':'nvme0n1p2'},'sum','cumulative',true),
    (toDateTime(1749729493),toDateTime(1749741163),'system.disk.operations',{},{},{'device':'nvme0n1p2','direction':'write'},'sum','cumulative',true),
    (toDateTime(1749729493),toDateTime(1749741163),'system.cpu.time',{},{},{'state':'nice','cpu':'cpu9'},'sum','cumulative',true),
    (toDateTime(1749729493),toDateTime(1749741163),'system.network.packets',{},{},{'device':'lo','direction':'transmit'},'sum','cumulative',true),
    (toDateTime(1749729493),toDateTime(1749741163),'system.disk.io_time',{},{},{'device':'nvme0n1p2'},'sum','cumulative',true),
    (toDateTime(1749729493),toDateTime(1749741163),'system.disk.io',{},{},{'device':'dm-2','direction':'write'},'sum','cumulative',true),
    (toDateTime(1749729493),toDateTime(1749741163),'system.cpu.time',{},{},{'cpu':'cpu14','state':'steal'},'sum','cumulative',true),
    (toDateTime(1749729493),toDateTime(1749741163),'system.disk.io_time',{},{},{'device':'nvme0n1p1'},'sum','cumulative',true),
    (toDateTime(1749729493),toDateTime(1749741163),'system.processes.count',{},{},{'status':'blocked'},'sum','cumulative',false),
    (toDateTime(1749729493),toDateTime(1749741163),'system.network.errors',{},{},{'device':'vethd73fd86','direction':'receive'},'sum','cumulative',true),
    (toDateTime(1749729493),toDateTime(1749741163),'system.memory.usage',{},{},{'state':'cached'},'sum','cumulative',false),
    (toDateTime(1749729493),toDateTime(1749741163),'system.disk.operations',{},{},{'direction':'write','device':'nvme0n1p1'},'sum','cumulative',true),
    (toDateTime(1749729493),toDateTime(1749741163),'system.network.errors',{},{},{'direction':'transmit','device':'veth8473ec4'},'sum','cumulative',true),
    (toDateTime(1749729493),toDateTime(1749741163),'system.disk.io',{},{},{'device':'nvme0n1p2','direction':'read'},'sum','cumulative',true),
    (toDateTime(1749729493),toDateTime(1749741163),'system.network.connections',{},{},{'protocol':'tcp','state':'ESTABLISHED'},'sum','cumulative',false),
    (toDateTime(1749729493),toDateTime(1749741163),'system.cpu.time',{},{},{'state':'softirq','cpu':'cpu15'},'sum','cumulative',true),
    (toDateTime(1749729493),toDateTime(1749741163),'system.paging.usage',{},{},{'state':'used','device':'/dev/dm-1'},'sum','cumulative',false),
    (toDateTime(1749729493),toDateTime(1749741163),'system.network.packets',{},{},{'direction':'receive','device':'vethd73fd86'},'sum','cumulative',true),
    (toDateTime(1749729493),toDateTime(1749741163),'system.cpu.time',{},{},{'state':'softirq','cpu':'cpu3'},'sum','cumulative',true);

select * from metric_activity format Native;
"#;

#[test]
fn metric_activity() -> TestResult {
    let data = std::fs::read(crate::common::fixture("metric_activity.native"))?;
    let (_, block) = parse_single(&data)?;

    for index in 0..block.num_rows {
        for (col, name) in block.markers.iter().zip(block.col_names.iter()) {
            if !name.contains("attrs") {
                continue;
            }
            let value = col.get(index)?.unwrap();
            let value: MapIterator<&str, &str> = value.try_into()?;

            let mut map = HashMap::new();
            for (key, val) in value.flatten() {
                map.insert(key, val);
            }
        }
    }

    Ok(())
}

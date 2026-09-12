use bloch::FromBlock;
use bloch::parse::block::parse_single;
use bloch::reader::{I64, Ipv4, Ipv6};
use std::net::{Ipv4Addr, Ipv6Addr};

const _SQL: &str = r#"
drop table if exists ip_sample;

create table ip_sample
(
    id  Int64,
    ip4 IPv4,
    ip6 IPv6
) engine = MergeTree order by tuple();

insert into ip_sample (id, ip4, ip6) values
    (0, '100.64.0.2', '2001:db8::ff00:42:8329'),
    (1, '127.0.0.1', '::1'),
    (2, '10.10.10.10', '2001:0db8:85a3:0000:0000:8a2e:0370:7334');

select * from ip_sample order by id format Native;
"#;

#[derive(FromBlock, Copy, Clone)]
struct Row<'a> {
    id: I64<'a>,
    ip4: Ipv4<'a>,
    ip6: Ipv6<'a>,
}

#[test]
fn reads_ip_addresses() -> Result<(), Box<dyn std::error::Error>> {
    let data = std::fs::read(crate::common::fixture("ip_sample.native"))?;
    let (_, block) = parse_single(&data)?;
    let expected4 = [
        Ipv4Addr::new(100, 64, 0, 2),
        Ipv4Addr::LOCALHOST,
        Ipv4Addr::new(10, 10, 10, 10),
    ];
    let expected6 = [
        "2001:db8::ff00:42:8329".parse::<Ipv6Addr>()?,
        Ipv6Addr::LOCALHOST,
        "2001:db8:85a3::8a2e:370:7334".parse()?,
    ];
    for (index, row) in Row::rows(&block)?.enumerate() {
        let row = row?;
        assert_eq!(row.id, i64::try_from(index)?);
        assert_eq!((row.ip4, row.ip6), (expected4[index], expected6[index]));
    }
    Ok(())
}

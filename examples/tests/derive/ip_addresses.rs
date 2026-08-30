use chbr::FromBlock;
use chbr::parse::block::parse_single;
use chbr::reader::{I64, Ipv4, Ipv6};
use std::net::{Ipv4Addr, Ipv6Addr};

#[derive(FromBlock)]
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

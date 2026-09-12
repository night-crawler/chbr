use std::str::FromStr as _;

use bloch::parse::block::parse_single;
use pretty_assertions::assert_eq;
use testresult::TestResult;

#[test]
fn ip_sample() -> TestResult {
    let buf = std::fs::read(crate::common::fixture("ip_sample.native"))?;
    let (_, block) = parse_single(&buf)?;

    // 0,100.64.0.2,2001:db8:0:0:0:ff00:42:8329
    // 1,127.0.0.1,0:0:0:0:0:0:0:1
    // 2,10.10.10.10,2001:db8:85a3:0:0:8a2e:370:7334

    let ipv4_marker = &block.markers[1];
    let expected_ipv4 = [
        std::net::Ipv4Addr::new(100, 64, 0, 2),
        std::net::Ipv4Addr::new(127, 0, 0, 1),
        std::net::Ipv4Addr::new(10, 10, 10, 10),
    ];

    for (i, expected) in expected_ipv4.iter().enumerate() {
        let value: std::net::Ipv4Addr = ipv4_marker.get(i)?.unwrap().try_into()?;
        assert_eq!(value, *expected, "Mismatch at index {i}");
    }

    let ipv6_marker = &block.markers[2];
    let expected_ipv6 = [
        std::net::Ipv6Addr::from_str("2001:db8:0:0:0:ff00:42:8329")?,
        std::net::Ipv6Addr::from_str("0:0:0:0:0:0:0:1")?,
        std::net::Ipv6Addr::from_str("2001:db8:85a3:0:0:8a2e:370:7334")?,
    ];
    for (i, expected) in expected_ipv6.iter().enumerate() {
        let value: std::net::Ipv6Addr = ipv6_marker.get(i)?.unwrap().try_into()?;
        assert_eq!(value, *expected, "Mismatch at index {i}");
    }

    Ok(())
}

use bloch::parse::block::parse_single;
use bloch::value::Value;
use testresult::TestResult;

#[test]
fn uuid_and_ip_byte_order_matches_clickhouse_text_form() -> TestResult {
    let data = std::fs::read(crate::common::fixture("uuid_ip_order.native"))?;
    let (_, block) = parse_single(&data)?;
    let uuid = block.mark("u")?.get_uuid(0)?.unwrap();
    assert_eq!(uuid.to_string(), "61f0c404-5cb3-11e7-907b-a6006ad3dba0");
    let Some(Value::Ipv6(ip6)) = block.mark("ip6")?.get(0)? else {
        panic!("ipv6");
    };
    assert_eq!(std::net::Ipv6Addr::from(*ip6).to_string(), "2001:db8::1");
    let Some(Value::Ipv4(ip4)) = block.mark("ip4")?.get(0)? else {
        panic!("ipv4");
    };
    assert_eq!(ip4.to_string(), "192.168.1.2");
    Ok(())
}

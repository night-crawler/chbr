// not sure what to do with integration tests right now but leaving it here
// as a playground

// use chbr::parse::block::single;
// use testresult::TestResult;
//
// pub fn get_client() -> TestResult<clickhouse::Client> {
//     let client = clickhouse::Client::default()
//         .with_url("http://100.64.0.2:8124")
//         .with_database("default")
//         .with_user("test_user")
//         .with_password("test_user");
//
//     Ok(client)
// }
//
// #[tokio::test]
// async fn test_client() -> TestResult {
//     let c = get_client()?;
//
//     let bytes = c
//         .query("select * from empty_sample order by id")
//         .fetch_bytes("NATIVE")?
//         .collect()
//         .await?;
//
//     println!("{}", bytes.len());
//     let (input, block) = single(bytes.as_ref())?;
//
//     println!("Unparsed remainder: {}", input.len());
//     println!("{:?}", block.col_names);
//
//     Ok(())
// }

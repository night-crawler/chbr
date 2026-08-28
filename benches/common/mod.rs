use chbr::FromBlock;
use chbr::reader::{
    Array, Bool, DateTime, F64, Ipv6, LcNullableTrustedStr, LcTrustedStr, Nullable, U32, U64,
    U128, Uuid,
};

#[derive(FromBlock)]
pub struct BenchmarkCols<'a> {
    pub id: Uuid<'a>,
    pub lc_string_cd10: LcTrustedStr<'a>,
    pub timestamp: DateTime<'a>,
    pub count: F64<'a>,
    pub some_number: U32<'a>,

    pub lc_nullable_string_cd1000: LcNullableTrustedStr<'a>,
    pub lc_nullable_string_cd5000: LcNullableTrustedStr<'a>,
    pub lc_nullable_string_cd3000: LcNullableTrustedStr<'a>,
    pub lc_nullable_string_cd4000: LcNullableTrustedStr<'a>,
    pub lc_nullable_string_cd50000: LcNullableTrustedStr<'a>,
    pub lc_nullable_string_cd100: LcNullableTrustedStr<'a>,
    pub lc_nullable_string_cd500: LcNullableTrustedStr<'a>,

    pub some_ip_address: Nullable<'a, Ipv6<'a>>,

    pub lc_nullable_string8: LcNullableTrustedStr<'a>,
    pub lc_tags: Array<'a, LcTrustedStr<'a>>,
    pub lc_nullable_string_cd_00000: LcNullableTrustedStr<'a>,

    #[col(name = "nested_field.lc_string_cd10")]
    pub nested_lc_string_cd10: Array<'a, LcTrustedStr<'a>>,

    #[col(name = "nested_field.flag")]
    pub nested_flag: Array<'a, Bool<'a>>,

    #[col(name = "nested_field.some_id")]
    pub nested_some_id: Array<'a, U128<'a>>,

    #[col(name = "nested_field.some_other_id")]
    pub nested_some_other_id: Array<'a, U64<'a>>,
}

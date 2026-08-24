use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::{ToTokens, format_ident, quote};
use syn::punctuated::Punctuated;
use syn::spanned::Spanned;
use syn::token::Comma;
use syn::{Data, DeriveInput, Expr, Field, Fields, Lifetime, Meta, Variant, parse_macro_input};

struct ColSpec {
    ident: syn::Ident,
    ty: syn::Type,
    vis: syn::Visibility,
    index: usize,
    /// Expression evaluating to `&str`: the `#[col(name = ...)]` value, or a
    /// string literal of the field identifier.
    name: TokenStream2,
}

#[proc_macro_derive(FromBlock, attributes(col))]
pub fn derive_from_block(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    derive_from_block_inner(&input)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

#[proc_macro_derive(FromVariant, attributes(col))]
pub fn derive_from_variant(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    derive_from_variant_inner(&input)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

fn derive_from_block_inner(input: &DeriveInput) -> Result<TokenStream2, syn::Error> {
    let fields: &Punctuated<Field, Comma> = extract_fields(input)?;
    let lt = extract_lifetime(input)?;

    let mut specs = Vec::with_capacity(fields.len());
    for (index, field) in fields.iter().enumerate() {
        specs.push(parse_field(field, index)?);
    }

    let ident = &input.ident;
    let vis = &input.vis;
    let item_ident = format_ident!("{}Item", ident);
    let num_fields = specs.len();

    let item_fields = specs
        .iter()
        .map(|ColSpec { ident, ty, vis, .. }| {
            quote! { #vis #ident: <#ty as ::chbr::reader::TryRead<#lt>>::Item }
        })
        .collect::<Vec<_>>();

    let block_inits = specs
        .iter()
        .map(|ColSpec { ident, name, .. }| {
            quote! {
                #ident: {
                    let name: &str = #name;
                    ::core::convert::TryFrom::try_from(block.mark(name)?)?
                }
            }
        })
        .collect::<Vec<_>>();

    let named_inits = specs
        .iter()
        .map(|ColSpec { ident, name, .. }| {
            quote! {
                #ident: {
                    let name: &str = #name;
                    ::core::convert::TryFrom::try_from(nt.mark(name)?)?
                }
            }
        })
        .collect::<Vec<_>>();

    let positional_inits = specs
        .iter()
        .map(|ColSpec { ident, index, .. }| {
            quote! { #ident: ::core::convert::TryFrom::try_from(&tuple.values[#index])? }
        })
        .collect::<Vec<_>>();

    let read_fields = specs
        .iter()
        .map(|ColSpec { ident, .. }| {
            quote! { #ident: ::chbr::reader::TryRead::try_read(&self.#ident, idx)? }
        })
        .collect::<Vec<_>>();

    Ok(quote! {
        #vis struct #item_ident<#lt> {
            #(#item_fields,)*
        }

        #[automatically_derived]
        impl<#lt> ::core::marker::Copy for #ident<#lt> {}

        #[automatically_derived]
        impl<#lt> ::core::clone::Clone for #ident<#lt> {
            #[inline]
            fn clone(&self) -> Self {
                *self
            }
        }

        #[automatically_derived]
        impl<#lt> ::chbr::reader::FromBlock<#lt> for #ident<#lt> {
            fn from_block(block: &#lt ::chbr::ParsedBlock<#lt>) -> ::chbr::Result<Self> {
                ::core::result::Result::Ok(Self { #(#block_inits,)* })
            }
        }

        #[automatically_derived]
        impl<#lt> ::chbr::reader::TryRead<#lt> for #ident<#lt> {
            type Item = #item_ident<#lt>;

            #[inline(always)]
            fn try_read(&self, idx: usize) -> ::chbr::Result<Self::Item> {
                ::core::result::Result::Ok(#item_ident { #(#read_fields,)* })
            }
        }

        #[automatically_derived]
        impl<#lt> ::core::convert::TryFrom<&#lt ::chbr::mark::Mark<#lt>> for #ident<#lt> {
            type Error = ::chbr::error::Error;

            fn try_from(
                mark: &#lt ::chbr::mark::Mark<#lt>,
            ) -> ::core::result::Result<Self, Self::Error> {
                match mark {
                    ::chbr::mark::Mark::NamedTuple(nt) => {
                        ::core::result::Result::Ok(Self { #(#named_inits,)* })
                    }
                    ::chbr::mark::Mark::Tuple(tuple) => {
                        if tuple.values.len() != #num_fields {
                            return ::core::result::Result::Err(
                                ::chbr::error::Error::MismatchedType(
                                    "Tuple",
                                    "Tuple with matching arity",
                                ),
                            );
                        }
                        ::core::result::Result::Ok(Self { #(#positional_inits,)* })
                    }
                    other => ::core::result::Result::Err(::chbr::error::Error::MismatchedType(
                        other.as_str(),
                        "NamedTuple/Tuple",
                    )),
                }
            }
        }
    })
}

fn derive_from_variant_inner(input: &DeriveInput) -> Result<TokenStream2, syn::Error> {
    let Data::Enum(en) = &input.data else {
        return Err(syn::Error::new(
            input.span(),
            "FromVariant only supports enums",
        ));
    };
    if en.variants.is_empty() {
        return Err(syn::Error::new(
            input.span(),
            "FromVariant requires at least one variant",
        ));
    }

    // Zero lifetimes is fine (all-owned payloads); a fresh one is used then.
    let lt = match extract_optional_lifetime(input)? {
        Some(lt) => lt.clone(),
        None => Lifetime::new("'chbr", proc_macro2::Span::call_site()),
    };

    let ident = &input.ident;
    let (_, ty_generics, _) = input.generics.split_for_impl();
    let num_variants = en.variants.len();

    let mut reader_tys = Vec::with_capacity(num_variants);
    let mut read_arms = Vec::with_capacity(num_variants);
    let mut init_exprs = Vec::with_capacity(num_variants);
    for (index, variant) in en.variants.iter().enumerate() {
        let payload = extract_payload(variant)?;
        let reader_ty = match parse_col_reader(variant)? {
            Some(ty) => ty.to_token_stream(),
            None => quote! { <#payload as ::chbr::reader::Readable<#lt>>::Reader },
        };
        reader_tys.push(reader_ty);

        let var_ident = &variant.ident;
        let tuple_idx = syn::Index::from(index);
        read_arms.push(quote! {
            #index => ::core::result::Result::Ok(Self::#var_ident(
                ::chbr::reader::TryRead::try_read(&readers.#tuple_idx, idx)?,
            ))
        });
        init_exprs.push(quote! { ::core::convert::TryFrom::try_from(&marks[#index])? });
    }

    let arity_msg = format!("{ident} ({num_variants} variants)");
    let disc_msg = format!("{ident} discriminators");

    Ok(quote! {
        #[automatically_derived]
        impl<#lt> ::chbr::reader::FromVariant<#lt> for #ident #ty_generics {
            type Readers = (#(#reader_tys,)*);

            fn from_marks(
                marks: &#lt [::chbr::mark::Mark<#lt>],
            ) -> ::chbr::Result<Self::Readers> {
                if marks.len() != #num_variants {
                    return ::core::result::Result::Err(
                        ::chbr::error::Error::MismatchedType("Variant", #arity_msg),
                    );
                }
                ::core::result::Result::Ok((#(#init_exprs,)*))
            }

            #[inline(always)]
            fn read(
                readers: &Self::Readers,
                discriminator: usize,
                idx: usize,
            ) -> ::chbr::Result<Self> {
                match discriminator {
                    #(#read_arms,)*
                    _ => ::core::result::Result::Err(
                        ::chbr::error::Error::IndexOutOfBounds(discriminator, #disc_msg),
                    ),
                }
            }
        }
    })
}

fn extract_payload(variant: &Variant) -> Result<&syn::Type, syn::Error> {
    let Fields::Unnamed(fields) = &variant.fields else {
        return Err(syn::Error::new(
            variant.span(),
            "FromVariant variants must have exactly one unnamed payload, e.g. `Num(i64)`",
        ));
    };

    let mut iter = fields.unnamed.iter();
    let (Some(payload), None) = (iter.next(), iter.next()) else {
        return Err(syn::Error::new(
            variant.span(),
            "FromVariant variants must have exactly one unnamed payload, e.g. `Num(i64)`",
        ));
    };

    Ok(&payload.ty)
}

/// `#[col(reader = SomeReader<'a>)]`: overrides the payload's
/// `Readable`-inferred column reader.
fn parse_col_reader(variant: &Variant) -> Result<Option<syn::Type>, syn::Error> {
    let mut reader = None;
    for attr in variant.attrs.iter().filter(|a| a.path().is_ident("col")) {
        match &attr.meta {
            Meta::List(list) => {
                list.parse_nested_meta(|meta| {
                    if !meta.path.is_ident("reader") {
                        return Err(syn::Error::new(
                            meta.path.span(),
                            format!(
                                "only the `reader` attribute is supported; you provided: {}",
                                meta.path.to_token_stream()
                            ),
                        ));
                    }

                    let value: syn::Type = meta.value()?.parse()?;
                    if reader.replace(value).is_some() {
                        return Err(syn::Error::new(
                            meta.path.span(),
                            "duplicate `reader` attribute",
                        ));
                    }

                    Ok(())
                })?;
            }
            other => {
                return Err(syn::Error::new(
                    other.span(),
                    format!(
                        "unsupported attribute form, use #[col(reader = ...)]; you passed: {}",
                        other.to_token_stream()
                    ),
                ));
            }
        }
    }

    Ok(reader)
}

fn extract_lifetime(input: &DeriveInput) -> Result<&Lifetime, syn::Error> {
    extract_optional_lifetime(input)?.ok_or_else(|| {
        syn::Error::new(
            input.generics.span(),
            "FromBlock requires exactly one lifetime parameter",
        )
    })
}

fn extract_optional_lifetime(input: &DeriveInput) -> Result<Option<&Lifetime>, syn::Error> {
    if input.generics.type_params().next().is_some()
        || input.generics.const_params().next().is_some()
    {
        return Err(syn::Error::new(
            input.generics.span(),
            "type or const parameters are not supported",
        ));
    }

    let mut lifetimes = input.generics.lifetimes();
    match (lifetimes.next(), lifetimes.next()) {
        (None, _) => Ok(None),
        (Some(first), None) => Ok(Some(&first.lifetime)),
        (Some(_), Some(second)) => Err(syn::Error::new(
            second.span(),
            "at most one lifetime parameter is supported",
        )),
    }
}

fn parse_field(field: &Field, index: usize) -> Result<ColSpec, syn::Error> {
    let Some(ident) = field.ident.clone() else {
        return Err(syn::Error::new(
            field.span(),
            "FromBlock only supports structs with named fields",
        ));
    };

    let name = match parse_col_name(field)? {
        Some(expr) => expr.to_token_stream(),
        None => {
            let repr = ident.to_string();
            let repr = repr.strip_prefix("r#").unwrap_or(&repr);
            syn::LitStr::new(repr, ident.span()).to_token_stream()
        }
    };

    Ok(ColSpec {
        ident,
        ty: field.ty.clone(),
        vis: field.vis.clone(),
        index,
        name,
    })
}

fn parse_col_name(field: &Field) -> Result<Option<Expr>, syn::Error> {
    let mut name = None;
    for attr in field.attrs.iter().filter(|a| a.path().is_ident("col")) {
        match &attr.meta {
            Meta::List(list) => {
                list.parse_nested_meta(|meta| {
                    if !meta.path.is_ident("name") {
                        return Err(syn::Error::new(
                            meta.path.span(),
                            format!(
                                "only the `name` attribute is supported; you provided: {}",
                                meta.path.to_token_stream()
                            ),
                        ));
                    }

                    let value: Expr = meta.value()?.parse()?;
                    if name.replace(value).is_some() {
                        return Err(syn::Error::new(
                            meta.path.span(),
                            "duplicate `name` attribute",
                        ));
                    }

                    Ok(())
                })?;
            }
            other => {
                return Err(syn::Error::new(
                    other.span(),
                    format!(
                        "unsupported attribute form, use #[col(name = ...)]; you passed: {}",
                        other.to_token_stream()
                    ),
                ));
            }
        }
    }

    Ok(name)
}

fn extract_fields(input: &DeriveInput) -> Result<&Punctuated<Field, Comma>, syn::Error> {
    match &input.data {
        Data::Struct(s) => match &s.fields {
            Fields::Named(named) => Ok(&named.named),
            other => {
                let repr = other.to_token_stream().to_string();
                Err(syn::Error::new(
                    s.struct_token.span,
                    format!(
                        "FromBlock only supports structs with named fields; you provided: {repr}"
                    ),
                ))
            }
        },
        Data::Enum(en) => {
            let repr = en.enum_token.to_token_stream().to_string();
            Err(syn::Error::new(
                input.span(),
                format!("FromBlock only supports structs; you provided: {repr}"),
            ))
        }
        Data::Union(un) => {
            let repr = un.union_token.to_token_stream().to_string();
            Err(syn::Error::new(
                input.span(),
                format!("FromBlock only supports structs; you provided: {repr}"),
            ))
        }
    }
}

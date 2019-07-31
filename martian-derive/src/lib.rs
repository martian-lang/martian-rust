#![recursion_limit = "128"]

/// TODO:
/// - Filetypes at the top - add a test
/// - Retain attribute
/// - Handle default values for FileType
/// - Derive AsMartianPrimary
/// - Handle attributes in MartianStruct
/// - Martian filetype as a procedural macro
/// - Error message MartianStruct showing MartianFiletype
extern crate proc_macro;
use martian::{utils, StageKind, Volatile, MARTIAN_TOKENS};
use quote::quote;
use std::collections::HashSet;
use std::str::FromStr;
use syn::{Error, Fields, Ident, ImplItem, ItemImpl, ItemStruct, Type};

const ATTR_NOT_ON_TRAIT_IMPL_ERROR: &'static str = r#"The attribute #[make_mro] should only be applied to `martian::MartianMain` or `martian::MartianStage` trait implementation of a stage struct"#;
const MARTIAN_MAIN_TRAIT: &'static str = "MartianMain";
const MARTIAN_STAGE_TRAIT: &'static str = "MartianStage";
const STAGE_INPUT_IDENT: &'static str = "StageInputs";
const STAGE_OUTPUT_IDENT: &'static str = "StageOutputs";
const CHUNK_INPUT_IDENT: &'static str = "ChunkInputs";
const CHUNK_OUTPUT_IDENT: &'static str = "ChunkOutputs";

const MARTIAN_STRUCT_NOT_ON_NAMED_STRUCT_ERROR: &'static str =
    r#"#[derive(MartianStruct)] can only be used on structs with named fields."#;

/// When this attribute is applied to the `MartianMain` or `MartianStage` trait implementation of
/// a stage struct, it derives the trait `MakeMro` to the stage struct, which lets you generate
/// the mro corresponding to the stage.
#[proc_macro_attribute]
pub fn make_mro(
    attr: proc_macro::TokenStream,
    item: proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    let item_clone = item.clone();

    // ::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    // STEP 0
    // ::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    // Parse the attributes and emit error if it is invalid
    // Attributes are stuff inside the bracket after make_mro in
    // `#[make_mro(mem_gb = 4, threads = 2)]`
    // If we get valid attributes, create `fn using_attributes() -> MroUsing;`
    let parsed_attr: MakeMroAttr = match attr.to_string().parse() {
        Ok(parsed) => parsed,
        Err(e) => {
            let span = attr.into_iter().next().unwrap().span().into();
            return syn::Error::new(span, e).to_compile_error().into();
        }
    };

    // If a stage name is specified, make sure it's in SHOUTY_SNAKE_CASE
    if let Some(ref name) = parsed_attr.stage_name {
        let expected = utils::to_shouty_snake_case(name);
        if expected != *name {
            let span = attr
                .into_iter()
                .filter(|tt| {
                    if let proc_macro::TokenTree::Ident(ref ident) = tt {
                        ident.to_string() == "stage_name"
                    } else {
                        false
                    }
                })
                .next()
                .unwrap()
                .span()
                .into();
            return syn::Error::new(
                span,
                format!(
                    "`stage_name` needs to be in SHOUTY_SNAKE_CASE (without any surrounding quotes). Found {}, use {} instead",
                    name, expected
                ),
            )
            .to_compile_error()
            .into();
        }
    }

    let mem_gb_quote = parsed_attr
        .mem_gb
        .map(|x| quote![mem_gb: Some(#x),])
        .unwrap_or(quote![]);
    let vmem_gb_quote = parsed_attr
        .vmem_gb
        .map(|x| quote![vmem_gb: Some(#x),])
        .unwrap_or(quote![]);
    let threads_quote = parsed_attr
        .threads
        .map(|x| quote![threads: Some(#x),])
        .unwrap_or(quote![]);
    let volatile_quote = match parsed_attr.volatile {
        Some(k) => match k {
            Volatile::Strict => quote![volatile: Some(::martian::Volatile::Strict),],
        },
        None => quote![volatile: None,],
    };
    let using_attributes_fn = quote![
        fn using_attributes() -> ::martian::MroUsing {
            ::martian::MroUsing {
                #mem_gb_quote
                #vmem_gb_quote
                #threads_quote
                #volatile_quote
                ..Default::default()
            }
        }
    ];

    // ::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    // STEP 1
    // ::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    // Make sure that #[make_mro] attribute is used on a trait impl and not
    // anything else.
    // The way we achieve it is to try parsing the input TokenStrean as `ItemImpl`
    // and checking the parse result
    let item_impl = match syn::parse::<ItemImpl>(item.clone()) {
        Ok(item_impl) => item_impl,
        Err(_) => {
            let span = item_clone.into_iter().next().unwrap().span().into();
            return syn::Error::new(span, ATTR_NOT_ON_TRAIT_IMPL_ERROR)
                .to_compile_error()
                .into();
        }
    };

    // ::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    // STEP 2
    // ::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    // Identify whether #[make_mro] was applied to a `MartianMain` or `MartianStage`
    // If we find that it was applied to a different trait impl, produce a
    // sensible compile error. This only checks for the trait by name, so it is possible
    // to trick the compiler to continue, but it should fail later if the trait
    // signature is different from the one defined in `martian` crate
    let trait_path = item_impl.trait_.unwrap().1;
    let which_trait = match parse_which_trait(trait_path.clone()) {
        Ok(t) => t,
        Err(e) => return e.to_compile_error().into(),
    };

    // ::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    // STEP 3
    // ::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    // Now we know which trait the attribute has been applied to. Find out the
    // name of the stage struct. We need to implement `MakeMro` trait for this
    // struct. We can build the `stage_name()` function from the struct name.
    // TODO: This might also be a good time to check that the implementation is
    // actually for a unit struct. But is it possible?
    let stage_struct = item_impl.self_ty.clone();
    let stage_struct_name = match *stage_struct {
        Type::Path(ref ty_path) => {
            let segments = &ty_path.path.segments;
            segments.iter().last().unwrap().ident.to_string()
        }
        _ => {
            let span = item_clone.into_iter().next().unwrap().span().into();
            return syn::Error::new(span, "Expecting the impl for a struct.")
                .to_compile_error()
                .into();
        }
    };
    // Stage name is the name of the stage in the mro. It can either be set using
    // the name attribute or can be derived by converting the struct to shouting
    // snake case
    let stage_name =
        utils::to_shouty_snake_case(&parsed_attr.stage_name.unwrap_or(stage_struct_name));
    let stage_name_fn = quote![
        fn stage_name() -> String {
            String::from(#stage_name)
        }
    ];

    // ::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    // STEP 4
    // ::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    // Find out the associated types `StageInputs`, `StageOutputs`,
    // `ChunkInputs`, `ChunkOutputs` and meke the two finctions
    // - `fn stage_in_and_out() -> InAndOut;`
    // - `fn chunk_in_and_out() -> Option<InAndOut>;`
    let mut builder = AssociatedTypeBuilder::default();
    for impl_item in &item_impl.items {
        if let ImplItem::Type(ty) = impl_item {
            builder.add(ty.ident.clone(), ty.ty.clone());
        }
    }
    let stage_var_fn = builder.to_quote(which_trait);

    // ::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    // STEP 5
    // ::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    // Stitch the quotes together
    let item_clone2 = proc_macro2::TokenStream::from(item_clone);
    let final_token = quote![
        #item_clone2
        impl ::martian::MakeMro for #stage_struct {
            #stage_var_fn
            #stage_name_fn
            #using_attributes_fn
        }
    ]
    .into();
    final_token
}

#[derive(Default)]
struct AssociatedTypeBuilder {
    stage_inputs: Option<Type>,
    stage_outputs: Option<Type>,
    chunk_inputs: Option<Type>,
    chunk_outputs: Option<Type>,
}

impl AssociatedTypeBuilder {
    fn add(&mut self, associated_type_key: Ident, associated_type_val: Type) {
        match associated_type_key.to_string().as_ref() {
            STAGE_INPUT_IDENT => {
                if self.stage_inputs.is_some() {
                    unreachable!(format!("Found multiple assignments for associated type {}. I am not sure how to proceed.", STAGE_INPUT_IDENT))
                }
                self.stage_inputs = Some(associated_type_val);
            }
            STAGE_OUTPUT_IDENT => {
                if self.stage_outputs.is_some() {
                    unreachable!(format!("Found multiple assignments for associated type {}. I am not sure how to proceed.", STAGE_OUTPUT_IDENT))
                }
                self.stage_outputs = Some(associated_type_val);
            }
            CHUNK_INPUT_IDENT => {
                if self.chunk_inputs.is_some() {
                    unreachable!(format!("Found multiple assignments for associated type {}. I am not sure how to proceed.", CHUNK_INPUT_IDENT))
                }
                self.chunk_inputs = Some(associated_type_val);
            }
            CHUNK_OUTPUT_IDENT => {
                if self.chunk_outputs.is_some() {
                    unreachable!(format!("Found multiple assignments for associated type {}. I am not sure how to proceed.", CHUNK_INPUT_IDENT))
                }
                self.chunk_outputs = Some(associated_type_val);
            }
            s => unreachable!(format!(
                "Got an unexpected associated type {}. I am not sure how to proceed",
                s
            )),
        }
    }
    fn to_quote(&self, which: StageKind) -> proc_macro2::TokenStream {
        assert!(self.stage_inputs.is_some());
        assert!(self.stage_outputs.is_some());
        let si = self.stage_inputs.clone().unwrap();
        let so = self.stage_outputs.clone().unwrap();
        let stage_in_out_fn = quote![
            fn stage_in_and_out() -> ::martian::InAndOut {
                ::martian::InAndOut {
                    inputs: <#si as ::martian::MartianStruct>::mro_fields(),
                    outputs: <#so as ::martian::MartianStruct>::mro_fields(),
                }
            }
        ];
        let chunk_in_out_fn = match which {
            StageKind::MainOnly => {
                assert!(self.chunk_inputs.is_none());
                assert!(self.chunk_outputs.is_none());
                quote![
                    fn chunk_in_and_out() -> Option<::martian::InAndOut> {
                        None
                    }
                ]
            }
            StageKind::WithSplit => {
                assert!(self.chunk_inputs.is_some());
                assert!(self.chunk_outputs.is_some());
                let ci = self.chunk_inputs.clone().unwrap();
                let co = self.chunk_outputs.clone().unwrap();
                quote![
                    fn chunk_in_and_out() -> Option<::martian::InAndOut> {
                        Some(::martian::InAndOut {
                            inputs: <#ci as ::martian::MartianStruct>::mro_fields(),
                            outputs: <#co as ::martian::MartianStruct>::mro_fields(),
                        })
                    }
                ]
            }
        };
        quote![
            #stage_in_out_fn
            #chunk_in_out_fn
        ]
    }
}

// Identify which trait impl the attribute is applied to among `MartianMain`
// and `MartianStage`. If we find that this is applied to a different trait,
// return an error.
fn parse_which_trait(trait_path: syn::Path) -> Result<StageKind, Error> {
    let mut last_ident = String::from("");
    let span = trait_path.segments[0].ident.span();
    for segment in trait_path.segments {
        if segment.ident == MARTIAN_MAIN_TRAIT {
            return Ok(StageKind::MainOnly);
        }
        if segment.ident == MARTIAN_STAGE_TRAIT {
            return Ok(StageKind::WithSplit);
        }
        last_ident = segment.ident.to_string();
    }

    Err(Error::new(
        span,
        format!(
            "{}. You are trying to use it on {} trait implementation.",
            ATTR_NOT_ON_TRAIT_IMPL_ERROR, last_ident
        ),
    ))
}

// A macro to create MakeMroAttr struct which will parse a comma separated
// `key=value` pairs. The input to the macro is the list of keys and their
// type. All they keys are optional and the type can be any type which can be
// parsed
macro_rules! attr_parse {
    ($($property:ident: $type:ty),*) => {
        #[derive(Debug, Default, PartialEq)]
        struct MakeMroAttr {
            $($property: Option<$type>,)*
        }
        impl FromStr for MakeMroAttr {
            type Err = String;
            fn from_str(s: &str) -> Result<Self, Self::Err> {
                if s.is_empty() {
                    return Ok(MakeMroAttr::default());
                }
                $(let mut $property = None;)*
                for using_spec in s.split(',') {
                    let parts: Vec<_> = using_spec.trim().split("=").map(|part| part.trim()).collect();
                    if parts.len() != 2 {
                        return Err(format!("Expecting a comma separated `key=value` like tokens here. The allowed keys are: [{}]", stringify!($($property),*)));
                    }
                    match parts[0] {
                        $(stringify!($property) => {
                            // Make sure that this is the first assignment
                            if $property.is_some() {
                                return Err(format!("Found multiple assignments for {}.", parts[0]))
                            }
                            $property = match parts[1].parse::<$type>() {
                                Ok(parsed) => Some(parsed),
                                Err(_) => return Err(format!("Unable to parse {0} as {1} from `{0}={2}`", parts[0], stringify!($type), parts[1]))
                            };
                        },)*
                        _ => return Err(format!("Expecting a comma separated `key=value` like tokens here. The allowed keys are: [{}]. Found an invalid key {}", stringify!($($property),*), parts[0]))
                    }
                }
                Ok(MakeMroAttr {
                    $($property,)*
                })
            }
        }
    }
}

attr_parse!(
    mem_gb: i16,
    threads: i16,
    vmem_gb: i16,
    volatile: Volatile,
    stage_name: String
);

#[proc_macro_derive(MartianStruct)]
pub fn martian_struct(item: proc_macro::TokenStream) -> proc_macro::TokenStream {
    // ::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    // STEP 1
    // ::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    // Make sure that #[derive(MartianStruct)] is used on struct
    // The way we achieve it is to try parsing the input TokenStrean as `ItemStruct`
    // and checking the parse result
    let item_struct = match syn::parse::<ItemStruct>(item.clone()) {
        Ok(item_struct) => item_struct,
        Err(_) => {
            let span = item.into_iter().next().unwrap().span().into();
            return syn::Error::new(span, MARTIAN_STRUCT_NOT_ON_NAMED_STRUCT_ERROR)
                .to_compile_error()
                .into();
        }
    };

    // ::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    // STEP 2
    // ::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    // Collect the fields of the struct. If we get a unit struct or tuple struct
    // produce a compile error.
    let fields = match item_struct.fields.clone() {
        Fields::Named(f) => f.named,
        _ => {
            let span = item.into_iter().next().unwrap().span().into();
            return syn::Error::new(span, MARTIAN_STRUCT_NOT_ON_NAMED_STRUCT_ERROR)
                .to_compile_error()
                .into();
        }
    };

    // ::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    // STEP 3
    // ::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    // Generate tokenstream for `MroField` calls for each field
    // Make sure that none of the field names are martian keywords
    let mut vec_inner = Vec::new();
    let blacklist: HashSet<String> = MARTIAN_TOKENS.iter().map(|x| x.to_string()).collect();
    for field in fields {
        let name = field.ident.clone().unwrap().to_string();
        if blacklist.contains(&name) {
            return syn::Error::new(
                field.ident.unwrap().span(),
                format!(
                    "Field name {} is not allowed here since it is a martian keyword",
                    name
                ),
            )
            .to_compile_error()
            .into();
        }
        let ty = field.ty;
        vec_inner.push(quote![
            <::martian::MroField>::new(#name, <#ty as ::martian::AsMartianBlanketType>::as_martian_type())
        ]);
    }

    // ::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    // STEP 4
    // ::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    // Generate the `impl MartianStruct` token stream
    // Handle generics in the struct
    let (impl_generics, ty_generics, where_clause) = item_struct.generics.split_for_impl();
    let item_ident = item_struct.ident.clone();
    let final_token = quote![
        impl #impl_generics ::martian::MartianStruct for #item_ident #ty_generics #where_clause {
            fn mro_fields() -> Vec<::martian::MroField> {
                vec![
                    #(#vec_inner),*
                ]
            }
        }
    ];

    proc_macro::TokenStream::from(final_token)
}

#[cfg(test)]
mod tests {
    use super::*;
    // See https://docs.rs/trybuild/1.0.9/trybuild/ on how this test setup works
    // run `cargo test` with the environment variable `TRYBUILD=overwrite` to regenerate the
    // expected output in case you change the error message.
    // You should only use one test function.
    #[test]
    fn ui() {
        let t = trybuild::TestCases::new();
        t.compile_fail("tests/ui_make_mro/*.rs");
        t.compile_fail("tests/ui_martian_struct/*.rs");
    }

    #[test]
    fn test_attr_parse() {
        assert!("".parse::<MakeMroAttr>() == Ok(MakeMroAttr::default()));
        assert!("foo".parse::<MakeMroAttr>().is_err());
        assert!("mem_gb".parse::<MakeMroAttr>().is_err());
        assert!("mem_gb=f".parse::<MakeMroAttr>().is_err());
        assert!(
            "mem_gb=10".parse::<MakeMroAttr>().unwrap()
                == MakeMroAttr {
                    mem_gb: Some(10),
                    ..Default::default()
                }
        );
        assert!("mem_gb=10, mem_gb=5".parse::<MakeMroAttr>().is_err());
        assert!("mem_gb=10, thread=5".parse::<MakeMroAttr>().is_err());
        assert!(
            "mem_gb=10, threads=-1, volatile=strict"
                .parse::<MakeMroAttr>()
                .unwrap()
                == MakeMroAttr {
                    mem_gb: Some(10),
                    threads: Some(-1),
                    volatile: Some(Volatile::Strict),
                    ..Default::default()
                }
        );
        assert!(
            "stage_name=MY_STAGE".parse::<MakeMroAttr>().unwrap()
                == MakeMroAttr {
                    stage_name: Some("MY_STAGE".into()),
                    ..Default::default()
                }
        );
    }
}
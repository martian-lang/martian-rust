#![recursion_limit = "128"]

/// TODO:
/// - Handle default values for FileType
/// - Repo wide reorganization
extern crate proc_macro;
use martian::{utils, MartianPrimaryType, StageKind, Volatile, MARTIAN_TOKENS};
use quote::quote;
use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use syn::{Data, DeriveInput, Error, Fields, Ident, ImplItem, ItemImpl, ItemStruct, Type};

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
/// a stage struct, it derives the trait `MroMaker` to the stage struct, which lets you generate
/// the mro corresponding to the stage.
///
/// You can optionally specify `mem_gb`, `threads`, `vmem_gb` and `volatile` within this proc-macro.
/// For example, use `#[make_mro(mem_gb = 4, threads = 2]` for setting `mem_gb` and `threads` that would
/// appear in the `using()` section of the mro definition.
///
/// You can also set the stage name here. By default, the stage name in the mro is the SHOUTY_SNAKE_CASE version
/// of the stage struct name. You can override that using: `#[make_mro(mem_gb = 2, stage_name = MY_CUSTOM_NAME)]`
///
/// For examples on how to use it and customize, take a look at `tests/test_full_mro.rs`
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
            let attr2 = proc_macro2::TokenStream::from(attr);
            return syn::Error::new_spanned(attr2, e).to_compile_error().into();
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
            let span = proc_macro2::TokenStream::from(item_clone);
            return syn::Error::new_spanned(span, ATTR_NOT_ON_TRAIT_IMPL_ERROR)
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
            let span = proc_macro2::TokenStream::from(item_clone);
            return syn::Error::new_spanned(span, "Expecting the impl for a struct.")
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
    let (impl_generics, _, where_clause) = item_impl.generics.split_for_impl();
    let item_clone2 = proc_macro2::TokenStream::from(item_clone);
    let final_token = quote![
        #item_clone2
        #[automatically_derived]
        impl #impl_generics ::martian::MroMaker for #stage_struct #where_clause {
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

/// Structs which are used as associated types in `MartianMain` or `MartianStage`
/// traits need to implement `MartianStruct`. You can derive it using `#[derive(MartianStruct)]`
#[proc_macro_derive(MartianStruct, attributes(mro_retain))]
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
            let span = proc_macro2::TokenStream::from(item);
            return syn::Error::new_spanned(span, MARTIAN_STRUCT_NOT_ON_NAMED_STRUCT_ERROR)
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
            let span = proc_macro2::TokenStream::from(item);
            return syn::Error::new_spanned(span, MARTIAN_STRUCT_NOT_ON_NAMED_STRUCT_ERROR)
                .to_compile_error()
                .into();
        }
    };

    // ::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    // STEP 3
    // ::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    // Generate tokenstream for `MroField` calls for each field
    // Make sure that none of the field names are martian keywords.
    // Parse the #[mro_retian] attributes attached to the field, and make sure
    // that no serde field attributes are used
    let mut vec_inner = Vec::new();
    let blacklist: HashSet<String> = MARTIAN_TOKENS.iter().map(|x| x.to_string()).collect();
    for field in fields {
        let name = field.ident.clone().unwrap().to_string();
        let mut retain = false;
        for attr in &field.attrs {
            if let Ok(meta) = attr.parse_meta() {
                match meta {
                    syn::Meta::Word(ref attr_ident) if attr_ident == "mro_retain" => {
                        retain = true;
                    }
                    syn::Meta::List(ref list) if list.ident == "serde" => {
                        return syn::Error::new_spanned(field, "Cannot use serde attributes here. This might be okay, but it's hard to guarantee that deriving MartianStruct would work correctly when using serde attributes.")
                            .to_compile_error()
                            .into();
                    }
                    _ => {}
                }
            }
        }
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
        vec_inner.push(if retain {
            quote![
                <::martian::MroField>::retained(#name, <#ty as ::martian::AsMartianBlanketType>::as_martian_blanket_type())
            ]
        } else {
            quote![
                <::martian::MroField>::new(#name, <#ty as ::martian::AsMartianBlanketType>::as_martian_blanket_type())
            ]
        });
    }

    // ::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    // STEP 4
    // ::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    // Generate the `impl MartianStruct` token stream
    // Handle generics in the struct
    let (impl_generics, ty_generics, where_clause) = item_struct.generics.split_for_impl();
    let item_ident = item_struct.ident.clone();
    let final_token = quote![
        #[automatically_derived]
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

/// Custom types which are fields of a `MartianStruct` need to implement `AsMartianBlanketType`.
/// You can derive that trait on an enum or struct using `#[derive(MartianType)]`
#[proc_macro_derive(MartianType)]
pub fn martian_type(item: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input = syn::parse_macro_input!(item as DeriveInput);
    let ident = input.ident.clone();
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();

    match input.data {
        Data::Union(_) => {
            syn::Error::new_spanned(
                input,
                "Usage of union is highly discouraged. MartianType cannot be derived for a Union",
            )
            .to_compile_error()
            .into()
        },
        Data::Struct(ref struct_data) => match struct_data.fields {
            Fields::Unit => {
                syn::Error::new_spanned(
                    input,
                    "MartianType cannot be derived for a unit struct. Unit structs don't store any data, so they are most likely not useful as a MartianType.",
                )
                .to_compile_error()
                .into()
            }
            Fields::Named(_) => {
                quote![
                    #[automatically_derived]
                    impl #impl_generics ::martian::AsMartianPrimaryType for #ident #ty_generics #where_clause {
                        fn as_martian_primary_type() -> ::martian::MartianPrimaryType {
                            ::martian::MartianPrimaryType::Map
                        }
                    }
                ].into()
            },
            Fields::Unnamed(_) => {
                syn::Error::new_spanned(
                    input,
                    "Using an tuple struct as an mro field is not recommended. The reason is that serde serializes unnamed structs as vectors and it can be represented as a type in martian only if all the fields serialize to the same martian type. i.e `struct Good(u8, u16, i32);` can be represented as `int[]`, but there is no martian representation for `struct Bad(u8, String, Foo)`. This property is hard to check in a procedural macro. Hence it is strongly recommended to use a named struct. Naming the fields would also improve the readability of the code.",
                )
                .to_compile_error()
                .into()
            },
        },
        Data::Enum(ref enum_data) => {
            let mut variant_type_map = HashMap::new();
            for variant in &enum_data.variants {
                let this_type = match variant.fields {
                    Fields::Named(_) => MartianPrimaryType::Map,
                    Fields::Unnamed(_) => MartianPrimaryType::Map,
                    Fields::Unit => MartianPrimaryType::Str,
                };
                variant_type_map.entry(this_type).or_insert(Vec::new()).push(variant.ident.to_string());
            }
            match variant_type_map.len() {
                0 => { // Empty Enum
                    syn::Error::new_spanned(
                        input,
                        "MartianType cannot be derived on enums with no variants. They are most likely not useful as a MartianType."
                    )
                    .to_compile_error()
                    .into()
                },
                1 => { // All variants mape to either Str or Map, we can generate the derive
                    if variant_type_map.contains_key(&MartianPrimaryType::Str) {
                        quote![
                            #[automatically_derived]
                            impl #impl_generics ::martian::AsMartianPrimaryType for #ident #ty_generics #where_clause {
                                fn as_martian_primary_type() -> ::martian::MartianPrimaryType {
                                    ::martian::MartianPrimaryType::Str
                                }
                            }
                        ].into()
                    } else {
                        quote![
                            #[automatically_derived]
                            impl #impl_generics ::martian::AsMartianPrimaryType for #ident #ty_generics #where_clause {
                                fn as_martian_primary_type() -> ::martian::MartianPrimaryType {
                                    ::martian::MartianPrimaryType::Map
                                }
                            }
                        ].into()
                    }
                }
                2 => {
                    let map_fields = variant_type_map.get(&MartianPrimaryType::Map).unwrap().join(", ");
                    let str_fields = variant_type_map.get(&MartianPrimaryType::Str).unwrap().join(", ");
                    syn::Error::new_spanned(
                        input,
                        format!(
                            "Deriving MartianType on enum {} failed because some of the variants in this enum map to MartianPrimaryType::Map while other variants map to MartianPrimaryType::Str.\n    1) MartianPrimaryType::Map -> [{}]\n    2) MartianPrimaryType::Str -> {}\nThe reason this happens is because serde will deserialize different variants of an enum differently. As a result, we cannot assign a unique martian type for this enum. Consider redesigning your enum to account for this.",
                            ident.to_string(),
                            map_fields,
                            str_fields
                        )
                    )
                    .to_compile_error()
                    .into()
                }
                _ => unreachable!(),
            }
        },
    }
}

/// A macro to define a new struct that implements `MartianFileType` trait
///
/// Because this is a procedural macro, as of now, you can only define it
/// outside functions because they cannot be expanded to statements.
/// ```rust
/// use serde::{Serialize, Deserialize};
/// use martian_derive::martian_filetype;
/// use martian::MartianFileType;
/// martian_filetype! { TxtFile, "txt" }
/// martian_filetype! { BamIndexFile, "bam.bai" }
/// fn main() {
///     assert_eq!(TxtFile::extension(), "txt");
///     assert_eq!(
///         BamIndexFile::new("/path/to/folder", "filename").as_ref(),
///         std::path::Path::new("/path/to/folder/filename.bam.bai")
///     )
/// }
/// ```
#[proc_macro]
pub fn martian_filetype(item: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let item2 = proc_macro2::TokenStream::from(item.clone());

    // ::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    // STEP 1
    // ::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    // Convert the tokenstream representing the inputs to the macro as a string.
    // Do incremental manual parsing instead of a single regex because we want
    // to generate compile errors that are very specific
    let input = item.to_string();

    // ::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    // STEP 2
    // ::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    // Check that the input is two items separated by a comma. Generate a compile
    // error if it is not in the expected format. First part is the struct name
    // and the second part is the extension
    let parts = input.split(",").collect::<Vec<_>>();
    if parts.len() != 2 {
        return syn::Error::new_spanned(item2, "The input to the martian_filetype! macro needs to be two items separated by a comma.\n\tThe first item is the struct name that will be generated and the second item is the filetype extension within double quotes.\n\tFor example martian_filetype! {TxtFile, \"txt\"}")
            .to_compile_error()
            .into();
    }
    let struct_name = parts[0].trim();
    let extension = parts[1].trim();

    // ::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    // STEP 3
    // ::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    // Check that the struct name is not empty. Generate a compiler error
    // otherwise.
    let struct_ident = match syn::parse_str::<Ident>(struct_name) {
        Ok(ident) => ident,
        Err(_) => return syn::Error::new_spanned(item2, format!("The first item `{}` in the martian_filetype! macro should be a valid identifier that can be used as a struct name.", struct_name))
                .to_compile_error()
                .into(),
    };

    // ::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    // STEP 5
    // ::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    // The struct name is in good shape now. Check that the extension is a
    // string literal enclosed in double quotes. Generate a compiler error
    // otherwise.
    if !(extension.starts_with('"') && extension.ends_with('"')) {
        return syn::Error::new_spanned(item2, "The second item in the martian_filetype! macro should be a string literal enclosed in double quotes.")
                .to_compile_error()
                .into();
    }

    // ::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    // STEP 6
    // ::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    // Make sure that the string literal within the quotes in the extension
    // is not empty. Generate a compiler error otherwise.
    let chars_within_quotes: Vec<_> = {
        let mut chars: Vec<_> = extension.chars().skip(1).collect();
        chars.pop();
        chars
    };
    if chars_within_quotes.is_empty() {
        return syn::Error::new_spanned(
                item2,
                "The extension for a filetype cannot be empty. Consider using a PathBuf for filenames without any extension."
            )
            .to_compile_error()
            .into();
    }

    // ::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    // STEP 7
    // ::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    // Make sure that the extension specified but does not start or end with a
    // dot (.). Generate a compiler error otherwise.
    if chars_within_quotes[0] == '.' {
        return syn::Error::new_spanned(
            item2,
            "No need to specify the leading dot(.) in the extension",
        )
        .to_compile_error()
        .into();
    }
    if *chars_within_quotes.last().unwrap() == '.' {
        return syn::Error::new_spanned(item2, "Extensions cannot end in a dot(.)")
            .to_compile_error()
            .into();
    }

    // ::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    // STEP 8
    // ::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    // Make sure that the extension is ascii alphanumeric or a dot (.). We have
    // already checked for leading/trailing dots
    for (i, c) in chars_within_quotes.iter().enumerate() {
        if !((i > 0 && (c.is_ascii_alphanumeric() || *c == '.')) || c.is_ascii_alphabetic()) {
            return syn::Error::new_spanned(
                    item2,
                    format!("The extension `{}` in the martian_filetype! macro should be alphanumeric (internal dots(.) are okay) starting with an alphabet.\n\tFound invalid character `{}` at position {}", extension, c, i))
                .to_compile_error()
                .into();
        }
    }

    // ::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    // STEP 9
    // ::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
    // Now we are ready to actually generate the code.
    let extension: String = chars_within_quotes.iter().collect();
    quote![
        #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
        pub struct #struct_ident(::std::path::PathBuf);
        #[automatically_derived]
        impl ::martian::MartianFileType for #struct_ident {
            fn extension() -> &'static str {
                #extension
            }
            fn new(
                file_path: impl ::std::convert::AsRef<::std::path::Path>,
                file_name: impl ::std::convert::AsRef<::std::path::Path>,
            ) -> Self {
                let mut path = ::std::path::PathBuf::from(file_path.as_ref());
                path.push(file_name);
                if !path.to_string_lossy().ends_with(Self::extension()) {
                    let full_extension = match path.extension() {
                        Some(ext) => format!("{}.{}", ext.to_string_lossy(), Self::extension()),
                        _ => Self::extension().to_string(),
                    };
                    path.set_extension(full_extension);
                }
                #struct_ident(path)
            }
        }
        #[automatically_derived]
        impl ::std::convert::AsRef<::std::path::Path> for #struct_ident {
            fn as_ref(&self) -> &::std::path::Path {
                &self.0
            }
        }
        #[automatically_derived]
        impl<T> ::std::convert::From<T> for #struct_ident
        where
            ::std::path::PathBuf: ::std::convert::From<T>,
        {
            fn from(source: T) -> Self {
                #struct_ident(::std::path::PathBuf::from(source))
            }
        }
        #[automatically_derived]
        impl ::martian::AsMartianPrimaryType for #struct_ident {
            fn as_martian_primary_type() -> ::martian::MartianPrimaryType {
                ::martian::MartianPrimaryType::FileType(String::from(<#struct_ident as ::martian::MartianFileType>::extension()))
            }
        }
    ]
    .into()
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
        t.compile_fail("tests/ui_martian_type/*.rs");
        t.compile_fail("tests/ui_martian_filetype/*.rs");
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

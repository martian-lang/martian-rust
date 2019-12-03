//!
//! This module defines objects that would help auto generate mro definitions
//! for a stage and also defines the types that exist in the martian world
//!
//! # Mro syntax
//! Think about `mro` as an entity with the following properties
//! - Stage name
//! - List of stage/chunk inputs/outputs, each with a martian type
//! - Source for execution
//! - Attributes (mem_gb, vmem_gb, threads, volatile etc.)
//!

use crate::MartianVoid;
use failure::{format_err, Error};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fmt::{Display, Write};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::string::ToString;

/// Keywords used in the martian language. Using these keywords as mro field names
/// is disallowed.
pub const MARTIAN_TOKENS: &[&str] = &[
    "in", "out", "stage", "volatile", "strict", "true", "split", "filetype", "src", "py", "comp",
    "retain", "mro", "using", "int", "float", "string", "map", "bool", "path", "__null__",
];

/// Defines how an entity that denotes some part of the mro is displayed
pub trait MroDisplay {
    fn mro_string(&self, field_width: Option<usize>) -> String {
        match field_width {
            Some(width) => {
                let min_width = self.min_width();
                assert!(
                    width >= min_width,
                    format!("Need a minimum width of {}. Found {}", min_width, width)
                );
                self.mro_string_with_width(width)
            }
            None => self.mro_string_no_width(),
        }
    }
    fn min_width(&self) -> usize;
    fn mro_string_no_width(&self) -> String;
    fn mro_string_with_width(&self, field_width: usize) -> String;
}

/// A generic display impl for MroDisplay does not work due
/// to conflicting blanket impl. This is a simple macro to
/// write out the Display trait for MroDisplay
macro_rules! mro_display_to_display {
    ($type:ty) => {
        impl Display for $type {
            fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str(&self.mro_string_no_width())
            }
        }
    };
    ($type:ty, $width:ident) => {
        impl Display for $type {
            fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str(&self.mro_string_with_width($width))
            }
        }
    };
}

macro_rules! usize_field_len {
    () => {
        fn min_width(&self) -> usize {
            self.mro_string_no_width().len()
        }
        fn mro_string_with_width(&self, field_width: usize) -> String {
            let value = self.mro_string_no_width();
            format!("{value:<width$}", value = value, width = field_width)
        }
    };
}

/// Primary data types in Martian world
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, Hash)]
pub enum MartianPrimaryType {
    Int,
    Float,
    Str,
    Bool,
    Map,
    Path,
    FileType(String),
}

impl MroDisplay for MartianPrimaryType {
    usize_field_len! {}
    fn mro_string_no_width(&self) -> String {
        let value = match *self {
            MartianPrimaryType::Int => "int",
            MartianPrimaryType::Float => "float",
            MartianPrimaryType::Str => "string",
            MartianPrimaryType::Bool => "bool",
            MartianPrimaryType::Map => "map",
            MartianPrimaryType::Path => "path",
            MartianPrimaryType::FileType(ref ext) => ext,
        };
        value.to_string()
    }
}

impl FromStr for MartianPrimaryType {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let prim_ty = match s {
            "int" => MartianPrimaryType::Int,
            "float" => MartianPrimaryType::Float,
            "string" => MartianPrimaryType::Str,
            "bool" => MartianPrimaryType::Bool,
            "map" => MartianPrimaryType::Map,
            "path" => MartianPrimaryType::Path,
            _ => return Err(format_err!("Cannot find the martian primary type from {}. Supported entries are [int, float, string, bool, map, path]", s)),
        };
        Ok(prim_ty)
    }
}

mro_display_to_display! {MartianPrimaryType}

/// Primary Data type in martian + Arrays (which are derived from primary types)
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub enum MartianBlanketType {
    Primary(MartianPrimaryType),
    Array(MartianPrimaryType),
}

impl MroDisplay for MartianBlanketType {
    usize_field_len! {}
    fn mro_string_no_width(&self) -> String {
        match *self {
            MartianBlanketType::Primary(ref primary) => primary.to_string(),
            MartianBlanketType::Array(ref primary) => format!("{}[]", primary.to_string()),
        }
    }
}
mro_display_to_display! {MartianBlanketType}

impl FromStr for MartianBlanketType {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.ends_with("[]") {
            let t = s.get(0..s.len() - 2).unwrap();
            Ok(MartianBlanketType::Array(MartianPrimaryType::from_str(t)?))
        } else {
            Ok(MartianBlanketType::Primary(MartianPrimaryType::from_str(
                s,
            )?))
        }
    }
}

/// A trait that tells you how to convert a Rust data type to a
/// basic Martian type.
pub trait AsMartianPrimaryType {
    fn as_martian_primary_type() -> MartianPrimaryType;
}

/// A trait that defines how to convert this Rust type into an `MartianBlanketType`.
/// Not all rust types can be converted to an `MartianBlanketType`.
/// Not defined for
/// - Unit, the type of () in Rust.
/// - Unit Struct For example `struct Unit` or `PhantomData<T>`. It represents
///     a named value containing no data.
/// Any type which implements `AsMartianPrimaryType` also implements `AsMartianBlanketType`
/// It is stringly recommended not to extend any types with this trait, instead
/// use the `AsMartianPrimaryType` trait.
pub trait AsMartianBlanketType {
    fn as_martian_blanket_type() -> MartianBlanketType;
}

/// Macro for implementing `AsMartianPrimaryType` trait
macro_rules! impl_primary_mro_type {
    ($rust_type:ty, $mro_type:stmt) => {
        impl AsMartianPrimaryType for $rust_type {
            fn as_martian_primary_type() -> MartianPrimaryType {
                $mro_type
            }
        }
    };
}

impl_primary_mro_type!(i8, MartianPrimaryType::Int);
impl_primary_mro_type!(i16, MartianPrimaryType::Int);
impl_primary_mro_type!(i32, MartianPrimaryType::Int);
impl_primary_mro_type!(i64, MartianPrimaryType::Int);
// impl_primary_mro_type!(i128, MartianPrimaryType::Int);
impl_primary_mro_type!(isize, MartianPrimaryType::Int);
impl_primary_mro_type!(u8, MartianPrimaryType::Int);
impl_primary_mro_type!(u16, MartianPrimaryType::Int);
impl_primary_mro_type!(u32, MartianPrimaryType::Int);
impl_primary_mro_type!(u64, MartianPrimaryType::Int);
// impl_primary_mro_type!(u128, MartianPrimaryType::Int);
impl_primary_mro_type!(usize, MartianPrimaryType::Int);
impl_primary_mro_type!(bool, MartianPrimaryType::Bool);
impl_primary_mro_type!(f32, MartianPrimaryType::Float);
impl_primary_mro_type!(f64, MartianPrimaryType::Float);
impl_primary_mro_type!(char, MartianPrimaryType::Str);
impl_primary_mro_type!(String, MartianPrimaryType::Str);
impl_primary_mro_type!(&'static str, MartianPrimaryType::Str);
impl_primary_mro_type!(Path, MartianPrimaryType::Path);
impl_primary_mro_type!(PathBuf, MartianPrimaryType::Path);

impl<T: AsMartianPrimaryType> AsMartianBlanketType for T {
    fn as_martian_blanket_type() -> MartianBlanketType {
        MartianBlanketType::Primary(T::as_martian_primary_type())
    }
}

impl<T: AsMartianBlanketType> AsMartianBlanketType for Option<T> {
    fn as_martian_blanket_type() -> MartianBlanketType {
        // Any variable can be `null` in Martian
        T::as_martian_blanket_type()
    }
}

impl<T: AsMartianPrimaryType> AsMartianBlanketType for Vec<T> {
    fn as_martian_blanket_type() -> MartianBlanketType {
        MartianBlanketType::Array(T::as_martian_primary_type())
    }
}

impl<K: AsMartianPrimaryType, H> AsMartianBlanketType for HashSet<K, H> {
    fn as_martian_blanket_type() -> MartianBlanketType {
        MartianBlanketType::Array(K::as_martian_primary_type())
    }
}

impl<K, V, H> AsMartianPrimaryType for HashMap<K, V, H> {
    fn as_martian_primary_type() -> MartianPrimaryType {
        MartianPrimaryType::Map
    }
}

/// Each variable that is listed in the mro along with it's type form
/// a `MroField`.
///
/// For example, the following stage:
/// ```mro
/// stage SORT_ITEMS(
///     in  int[] unsorted,
///     in  bool  reverse,
///     out int[] sorted,
///     src comp  "my_stage martian sort_items",
/// )
/// ```
/// contains 3 `MroFields`
/// - MroField { name: "unsorted", ty: MartianBlanketType::Array(MartianPrimaryType::Int)}
/// - MroField { name: "reverse", ty: MartianBlanketType::Primary(MartianPrimaryType::Bool)}
/// - MroField { name: "sorted", ty: MartianBlanketType::Array(MartianPrimaryType::Int)}
#[derive(Debug, Serialize, Clone, Deserialize, PartialEq, Eq)]
pub struct MroField {
    name: String,
    ty: MartianBlanketType,
    retain: bool,
}

/// `field_width` will decide the length of the type column
impl MroDisplay for MroField {
    fn mro_string_no_width(&self) -> String {
        format!("{ty} {name}", ty = self.ty.to_string(), name = &self.name)
    }
    fn min_width(&self) -> usize {
        self.ty.min_width()
    }

    fn mro_string_with_width(&self, field_width: usize) -> String {
        format!(
            "{ty} {name}",
            ty = self.ty.mro_string_with_width(field_width),
            name = &self.name
        )
    }
}

mro_display_to_display! {MroField}

impl MroField {
    /// Create a new `MroField` with the given name and type.
    /// The field has a default `retain = false`.
    pub fn new(name: impl ToString, ty: MartianBlanketType) -> Self {
        let field = MroField {
            name: name.to_string(),
            ty,
            retain: false,
        };
        field.verify(); // No use case to resultify this so far
        field
    }
    /// Create a new `MroField` with the given name and type, with the
    /// `retain` field set to true
    pub fn retained(name: impl ToString, ty: MartianBlanketType) -> Self {
        let mut field = Self::new(name, ty);
        field.retain = true;
        field
    }
    // Check that name does not match any martian token.
    fn verify(&self) {
        for &token in MARTIAN_TOKENS.iter() {
            assert!(
                self.name != token,
                "Martian token {} cannot be used as field name",
                token
            );
        }
        assert!(!self.name.starts_with("__"));
    }
}

/// A trait that defines how to expand a struct into a list of `MroField`s
/// The `MartianStage` and `MartianMain` traits already has independent associated
/// types for stage/chunk inputs and outputs. If those associated types implement
/// this trait, then we can readily generate all the mro variables with the appropriate
/// type and put them at the right place (withing stage def or chunk def).
///
/// TODO : Auto derive for structs with named fields if all the fields implement `AsMartianBlanketType`
pub trait MartianStruct {
    /// How to convert this struct into a list of `MroField`s
    fn mro_fields() -> Vec<MroField>;
}

impl MartianStruct for MartianVoid {
    fn mro_fields() -> Vec<MroField> {
        Vec::new()
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub enum Volatile {
    Strict,
}

impl FromStr for Volatile {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "strict" => Ok(Volatile::Strict),
            _ => Err(format!("Expected strict for volatile, Found {}", s)),
        }
    }
}

// Maybe just need display?
impl MroDisplay for Volatile {
    usize_field_len! {}
    fn mro_string_no_width(&self) -> String {
        match self {
            Volatile::Strict => "strict".into(),
        }
    }
}

mro_display_to_display! {Volatile}

const TAB_WIDTH_FOR_MRO: usize = 4;
macro_rules! mro_using {
    ($($property:ident: $type:ty),*) => {
        /// Stuff that comes in the `using` section of a stage definition
        ///
        /// For example:
        /// ```mro
        /// using (
        ///     mem_gb  = 4,
        ///     threads = 16,
        /// )
        /// ```
        #[derive(Debug, Default, Clone, Copy, Serialize, Deserialize)]
        pub struct MroUsing {
            $(pub $property: Option<$type>,)*
        }

        impl MroUsing {
            /// If all fields as None, return False
            pub fn need_using(&self) -> bool {
                !($(self.$property.is_none())&&*)
            }
        }

        /// Using section
        /// ```md
        /// mem_gb = 1,
        /// ```
        impl MroDisplay for MroUsing {
            fn min_width(&self) -> usize {
                let mut w = 0;
                $(if self.$property.is_some() {
                    w = std::cmp::max(w, stringify!($property).len());
                })*
                w
            }

            fn mro_string_no_width(&self) -> String {
                self.mro_string_with_width(self.min_width())
            }

            fn mro_string_with_width(&self, field_width: usize) -> String {
                let mut result = String::new();
                // If every field is None, return empty String
                if !self.need_using() {
                    return result;
                }
                $(
                    if let Some($property) = self.$property {
                        writeln!(
                            &mut result,
                            "{key:<width$} = {value},",
                            key=stringify!($property),
                            width=field_width,
                            value=$property
                        ).unwrap()
                    }
                )*
                result
            }
        }
        mro_display_to_display! {MroUsing}
    };
}

mro_using! {mem_gb: i16, vmem_gb: i16, threads: i16, volatile: Volatile}

/// Input and outputs fields together
#[derive(Debug, Default)]
pub struct InAndOut {
    pub inputs: Vec<MroField>,
    pub outputs: Vec<MroField>,
}

impl InAndOut {
    fn retain_field_names(&self) -> Vec<String> {
        self.outputs
            .iter()
            .filter(|field| field.retain)
            .map(|field| field.name.clone())
            .collect()
    }
}

impl MroDisplay for InAndOut {
    fn min_width(&self) -> usize {
        std::cmp::max(
            self.inputs
                .iter()
                .map(|field| field.min_width())
                .max()
                .unwrap_or(0),
            self.outputs
                .iter()
                .map(|field| field.min_width())
                .max()
                .unwrap_or(0),
        )
    }

    fn mro_string_no_width(&self) -> String {
        self.mro_string_with_width(self.min_width())
    }

    fn mro_string_with_width(&self, field_width: usize) -> String {
        let mut result = String::new();
        for (key, fields) in &[("in", &self.inputs), ("out", &self.outputs)] {
            for field in *fields {
                writeln!(
                    &mut result,
                    "{key:3} {f},",
                    key = key,
                    f = field.mro_string_with_width(field_width)
                )
                .unwrap();
            }
        }
        result
    }
}
mro_display_to_display! {InAndOut}

/// The list of filetypes we list at the top of the mro.
/// This struct is a simple wrapper around a HashSet of all file extensions.
#[derive(Debug, PartialEq, Default)]
pub struct FiletypeHeader(HashSet<String>);

impl From<&MroField> for FiletypeHeader {
    fn from(field: &MroField) -> FiletypeHeader {
        let mut result = HashSet::new();
        match field.ty {
            MartianBlanketType::Primary(MartianPrimaryType::FileType(ref ext)) => {
                result.insert(ext.to_string());
            }
            MartianBlanketType::Array(MartianPrimaryType::FileType(ref ext)) => {
                result.insert(ext.to_string());
            }
            _ => {}
        }
        FiletypeHeader(result)
    }
}

impl From<&InAndOut> for FiletypeHeader {
    fn from(in_out: &InAndOut) -> FiletypeHeader {
        let mut result = HashSet::new();
        for field in in_out.inputs.iter().chain(in_out.outputs.iter()) {
            result.extend(FiletypeHeader::from(field).0);
        }
        FiletypeHeader(result)
    }
}

impl From<&StageMro> for FiletypeHeader {
    fn from(stage_mro: &StageMro) -> FiletypeHeader {
        let mut result = FiletypeHeader::from(&stage_mro.stage_in_out);
        if let Some(ref chunk_in_out) = stage_mro.chunk_in_out {
            result.0.extend(FiletypeHeader::from(chunk_in_out).0)
        }
        result
    }
}

impl FiletypeHeader {
    /// Find out all the filetypes in the stage and add the extensions
    /// to the internal hashset which stores all the extensions
    pub fn add_stage(&mut self, stage_mro: &StageMro) {
        self.0.extend(FiletypeHeader::from(stage_mro).0);
    }
}

// Just need display here
impl MroDisplay for FiletypeHeader {
    fn min_width(&self) -> usize {
        // No configuration here
        0
    }
    fn mro_string_no_width(&self) -> String {
        let mut result = String::new();
        if self.0.is_empty() {
            return result;
        }
        let mut extensions: Vec<_> = self.0.iter().collect();
        writeln!(&mut result).unwrap();
        extensions.sort();
        for ext in extensions {
            writeln!(&mut result, "filetype {};", ext).unwrap();
        }
        writeln!(&mut result).unwrap();
        result
    }
    fn mro_string_with_width(&self, _: usize) -> String {
        self.mro_string_no_width()
    }
}

mro_display_to_display! { FiletypeHeader }

/// An object that can generate a `StageMro`
///
/// Can be auto generated using proc macro attribute
/// `#[make_mro]` on MartianMain or MartianStage
/// implementations if the associated types implement `MartianStruct`
pub trait MroMaker {
    fn stage_mro(adapter_name: impl ToString, stage_key: impl ToString) -> StageMro {
        let result = StageMro {
            stage_name: Self::stage_name(),
            adapter_name: adapter_name.to_string(),
            stage_key: stage_key.to_string(),
            stage_in_out: Self::stage_in_and_out(),
            chunk_in_out: Self::chunk_in_and_out(),
            using_attrs: Self::using_attributes(),
        };
        result.verify();
        result
    }
    fn mro(adapter_name: impl ToString, stage_key: impl ToString) -> String {
        let stage_mro = Self::stage_mro(adapter_name, stage_key);
        let filetype = FiletypeHeader::from(&stage_mro);
        format!("{}{}", filetype.to_string(), stage_mro.to_string())
    }
    fn stage_name() -> String;
    fn stage_in_and_out() -> InAndOut;
    fn chunk_in_and_out() -> Option<InAndOut>;
    fn using_attributes() -> MroUsing;
}

/// All the data needed to create a stage definition mro.
#[derive(Debug)]
pub struct StageMro {
    stage_name: String,     // e.g CORRECT_BARCODES in `stage CORRECT_BARCODES(..)`
    adapter_name: String, // Martian adapter e.g `cr_slfe` in `src comp "cr_slfe martian correct_barcodes"
    stage_key: String, // Key used in the hashmap containing all stages e.g `correct_barcodes` in `src comp "cr_slfe martian correct_barcodes"
    stage_in_out: InAndOut, // Inputs and outputs of the stage
    chunk_in_out: Option<InAndOut>, // Inputs and outputs of the chunk. None indicates a stage with only a main
    using_attrs: MroUsing,          // Things coming under using
}

impl MroDisplay for StageMro {
    fn min_width(&self) -> usize {
        0
    }
    fn mro_string_no_width(&self) -> String {
        self.mro_string_with_width(self.min_width())
    }

    fn mro_string_with_width(&self, field_width: usize) -> String {
        let mut result = String::new();
        // Determing the field width for the type field
        let ty_width = std::cmp::max(
            self.stage_in_out.min_width(),
            self.chunk_in_out
                .as_ref()
                .map(|chunk| chunk.min_width())
                .unwrap_or(0),
        );
        let indent = format!("{blank:indent$}", blank = "", indent = field_width);
        writeln!(&mut result, "stage {}(", self.stage_name).unwrap();

        for line in self.stage_in_out.mro_string(Some(ty_width)).lines() {
            writeln!(&mut result, "{}{}", indent, line).unwrap();
        }
        writeln!(
            &mut result,
            r#"{space}src {comp:ty_width$} "{adapter} martian {stage_key}","#,
            space = indent,
            comp = "comp",
            ty_width = ty_width,
            adapter = self.adapter_name,
            stage_key = self.stage_key,
        )
        .unwrap();

        if let Some(ref chunk_in_out) = self.minified_chunk_in_outs() {
            writeln!(&mut result, ") split (").unwrap();
            for line in chunk_in_out.mro_string(Some(ty_width)).lines() {
                writeln!(&mut result, "{}{}", indent, line).unwrap();
            }
        }

        if self.using_attrs.need_using() {
            writeln!(&mut result, ") using (").unwrap();
            for line in self.using_attrs.mro_string(None).lines() {
                writeln!(&mut result, "{}{}", indent, line).unwrap();
            }
        }
        let retain_names = self.stage_in_out.retain_field_names();
        if !retain_names.is_empty() {
            writeln!(&mut result, ") retain (").unwrap();
            for line in retain_names {
                writeln!(&mut result, "{}{},", indent, line).unwrap();
            }
        }
        writeln!(&mut result, ")").unwrap();

        result
    }
}

mro_display_to_display! {StageMro, TAB_WIDTH_FOR_MRO}

impl StageMro {
    fn minified_chunk_in_outs(&self) -> Option<InAndOut> {
        match self.chunk_in_out {
            Some(ref chunk_in_out) => {
                let chunk_in = chunk_in_out.inputs.clone();
                let skip_names: HashSet<_> = self
                    .stage_in_out
                    .outputs
                    .iter()
                    .map(|field| &field.name)
                    .collect();
                let chunk_out: Vec<_> = chunk_in_out
                    .outputs
                    .iter()
                    .filter(|field| !skip_names.contains(&field.name))
                    .cloned()
                    .collect();
                Some(InAndOut {
                    inputs: chunk_in,
                    outputs: chunk_out,
                })
            }
            None => None,
        }
    }
    fn verify(&self) {
        // By design, all the field names are guaranteed to be not
        // any of the martian tokens. It raises a compile error when
        // deriving MartianStruct and is checked when creating a
        // MaroField wusing new() which is the only public entry point.
        // So we don't have anything to check for a MainOnly stage
        if self.chunk_in_out.is_none() {
            return;
        }

        let chunk_in_out = self.chunk_in_out.as_ref().unwrap();
        // Do not allow the same field name in stage and chunk inputs
        // O(mn) is good enough
        for f_chunk in chunk_in_out.inputs.iter() {
            for f_stage in self.stage_in_out.inputs.iter() {
                assert!(
                    !(f_chunk.name == f_stage.name),
                    "ERROR: Found identical field {} in stage and chunk inputs, which is not allowed",
                    f_chunk.name
                )
            }
        }

        // Fields having same name in stage and chunk outputs
        // should have identical types.
        // O(mn) is good enough
        for f_chunk in chunk_in_out.outputs.iter() {
            for f_stage in self.stage_in_out.outputs.iter() {
                if f_chunk.name == f_stage.name {
                    assert!(
                        f_chunk.ty == f_stage.ty,
                        "ERROR: Found identical field {} in stage and chunk outputs, but it has type {} in stage outputs and type {} in chunk outputs.",
                        f_chunk.name, f_stage.ty, f_chunk.ty,
                    )
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use indoc::indoc;
    use MartianBlanketType::*;
    use MartianPrimaryType::*;

    #[test]
    fn test_martian_primary_type_display() {
        assert_eq!(Int.mro_string_no_width(), "int");
        assert_eq!(Int.mro_string(Some(4)), "int ");
        assert_eq!(FileType("txt".into()).mro_string_with_width(5), "txt  ");
        assert_eq!(FileType("fastq.lz4".into()).mro_string(None), "fastq.lz4");
    }

    #[test]
    fn test_martian_type_display() {
        assert_eq!(Primary(Int).mro_string_no_width(), "int");
        assert_eq!(Array(Int).mro_string(Some(7)), "int[]  ");
        assert_eq!(
            Array(FileType("txt".into())).mro_string_with_width(5),
            "txt[]"
        );
        assert_eq!(
            Primary(FileType("fastq.lz4".into())).mro_string(None),
            "fastq.lz4"
        );
    }

    #[test]
    fn test_volatile_parse() {
        assert_eq!("strict".parse::<Volatile>(), Ok(Volatile::Strict));
        assert!("foo".parse::<Volatile>().is_err());
    }

    #[test]
    fn test_volatile_display() {
        let vol = Volatile::Strict;
        assert_eq!(vol.mro_string(None), "strict");
        assert_eq!(vol.mro_string_no_width(), "strict");
        assert_eq!(vol.min_width(), 6);
        assert_eq!(vol.mro_string(Some(10)), "strict    ");
    }

    #[test]
    fn test_mro_using_display() {
        assert_eq!(
            MroUsing {
                mem_gb: Some(1),
                ..Default::default()
            }
            .to_string(),
            indoc!(
                "
                mem_gb = 1,
            "
            )
        );

        assert_eq!(
            MroUsing {
                mem_gb: Some(1),
                vmem_gb: Some(4),
                volatile: Some(Volatile::Strict),
                ..Default::default()
            }
            .mro_string_no_width(),
            indoc!(
                "
                mem_gb   = 1,
                vmem_gb  = 4,
                volatile = strict,
            "
            )
        );

        assert_eq!(
            MroUsing {
                threads: Some(2),
                ..Default::default()
            }
            .mro_string_with_width(10),
            indoc!(
                "
                threads    = 2,
            "
            )
        );
    }

    #[test]
    fn test_mro_using_need_using() {
        assert_eq!(MroUsing::default().need_using(), false);
        assert_eq!(
            MroUsing {
                mem_gb: Some(1),
                ..Default::default()
            }
            .need_using(),
            true
        );
        assert_eq!(
            MroUsing {
                mem_gb: Some(1),
                threads: Some(3),
                ..Default::default()
            }
            .need_using(),
            true
        );
    }

    #[test]
    fn test_in_and_out_display() {
        let in_out = InAndOut {
            inputs: vec![
                MroField::new("unsorted", Array(Float)),
                MroField::new("reverse", Primary(Bool)),
            ],
            outputs: vec![
                MroField::new("sorted", Array(Float)),
                MroField::new("sum", Primary(Float)),
            ],
        };
        let expected = indoc!(
            "
            in  float[] unsorted,
            in  bool    reverse,
            out float[] sorted,
            out float   sum,
        "
        );
        assert_eq!(in_out.mro_string(None), expected);
        assert_eq!(in_out.to_string(), expected);
    }

    #[test]
    fn test_stage_mro_display_1() {
        let expected_mro = indoc!(
            r#"
            stage SUM_SQUARES(
                in  float[] values,
                out float   sum,
                src comp    "my_adapter martian sum_squares",
            ) split (
                in  float   value,
                out float   value,
            )
            "#
        );

        let stage_mro = StageMro {
            stage_name: "SUM_SQUARES".into(),
            adapter_name: "my_adapter".into(),
            stage_key: "sum_squares".into(),
            stage_in_out: InAndOut {
                inputs: vec![MroField::new("values", Array(Float))],
                outputs: vec![MroField::new("sum", Primary(Float))],
            },
            chunk_in_out: Some(InAndOut {
                inputs: vec![MroField::new("value", Primary(Float))],
                outputs: vec![MroField::new("value", Primary(Float))],
            }),
            using_attrs: MroUsing::default(),
        };

        assert_eq!(stage_mro.to_string(), expected_mro);
    }

    #[test]
    fn test_stage_mro_display_2() {
        let expected_mro = indoc!(
            r#"
            stage SUM_SQUARES(
                in  float[] values,
                out float   sum,
                src comp    "my_adapter martian sum_squares",
            ) split (
            )
            "#
        );

        let stage_mro = StageMro {
            stage_name: "SUM_SQUARES".into(),
            adapter_name: "my_adapter".into(),
            stage_key: "sum_squares".into(),
            stage_in_out: InAndOut {
                inputs: vec![MroField::new("values", Array(Float))],
                outputs: vec![MroField::new("sum", Primary(Float))],
            },
            chunk_in_out: Some(InAndOut::default()),
            using_attrs: MroUsing::default(),
        };

        assert_eq!(stage_mro.to_string(), expected_mro);
    }

    #[test]
    fn test_stage_mro_display_3() {
        let expected_mro = indoc!(
            r#"
            stage SUM_SQUARES(
                in  float[] values,
                out float   sum,
                src comp    "my_adapter martian sum_squares",
            )
            "#
        );

        let stage_mro = StageMro {
            stage_name: "SUM_SQUARES".into(),
            adapter_name: "my_adapter".into(),
            stage_key: "sum_squares".into(),
            stage_in_out: InAndOut {
                inputs: vec![MroField::new("values", Array(Float))],
                outputs: vec![MroField::new("sum", Primary(Float))],
            },
            chunk_in_out: None,
            using_attrs: MroUsing::default(),
        };

        assert_eq!(stage_mro.to_string(), expected_mro);
    }

    #[test]
    fn test_stage_mro_display_4() {
        let expected_mro = indoc!(
            r#"
            stage SUM_SQUARES(
                in  float[] values,
                out float   sum,
                src comp    "my_adapter martian sum_squares",
            ) using (
                mem_gb  = 1,
                threads = 2,
            )
            "#
        );

        let stage_mro = StageMro {
            stage_name: "SUM_SQUARES".into(),
            adapter_name: "my_adapter".into(),
            stage_key: "sum_squares".into(),
            stage_in_out: InAndOut {
                inputs: vec![MroField::new("values", Array(Float))],
                outputs: vec![MroField::new("sum", Primary(Float))],
            },
            chunk_in_out: None,
            using_attrs: MroUsing {
                mem_gb: Some(1),
                threads: Some(2),
                ..Default::default()
            },
        };

        assert_eq!(stage_mro.to_string(), expected_mro);
    }

    #[test]
    fn test_stage_mro_display_5() {
        let expected_mro = indoc!(
            r#"
            stage SUM_SQUARES(
                in  float[] values,
                out float   sum,
                src comp    "my_adapter martian sum_squares",
            ) using (
                mem_gb  = 1,
                threads = 2,
            ) retain (
                sum,
            )
            "#
        );

        let stage_mro = StageMro {
            stage_name: "SUM_SQUARES".into(),
            adapter_name: "my_adapter".into(),
            stage_key: "sum_squares".into(),
            stage_in_out: InAndOut {
                inputs: vec![MroField::new("values", Array(Float))],
                outputs: vec![MroField::retained("sum", Primary(Float))],
            },
            chunk_in_out: None,
            using_attrs: MroUsing {
                mem_gb: Some(1),
                threads: Some(2),
                ..Default::default()
            },
        };

        assert_eq!(stage_mro.to_string(), expected_mro);
    }

    #[test]
    #[should_panic]
    fn test_stage_mro_display_duplicate_inputs() {
        let stage_mro = StageMro {
            stage_name: "SUM_SQUARES".into(),
            adapter_name: "my_adapter".into(),
            stage_key: "sum_squares".into(),
            stage_in_out: InAndOut {
                inputs: vec![MroField::new("values", Array(Float))],
                outputs: vec![MroField::new("sum", Primary(Float))],
            },
            chunk_in_out: Some(InAndOut {
                inputs: vec![MroField::new("values", Array(Float))],
                outputs: Vec::new(),
            }),
            using_attrs: MroUsing {
                mem_gb: Some(1),
                threads: Some(2),
                ..Default::default()
            },
        };
        stage_mro.verify();
    }

    #[test]
    #[should_panic]
    fn test_stage_mro_display_duplicate_outputs() {
        let stage_mro = StageMro {
            stage_name: "SUM_SQUARES".into(),
            adapter_name: "my_adapter".into(),
            stage_key: "sum_squares".into(),
            stage_in_out: InAndOut {
                inputs: vec![MroField::new("values", Array(Float))],
                outputs: vec![MroField::new("sum", Primary(Float))],
            },
            chunk_in_out: Some(InAndOut {
                inputs: Vec::new(),
                outputs: vec![MroField::new("sum", Primary(Int))],
            }),
            using_attrs: MroUsing {
                mem_gb: Some(1),
                threads: Some(2),
                ..Default::default()
            },
        };
        stage_mro.verify();
    }

    #[test]
    fn test_stage_mro_display_duplicate_outputs_same_type() {
        let stage_mro = StageMro {
            stage_name: "SUM_SQUARES".into(),
            adapter_name: "my_adapter".into(),
            stage_key: "sum_squares".into(),
            stage_in_out: InAndOut {
                inputs: vec![MroField::new("values", Array(Float))],
                outputs: vec![MroField::new("sum", Primary(Float))],
            },
            chunk_in_out: Some(InAndOut {
                inputs: Vec::new(),
                outputs: vec![MroField::new("sum", Primary(Float))],
            }),
            using_attrs: MroUsing {
                mem_gb: Some(1),
                threads: Some(2),
                ..Default::default()
            },
        };
        stage_mro.verify();
        let expected = indoc!(
            r#"
            stage SUM_SQUARES(
                in  float[] values,
                out float   sum,
                src comp    "my_adapter martian sum_squares",
            ) split (
            ) using (
                mem_gb  = 1,
                threads = 2,
            )
        "#
        );
        assert_eq!(stage_mro.to_string(), expected);
    }

    #[test]
    fn test_filetype_header_from_mro_field() {
        assert_eq!(
            FiletypeHeader::from(&MroField::new("foo", Array(Float))),
            FiletypeHeader(HashSet::new())
        );
        assert_eq!(
            FiletypeHeader::from(&MroField::new("foo", Array(FileType("txt".into())))),
            FiletypeHeader(vec!["txt".to_string()].into_iter().collect())
        );
        assert_eq!(
            FiletypeHeader::from(&MroField::new("foo", Primary(FileType("json".into())))),
            FiletypeHeader(vec!["json".to_string()].into_iter().collect())
        );
    }

    #[test]
    fn test_filetype_header_from_in_out() {
        let filetype = FiletypeHeader::from(&InAndOut {
            inputs: vec![
                MroField::new("summary", Primary(FileType("json".into()))),
                MroField::new("contigs", Primary(FileType("bam".into()))),
            ],
            outputs: vec![MroField::new("contigs", Primary(FileType("bam".into())))],
        });
        let expected = FiletypeHeader(
            vec!["json".to_string(), "bam".to_string()]
                .into_iter()
                .collect(),
        );
        assert_eq!(filetype, expected);
    }

    #[test]
    fn test_filetype_header_display() {
        assert_eq!(FiletypeHeader(HashSet::new()).to_string(), "");
        assert_eq!(
            FiletypeHeader(vec!["txt"].into_iter().map(|x| x.to_string()).collect()).to_string(),
            "\nfiletype txt;\n\n"
        );
        assert_eq!(
            FiletypeHeader(
                vec!["txt", "json", "bam"]
                    .into_iter()
                    .map(|x| x.to_string())
                    .collect()
            )
            .to_string(),
            indoc![
                "

            filetype bam;
            filetype json;
            filetype txt;
            
            "
            ]
        );
    }

    #[test]
    fn test_martian_primary_type_parse() {
        use MartianPrimaryType::*;
        let roundtrip_assert = |t: MartianPrimaryType| {
            assert_eq!(t, t.to_string().parse::<MartianPrimaryType>().unwrap());
        };
        roundtrip_assert(Int);
        roundtrip_assert(Float);
        roundtrip_assert(Bool);
        roundtrip_assert(Str);
        roundtrip_assert(Bool);
        roundtrip_assert(Map);
        roundtrip_assert(Path);
        assert!(FileType("foo".into())
            .to_string()
            .parse::<MartianPrimaryType>()
            .is_err())
    }

    #[test]
    fn test_martian_blanket_type_parse() {
        use MartianBlanketType::*;
        use MartianPrimaryType::*;
        let roundtrip_blanket_assert = |t: MartianPrimaryType| {
            let p = Primary(t.clone());
            assert_eq!(p, p.to_string().parse::<MartianBlanketType>().unwrap());
            let a = Array(t);
            assert_eq!(a, a.to_string().parse::<MartianBlanketType>().unwrap());
        };
        roundtrip_blanket_assert(Int);
        roundtrip_blanket_assert(Float);
        roundtrip_blanket_assert(Bool);
        roundtrip_blanket_assert(Str);
        roundtrip_blanket_assert(Bool);
        roundtrip_blanket_assert(Map);
        roundtrip_blanket_assert(Path);
        assert!(Primary(FileType("foo".into()))
            .to_string()
            .parse::<MartianBlanketType>()
            .is_err())
    }
}

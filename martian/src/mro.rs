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

use crate::{Error, MartianVoid};
use anyhow::format_err;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::collections::{HashMap, HashSet};
use std::fmt::Display;
use std::hash::Hash;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::string::ToString;

/// Keywords used in the martian language. Using these keywords as mro field names
/// is disallowed.
pub const MARTIAN_TOKENS: &[&str] = &[
    "in", "out", "stage", "volatile", "strict", "true", "split", "filetype", "src", "py", "comp",
    "retain", "mro", "using", "int", "float", "string", "map", "bool", "path",
];

/// Defines how an entity that denotes some part of the mro is displayed
pub trait MroDisplay: Display {
    fn mro_string(&self, field_width: Option<usize>) -> String {
        match field_width {
            Some(width) => {
                let min_width = self.min_width();
                assert!(
                    width >= min_width,
                    "Need a minimum width of {}. Found {}",
                    min_width,
                    width
                );
                self.mro_string_with_width(width)
            }
            None => self.mro_string_no_width(),
        }
    }
    fn min_width(&self) -> usize;
    fn mro_string_no_width(&self) -> String {
        self.to_string()
    }
    fn mro_string_with_width(&self, field_width: usize) -> String {
        format!("{value:<width$}", value = self, width = field_width)
    }
}

macro_rules! usize_field_len {
    () => {
        fn min_width(&self) -> usize {
            let s: &str = self.into();
            s.len()
        }
    };
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, Hash)]
pub struct StructDef {
    name: String,
    fields: Vec<MroField>,
}

impl StructDef {
    pub fn new(name: String, fields: Vec<MroField>) -> Self {
        StructDef { name, fields }
    }
}

impl MroDisplay for StructDef {
    fn min_width(&self) -> usize {
        0
    }
}

impl Display for StructDef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Determing the field width for the type field
        let ty_width = self
            .fields
            .iter()
            .map(MroDisplay::min_width)
            .max()
            .unwrap_or(0);

        writeln!(f, "struct {}(", self.name)?;

        for field in &self.fields {
            writeln!(
                f,
                "{blank:indent$}{field:<ty_width$},",
                blank = "",
                indent = TAB_WIDTH_FOR_MRO,
                field = field,
                ty_width = ty_width
            )?;
        }
        writeln!(f, ")")
    }
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
    File,
    FileType(String),
    Struct(StructDef),
}

impl Display for MartianPrimaryType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let value: &str = self.into();
        write!(
            f,
            "{value:<w$}",
            value = value,
            w = f.width().unwrap_or_default()
        )
    }
}

impl<'a> From<&'a MartianPrimaryType> for &'a str {
    fn from(t: &'a MartianPrimaryType) -> Self {
        match *t {
            MartianPrimaryType::Int => "int",
            MartianPrimaryType::Float => "float",
            MartianPrimaryType::Str => "string",
            MartianPrimaryType::Bool => "bool",
            MartianPrimaryType::Map => "map",
            MartianPrimaryType::Path => "path",
            MartianPrimaryType::File => "file",
            MartianPrimaryType::FileType(ref ext) => ext.as_str(),
            MartianPrimaryType::Struct(ref def) => def.name.as_str(),
        }
    }
}

impl MroDisplay for MartianPrimaryType {
    usize_field_len! {}
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
            "file" => MartianPrimaryType::File,
            _ => return Err(format_err!("Cannot find the martian primary type from {}. Supported entries are [int, float, string, bool, map, path, file]", s)),
        };
        Ok(prim_ty)
    }
}

/// Primary Data type in martian + Arrays (which are derived from primary types)
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, Hash)]
pub enum MartianBlanketType {
    Primary(MartianPrimaryType),
    Array(Box<MartianBlanketType>),
    TypedMap(Box<MartianBlanketType>),
}

impl MartianBlanketType {
    fn inner(&self) -> MartianPrimaryType {
        match self {
            MartianBlanketType::Primary(ref primary) => primary.clone(),
            MartianBlanketType::Array(ref blanket) => blanket.inner(),
            MartianBlanketType::TypedMap(ref blanket) => blanket.inner(),
        }
    }
}

impl MroDisplay for MartianBlanketType {
    fn min_width(&self) -> usize {
        match *self {
            MartianBlanketType::Primary(ref primary) => primary.min_width(),
            MartianBlanketType::Array(ref blanket) => blanket.min_width() + 2,
            MartianBlanketType::TypedMap(ref blanket) => {
                // map of maps not allowed in Martian
                // this is a little hacky, we allow TypedMap<map> to be passed around internally in Martian-rust
                // but we just print it as "map"
                match **blanket {
                    MartianBlanketType::TypedMap(_)
                    | MartianBlanketType::Primary(MartianPrimaryType::Map) => 3,
                    // map<T>
                    _ => blanket.min_width() + 5,
                }
            }
        }
    }
}

impl Display for MartianBlanketType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {
            MartianBlanketType::Primary(ref primary) => {
                write!(f, "{v:<w$}", v = primary, w = f.width().unwrap_or_default())
            }
            MartianBlanketType::Array(ref blanket) => {
                write!(
                    f,
                    "{v}{a:<w$}",
                    v = blanket,
                    a = "[]",
                    w = f
                        .width()
                        .unwrap_or_default()
                        .saturating_sub(blanket.min_width())
                )
            }
            MartianBlanketType::TypedMap(ref blanket) => {
                // map of maps not allowed in Martian
                // this is a little hacky, we allow TypedMap<map> to be passed around internally in Martian-rust
                // but we just print it as "map"
                match **blanket {
                    MartianBlanketType::TypedMap(_)
                    | MartianBlanketType::Primary(MartianPrimaryType::Map) => {
                        write!(f, "{v:<w$}", v = "map", w = f.width().unwrap_or_default())
                    }
                    _ => write!(
                        f,
                        "map<{v}{c:<w$}",
                        v = blanket,
                        c = ">",
                        w = f
                            .width()
                            .unwrap_or_default()
                            .saturating_sub(blanket.min_width() + 4)
                    ),
                }
            }
        }
    }
}

impl FromStr for MartianBlanketType {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.ends_with("[]") {
            // array
            let t = s.get(0..s.len() - 2).unwrap();
            Ok(MartianBlanketType::Array(Box::new(
                MartianBlanketType::from_str(t)?,
            )))
        } else if s.starts_with("map<") && s.ends_with('>') {
            // typed map
            let t = s.get(4..s.len() - 1).unwrap();
            Ok(MartianBlanketType::TypedMap(Box::new(
                MartianBlanketType::from_str(t)?,
            )))
        } else {
            Ok(MartianBlanketType::Primary(MartianPrimaryType::from_str(
                s,
            )?))
        }
    }
}

impl From<MartianPrimaryType> for MartianBlanketType {
    fn from(other: MartianPrimaryType) -> Self {
        MartianBlanketType::Primary(other)
    }
}

impl From<MartianPrimaryType> for Box<MartianBlanketType> {
    fn from(other: MartianPrimaryType) -> Self {
        Box::new(MartianBlanketType::Primary(other))
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
/// It is strongly recommended not to extend any types with this trait, instead
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

impl<T: AsMartianBlanketType> AsMartianBlanketType for Vec<T> {
    fn as_martian_blanket_type() -> MartianBlanketType {
        MartianBlanketType::Array(Box::new(T::as_martian_blanket_type()))
    }
}

impl<K: AsMartianPrimaryType, H> AsMartianBlanketType for HashSet<K, H> {
    fn as_martian_blanket_type() -> MartianBlanketType {
        MartianBlanketType::Array(Box::new(K::as_martian_blanket_type()))
    }
}

// ideally we'd allow for any HashMap to be turned into a typed Map when possible, or an untyped Map by default
// but a typed map can only be made for Rust HashMap with a key implementing Display + Eq + Hash
// and a value implementing MartianPrimaryType
// it is not possible to have multiple implementations ranked in priority without specialization, which is an unstable feature
// and it is impossible to check what traits are implemented for a HashMap's K,V at runtime.
// instead, the current solution is that any HashMap not meeting these trait bounds must manually specify the type
// using #[mro_type = "map"]
impl<K, V: AsMartianBlanketType, H> AsMartianBlanketType for HashMap<K, V, H> {
    fn as_martian_blanket_type() -> MartianBlanketType {
        MartianBlanketType::TypedMap(Box::new(V::as_martian_blanket_type()))
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
#[derive(Debug, Serialize, Clone, Deserialize, PartialEq, Eq, Hash)]
pub struct MroField {
    name: String,
    ty: MartianBlanketType,
    retain: bool,
}

impl Display for MroField {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{ty:<width$} {name}",
            ty = self.ty,
            width = f.width().unwrap_or_default(),
            name = self.name.as_str()
        )
    }
}

/// `field_width` will decide the length of the type column
impl MroDisplay for MroField {
    fn min_width(&self) -> usize {
        self.ty.min_width()
    }
}

impl MroField {
    /// Create a new `MroField` with the given name and type.
    /// The field has a default `retain = false`.
    pub fn new(name: impl ToString, ty: MartianBlanketType) -> Self {
        fn _new_field(name: String, ty: MartianBlanketType) -> MroField {
            let field = MroField {
                name,
                ty,
                retain: false,
            };
            field.verify(); // No use case to resultify this so far
            field
        }
        _new_field(name.to_string(), ty)
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
        for &token in MARTIAN_TOKENS {
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

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum Volatile {
    Strict,
    False,
}

impl Default for Volatile {
    fn default() -> Self {
        Volatile::False
    }
}

impl From<&Volatile> for &'static str {
    fn from(v: &Volatile) -> Self {
        match v {
            Volatile::Strict => "strict",
            Volatile::False => "false",
        }
    }
}

impl FromStr for Volatile {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "strict" => Ok(Volatile::Strict),
            "false" => Ok(Volatile::False),
            _ => Err(format!("Expected strict for volatile, Found {}", s)),
        }
    }
}

// Maybe just need display?
impl MroDisplay for Volatile {
    usize_field_len! {}
}

impl Display for Volatile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s: &str = self.into();
        s.fmt(f)
    }
}

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

        impl Display for MroUsing {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                let field_width = f.width().unwrap_or_else(|| self.min_width());
                // If every field is None, return empty String
                if !self.need_using() {
                    return Ok(());
                }
                $(
                    if let Some($property) = self.$property {
                        writeln!(
                            f,
                            "{blank:indent$}{key:<width$} = {value},",
                            blank="",
                            indent = TAB_WIDTH_FOR_MRO,
                            key=stringify!($property),
                            width=field_width,
                            value=$property
                        )?;
                    }
                )*
                Ok(())
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
        }
    };
}

mro_using! {mem_gb: i16, threads: i16, vmem_gb: i16, volatile: Volatile}

/// Input and outputs fields together
#[derive(Debug, Default)]
pub struct InAndOut {
    pub inputs: Vec<MroField>,
    pub outputs: Vec<MroField>,
}

impl InAndOut {
    fn iter_mro_fields(&self) -> impl Iterator<Item = &MroField> {
        self.inputs.iter().chain(self.outputs.iter())
    }
    fn retain_field_names(&self) -> impl Iterator<Item = &str> {
        self.outputs.iter().filter_map(|field| {
            if field.retain {
                Some(field.name.as_ref())
            } else {
                None
            }
        })
    }
}

impl Display for InAndOut {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let field_width = f.width().unwrap_or_default().max(self.min_width());
        for (key, fields) in [("in ", &self.inputs), ("out", &self.outputs)] {
            for field in fields {
                writeln!(
                    f,
                    "{key:>indent$} {value:<width$},",
                    indent = TAB_WIDTH_FOR_MRO + 3,
                    key = key,
                    value = field,
                    width = field_width
                )?;
            }
        }
        Ok(())
    }
}

impl MroDisplay for InAndOut {
    fn min_width(&self) -> usize {
        self.iter_mro_fields()
            .map(MroDisplay::min_width)
            .max()
            .unwrap_or_default()
    }
}

/// The list of filetypes we list at the top of the mro.
/// This struct is a simple wrapper around a HashSet of all file extensions.
#[derive(Debug, PartialEq, Eq, Default)]
pub struct FiletypeHeader(HashSet<String>);

impl From<&MroField> for FiletypeHeader {
    fn from(field: &MroField) -> FiletypeHeader {
        let mut result = FiletypeHeader(HashSet::new());
        result.add_mro_field(field);
        result
    }
}

impl From<&InAndOut> for FiletypeHeader {
    fn from(in_out: &InAndOut) -> FiletypeHeader {
        let mut result = FiletypeHeader(HashSet::new());
        for field in in_out.iter_mro_fields() {
            result.add_mro_field(field);
        }
        result
    }
}

impl From<&StageMro> for FiletypeHeader {
    fn from(stage_mro: &StageMro) -> FiletypeHeader {
        let mut result = FiletypeHeader(HashSet::new());
        result.add_stage(stage_mro);
        result
    }
}

impl FiletypeHeader {
    /// Find out all the filetypes in the stage and add the extensions
    /// to the internal hashset which stores all the extensions
    pub fn add_stage(&mut self, stage_mro: &StageMro) {
        for field in stage_mro.iter_mro_fields() {
            self.add_mro_field(field);
        }
    }
    pub fn add_mro_field(&mut self, mro_field: &MroField) {
        match mro_field.ty.inner() {
            MartianPrimaryType::FileType(ref ext) => {
                self.0.insert(ext.to_string());
            }
            MartianPrimaryType::Struct(ref def) => {
                for field in &def.fields {
                    self.add_mro_field(field);
                }
            }
            _ => {}
        }
    }
}

impl Display for FiletypeHeader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.0.is_empty() {
            return Ok(());
        }
        let mut extensions: Vec<_> = self.0.iter().collect();
        extensions.sort();
        for ext in extensions {
            writeln!(f, "filetype {};", ext)?;
        }
        writeln!(f)
    }
}

// Just need display here
impl MroDisplay for FiletypeHeader {
    fn min_width(&self) -> usize {
        // No configuration here
        0
    }
}

/// All the structs that need to be defined in an mro
#[derive(Debug, Default)]
pub struct StructHeader(BTreeMap<String, (StructDef, usize)>); // key = struct name, val = (struct def, insertion index)

impl From<&StageMro> for StructHeader {
    fn from(stage_mro: &StageMro) -> StructHeader {
        let mut result = StructHeader(BTreeMap::new());
        result.add_stage(stage_mro);
        result
    }
}

impl StructHeader {
    /// Find out all the structs in the stage and add it to the
    /// internal hashmap which stores all the structs
    pub fn add_stage(&mut self, stage_mro: &StageMro) {
        for field in stage_mro.iter_mro_fields() {
            self.add_mro_field(field);
        }
    }
    pub fn add_mro_field(&mut self, mro_field: &MroField) {
        if let MartianPrimaryType::Struct(ref def) = mro_field.ty.inner() {
            for field in &def.fields {
                self.add_mro_field(field);
            }
            if self.0.contains_key(&def.name) {
                assert_eq!(
                    &self.0[&def.name].0, def,
                    "struct {} has conflicting definitions.\nDefinition 1: {:?}\nDefinition 2: {:?}",
                    def.name, &self.0[&def.name], def
                );
            } else {
                let index = self.0.len();
                self.0.insert(def.name.clone(), (def.clone(), index));
            }
        }
    }
}

impl MroDisplay for StructHeader {
    fn min_width(&self) -> usize {
        0
    }
}

impl Display for StructHeader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let field_width = f.width().unwrap_or_else(|| self.min_width());

        let mut struct_defs: Vec<(StructDef, usize)> = self.0.values().cloned().collect();
        struct_defs.sort_by_key(|x| x.1);

        for struct_def in struct_defs.iter() {
            writeln!(
                f,
                "{def:<field_width$}",
                def = struct_def.0,
                field_width = field_width
            )?;
        }
        Ok(())
    }
}

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
        let struct_header = StructHeader::from(&stage_mro);
        format!("{}{}{}", filetype, struct_header, stage_mro)
    }
    fn stage_name() -> &'static str;
    fn stage_in_and_out() -> InAndOut;
    fn chunk_in_and_out() -> Option<InAndOut>;
    fn using_attributes() -> MroUsing;
}

/// All the data needed to create a stage definition mro.
#[derive(Debug)]
pub struct StageMro {
    stage_name: &'static str, // e.g CORRECT_BARCODES in `stage CORRECT_BARCODES(..)`
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
}

impl Display for StageMro {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Determing the field width for the type field
        let ty_width = std::cmp::max(
            self.stage_in_out.min_width(),
            self.chunk_in_out
                .as_ref()
                .map(MroDisplay::min_width)
                .unwrap_or_default(),
        );
        writeln!(f, "stage {}(", self.stage_name)?;
        write!(
            f,
            "{params:<ty_width$}",
            params = self.stage_in_out,
            ty_width = ty_width
        )?;
        writeln!(
            f,
            r#"{blank:indent$}src {comp:<ty_width$} "{adapter} martian {stage_key}","#,
            blank = "",
            indent = TAB_WIDTH_FOR_MRO,
            comp = "comp",
            ty_width = ty_width,
            adapter = self.adapter_name,
            stage_key = self.stage_key,
        )?;

        if let Some(ref chunk_in_out) = self.minified_chunk_in_outs() {
            writeln!(f, ") split (")?;
            write!(
                f,
                "{params:<ty_width$}",
                params = chunk_in_out,
                ty_width = ty_width
            )?;
        }

        if self.using_attrs.need_using() {
            writeln!(f, ") using (")?;
            write!(f, "{}", self.using_attrs)?;
        }
        let mut retain_names = self.stage_in_out.retain_field_names();
        if let Some(first) = retain_names.next() {
            writeln!(f, ") retain (")?;
            writeln!(
                f,
                "{blank:indent$}{line},",
                blank = "",
                indent = TAB_WIDTH_FOR_MRO,
                line = first
            )?;
            for line in retain_names {
                writeln!(
                    f,
                    "{blank:indent$}{line},",
                    blank = "",
                    indent = TAB_WIDTH_FOR_MRO,
                    line = line
                )?;
            }
        }
        writeln!(f, ")")
    }
}

impl StageMro {
    fn iter_mro_fields(&self) -> impl Iterator<Item = &MroField> {
        self.stage_in_out
            .iter_mro_fields()
            .chain(self.chunk_in_out.iter().flat_map(InAndOut::iter_mro_fields))
    }
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
        let chunk_in_out = if let Some(ref chunk_in_out) = self.chunk_in_out {
            chunk_in_out
        } else {
            return;
        };
        // Do not allow the same field name in stage and chunk inputs
        // O(mn) is good enough
        for f_chunk in chunk_in_out.inputs.iter() {
            for f_stage in self.stage_in_out.inputs.iter() {
                assert!(
                    f_chunk.name != f_stage.name,
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
    use pretty_assertions::assert_eq;
    use MartianBlanketType::{Array, Primary};
    use MartianPrimaryType::{Bool, FileType, Float, Int, Path, Str, Struct};

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
        assert_eq!(Array(Int.into()).mro_string(Some(7)), "int[]  ");
        assert_eq!(
            Array(FileType("txt".into()).into()).mro_string_with_width(5),
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
        assert_eq!("false".parse::<Volatile>(), Ok(Volatile::False));
        assert!("foo".parse::<Volatile>().is_err());
    }

    #[test]
    fn test_volatile_display() {
        let vol = Volatile::Strict;
        assert_eq!(vol.mro_string(None), "strict");
        assert_eq!(vol.mro_string_no_width(), "strict");
        assert_eq!(vol.min_width(), 6);
        assert_eq!(vol.mro_string(Some(10)), "strict    ");
        let vol = Volatile::False;
        assert_eq!(vol.mro_string(None), "false");
    }

    #[test]
    fn test_mro_using_display() {
        assert_eq!(
            MroUsing {
                mem_gb: Some(1),
                ..Default::default()
            }
            .to_string(),
            "    mem_gb = 1,\n"
        );

        assert_eq!(
            MroUsing {
                mem_gb: Some(1),
                vmem_gb: Some(4),
                volatile: Some(Volatile::Strict),
                ..Default::default()
            }
            .mro_string_no_width(),
            "    mem_gb   = 1,
    vmem_gb  = 4,
    volatile = strict,
",
        );

        assert_eq!(
            MroUsing {
                threads: Some(2),
                ..Default::default()
            }
            .mro_string_with_width(10),
            "    threads    = 2,\n"
        );
    }

    #[test]
    fn test_mro_using_need_using() {
        assert!(!MroUsing::default().need_using());
        assert!(MroUsing {
            mem_gb: Some(1),
            ..Default::default()
        }
        .need_using());
        assert!(MroUsing {
            mem_gb: Some(1),
            threads: Some(3),
            ..Default::default()
        }
        .need_using());
    }

    #[test]
    fn test_in_and_out_display() {
        let in_out = InAndOut {
            inputs: vec![
                MroField::new("unsorted", Array(Float.into())),
                MroField::new("reverse", Primary(Bool)),
            ],
            outputs: vec![
                MroField::new("sorted", Array(Float.into())),
                MroField::new("sum", Primary(Float)),
            ],
        };
        let expected = "    in  float[] unsorted,
    in  bool    reverse,
    out float[] sorted,
    out float   sum,
";
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
            stage_name: "SUM_SQUARES",
            adapter_name: "my_adapter".into(),
            stage_key: "sum_squares".into(),
            stage_in_out: InAndOut {
                inputs: vec![MroField::new("values", Array(Float.into()))],
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
            stage_name: "SUM_SQUARES",
            adapter_name: "my_adapter".into(),
            stage_key: "sum_squares".into(),
            stage_in_out: InAndOut {
                inputs: vec![MroField::new("values", Array(Float.into()))],
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
            stage_name: "SUM_SQUARES",
            adapter_name: "my_adapter".into(),
            stage_key: "sum_squares".into(),
            stage_in_out: InAndOut {
                inputs: vec![MroField::new("values", Array(Float.into()))],
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
            stage_name: "SUM_SQUARES",
            adapter_name: "my_adapter".into(),
            stage_key: "sum_squares".into(),
            stage_in_out: InAndOut {
                inputs: vec![MroField::new("values", Array(Float.into()))],
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
            stage_name: "SUM_SQUARES",
            adapter_name: "my_adapter".into(),
            stage_key: "sum_squares".into(),
            stage_in_out: InAndOut {
                inputs: vec![MroField::new("values", Array(Float.into()))],
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
            stage_name: "SUM_SQUARES",
            adapter_name: "my_adapter".into(),
            stage_key: "sum_squares".into(),
            stage_in_out: InAndOut {
                inputs: vec![MroField::new("values", Array(Float.into()))],
                outputs: vec![MroField::new("sum", Primary(Float))],
            },
            chunk_in_out: Some(InAndOut {
                inputs: vec![MroField::new("values", Array(Float.into()))],
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
            stage_name: "SUM_SQUARES",
            adapter_name: "my_adapter".into(),
            stage_key: "sum_squares".into(),
            stage_in_out: InAndOut {
                inputs: vec![MroField::new("values", Array(Float.into()))],
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
            stage_name: "SUM_SQUARES",
            adapter_name: "my_adapter".into(),
            stage_key: "sum_squares".into(),
            stage_in_out: InAndOut {
                inputs: vec![MroField::new("values", Array(Float.into()))],
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
    fn test_stage_mro_type_width_1() {
        // Check field alignment agrees with `mro format` when chunk arg
        // types are narrower than stage arg types.
        let stage_mro = StageMro {
            stage_name: "SUM_SQUARES1",
            adapter_name: "my_adapter".into(),
            stage_key: "sum_squares".into(),
            stage_in_out: InAndOut {
                inputs: vec![MroField::new("values", Array(Float.into()))],
                outputs: vec![MroField::new("sum", Primary(Float))],
            },
            chunk_in_out: Some(InAndOut {
                inputs: Vec::new(),
                outputs: vec![MroField::new("value", Primary(Str))],
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
            stage SUM_SQUARES1(
                in  float[] values,
                out float   sum,
                src comp    "my_adapter martian sum_squares",
            ) split (
                out string  value,
            ) using (
                mem_gb  = 1,
                threads = 2,
            )
        "#
        );
        assert_eq!(stage_mro.to_string(), expected);
    }

    #[test]
    fn test_stage_mro_type_width_2() {
        // Check field alignment agrees with `mro format` when stage arg
        // types are narrower than stage src type.
        let stage_mro = StageMro {
            stage_name: "SUM_SQUARES2",
            adapter_name: "my_adapter".into(),
            stage_key: "sum_squares".into(),
            stage_in_out: InAndOut {
                inputs: vec![MroField::new("sum", Primary(Int))],
                outputs: Vec::new(),
            },
            chunk_in_out: Some(InAndOut {
                inputs: Vec::new(),
                outputs: Vec::new(),
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
            stage SUM_SQUARES2(
                in  int sum,
                src comp "my_adapter martian sum_squares",
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
    fn test_stage_mro_type_width_3() {
        // Check field alignment agrees with `mro format` when chunk arg
        // types are wider than stage arg types.
        let stage_mro = StageMro {
            stage_name: "SUM_SQUARES3",
            adapter_name: "my_adapter".into(),
            stage_key: "sum_squares".into(),
            stage_in_out: InAndOut {
                inputs: vec![MroField::new("value", Primary(Float))],
                outputs: vec![MroField::new("sum", Primary(Float))],
            },
            chunk_in_out: Some(InAndOut {
                inputs: Vec::new(),
                outputs: vec![MroField::new("value_s", Primary(Str))],
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
            stage SUM_SQUARES3(
                in  float  value,
                out float  sum,
                src comp   "my_adapter martian sum_squares",
            ) split (
                out string value_s,
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
            FiletypeHeader::from(&MroField::new("foo", Array(Float.into()))),
            FiletypeHeader(HashSet::new())
        );
        assert_eq!(
            FiletypeHeader::from(&MroField::new("foo", Array(FileType("txt".into()).into()))),
            FiletypeHeader(vec!["txt".to_string()].into_iter().collect())
        );
        assert_eq!(
            FiletypeHeader::from(&MroField::new("foo", Primary(FileType("json".into())))),
            FiletypeHeader(vec!["json".to_string()].into_iter().collect())
        );
    }

    #[test]
    fn test_filetype_header_from_struct() {
        assert_eq!(
            FiletypeHeader::from(&MroField::new(
                "foo",
                Primary(Struct(StructDef {
                    name: "MexFiles".to_string(),
                    fields: vec![MroField::new("foo", Array(FileType("txt".into()).into()))],
                }))
            )),
            FiletypeHeader(vec!["txt".to_string()].into_iter().collect())
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
            FiletypeHeader(
                vec!["txt"]
                    .into_iter()
                    .map(std::string::ToString::to_string)
                    .collect()
            )
            .to_string(),
            "filetype txt;\n\n"
        );
        assert_eq!(
            FiletypeHeader(
                vec!["txt", "json", "bam"]
                    .into_iter()
                    .map(std::string::ToString::to_string)
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
        use MartianPrimaryType::{Bool, File, FileType, Float, Int, Map, Path, Str};
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
        roundtrip_assert(File);
        assert!(FileType("foo".into())
            .to_string()
            .parse::<MartianPrimaryType>()
            .is_err())
    }

    #[test]
    fn test_martian_blanket_type_parse() {
        use MartianBlanketType::{Array, Primary};
        use MartianPrimaryType::{Bool, File, FileType, Float, Int, Map, Path, Str};
        let roundtrip_blanket_assert = |t: MartianPrimaryType| {
            let p = Primary(t.clone());
            assert_eq!(p, p.to_string().parse::<MartianBlanketType>().unwrap());
            let a = Array(t.into());
            assert_eq!(a, a.to_string().parse::<MartianBlanketType>().unwrap());
        };
        roundtrip_blanket_assert(Int);
        roundtrip_blanket_assert(Float);
        roundtrip_blanket_assert(Bool);
        roundtrip_blanket_assert(Str);
        roundtrip_blanket_assert(Bool);
        roundtrip_blanket_assert(Map);
        roundtrip_blanket_assert(Path);
        roundtrip_blanket_assert(File);
        assert!(Primary(FileType("foo".into()))
            .to_string()
            .parse::<MartianBlanketType>()
            .is_err())
    }

    #[test]
    fn test_in_and_out_display_with_struct() {
        let in_out = InAndOut {
            inputs: vec![MroField::new("raw_matrix", Primary(FileType("h5".into())))],
            outputs: vec![MroField::new(
                "mex_files",
                Primary(Struct(StructDef {
                    name: "MexFiles".to_string(),
                    fields: vec![],
                })),
            )],
        };
        let expected = "    in  h5       raw_matrix,
    out MexFiles mex_files,
";
        assert_eq!(in_out.mro_string(None), expected);
        assert_eq!(in_out.to_string(), expected);
    }

    #[test]
    fn test_struct_display() {
        let struct_def = StructDef {
            name: "MexFiles".to_string(),
            fields: vec![
                MroField::new("matrix", Primary(FileType("mtx".into()))),
                MroField::new("barcodes", Primary(Path)),
                MroField::new("features", Primary(Path)),
            ],
        };

        let expected = indoc!(
            r#"
            struct MexFiles(
                mtx  matrix,
                path barcodes,
                path features,
            )
        "#
        );
        assert_eq!(struct_def.to_string(), expected);
    }

    #[test]
    fn test_struct_header_display() {
        let struct_def = StructDef {
            name: "MexFiles".to_string(),
            fields: vec![
                MroField::new("matrix", Primary(FileType("mtx".into()))),
                MroField::new("barcodes", Primary(Path)),
                MroField::new("features", Primary(Path)),
            ],
        };
        let mut map = BTreeMap::new();
        map.insert(struct_def.name.clone(), (struct_def, 0));
        let header = StructHeader(map);

        let expected = indoc!(
            r#"
            struct MexFiles(
                mtx  matrix,
                path barcodes,
                path features,
            )

        "#
        );
        assert_eq!(header.to_string(), expected);
    }

    #[test]
    fn test_struct_header_recursive_display() {
        let sample_def = StructDef {
            name: "SampleDef".into(),
            fields: vec![MroField::new("read_path", Primary(Path))],
        };
        let chemistry_def = StructDef {
            name: "ChemistryDef".into(),
            fields: vec![
                MroField::new("name", Primary(Str)),
                MroField::new("barcode_read", Primary(Str)),
                MroField::new("barcode_length", Primary(Int)),
            ],
        };
        let rna_chunk = StructDef {
            name: "RnaChunk".to_string(),
            fields: vec![
                MroField::new("chemistry_def", Primary(Struct(chemistry_def.clone()))),
                MroField::new("chunk_id", Primary(Int)),
                MroField::new("r1", Primary(FileType("fastq.gz".into()))),
            ],
        };

        let stage_mro = StageMro {
            stage_name: "SETUP_CHUNKS",
            adapter_name: "my_adapter".into(),
            stage_key: "setup_chunks".into(),
            stage_in_out: InAndOut {
                inputs: vec![
                    MroField::new("sample_defs", Array(Struct(sample_def).into())),
                    MroField::new(
                        "custom_chemistry_def",
                        Primary(Struct(chemistry_def.clone())),
                    ),
                ],
                outputs: vec![
                    MroField::new("read_chunks", Array(Struct(rna_chunk).into())),
                    MroField::new("chemistry_def", Primary(Struct(chemistry_def))),
                ],
            },
            chunk_in_out: None,
            using_attrs: MroUsing::default(),
        };
        stage_mro.verify();

        assert_eq!(
            FiletypeHeader::from(&stage_mro),
            FiletypeHeader(vec!["fastq.gz".into()].into_iter().collect())
        );

        let expected = indoc!(
            r#"
            struct SampleDef(
                path read_path,
            )

            struct ChemistryDef(
                string name,
                string barcode_read,
                int    barcode_length,
            )

            struct RnaChunk(
                ChemistryDef chemistry_def,
                int          chunk_id,
                fastq.gz     r1,
            )

        "#
        );
        assert_eq!(StructHeader::from(&stage_mro).to_string(), expected);
    }

    #[test]
    fn test_vec_option() {
        assert_eq!(
            Vec::<Option<u32>>::as_martian_blanket_type(),
            MartianBlanketType::Array(MartianPrimaryType::Int.into())
        );
    }
}

use martian::martian_filetype;
use martian::{
    AsMartianBlanketType, MartianBlanketType, MartianFileType, MartianPrimaryType, MartianStruct,
    MroField,
};
use martian_derive::MartianStruct;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use MartianBlanketType::*;
use MartianPrimaryType::*;

#[derive(MartianStruct)]
pub struct SimpleVec {
    #[allow(dead_code)]
    values: Vec<f64>,
}

#[test]
fn test_simple_vec() {
    let expected = vec![MroField::new("values", Array(Float))];
    assert_eq!(expected, SimpleVec::mro_fields())
}

#[derive(MartianStruct)]
pub struct Generic<T: AsMartianBlanketType> {
    #[allow(dead_code)]
    param: T,
}

martian_filetype! {TxtFile, "txt"}

#[test]
fn test_generic() {
    assert_eq!(
        Generic::<i32>::mro_fields(),
        vec![MroField::new("param", Primary(Int))]
    );
    assert_eq!(
        Generic::<f64>::mro_fields(),
        vec![MroField::new("param", Primary(Float))]
    );
    assert_eq!(
        Generic::<bool>::mro_fields(),
        vec![MroField::new("param", Primary(Bool))]
    );
    assert_eq!(
        Generic::<TxtFile>::mro_fields(),
        vec![MroField::new("param", Primary(FileType("txt".into())))]
    );
    assert_eq!(
        Generic::<Vec<bool>>::mro_fields(),
        vec![MroField::new("param", Array(Bool))]
    );
    assert_eq!(
        Generic::<Vec<String>>::mro_fields(),
        vec![MroField::new("param", Array(Str))]
    );
    assert_eq!(
        Generic::<HashMap<String, f32>>::mro_fields(),
        vec![MroField::new("param", Primary(Map))]
    );
}

#[allow(dead_code)]
#[derive(MartianStruct)]
pub struct GenericTwo<T: AsMartianBlanketType, U: AsMartianBlanketType> {
    foo: T,
    bar: U,
    far: String,
}

#[test]
fn test_generic_two() {
    assert_eq!(
        GenericTwo::<i32, PathBuf>::mro_fields(),
        vec![
            MroField::new("foo", Primary(Int)),
            MroField::new("bar", Primary(Path)),
            MroField::new("far", Primary(Str)),
        ]
    );
}

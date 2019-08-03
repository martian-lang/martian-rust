use martian::{
    AsMartianBlanketType, MartianBlanketType, MartianPrimaryType, MartianStruct, MroField,
};
use martian_derive::{martian_filetype, MartianStruct};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use MartianBlanketType::*;
use MartianPrimaryType::*;

#[test]
fn test_simple_vec() {
    #[derive(MartianStruct)]
    struct SimpleVec {
        #[allow(dead_code)]
        values: Vec<f64>,
    }
    let expected = vec![MroField::new("values", Array(Float))];
    assert_eq!(expected, SimpleVec::mro_fields())
}

martian_filetype! {TxtFile, "txt"}
#[test]
fn test_generic() {
    #[derive(MartianStruct)]
    struct Generic<T: AsMartianBlanketType> {
        #[allow(dead_code)]
        param: T,
    }

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

#[test]
fn test_generic_two() {
    #[allow(dead_code)]
    #[derive(MartianStruct)]
    struct GenericTwo<T: AsMartianBlanketType, U: AsMartianBlanketType> {
        foo: T,
        bar: U,
        far: String,
    }
    assert_eq!(
        GenericTwo::<i32, PathBuf>::mro_fields(),
        vec![
            MroField::new("foo", Primary(Int)),
            MroField::new("bar", Primary(Path)),
            MroField::new("far", Primary(Str)),
        ]
    );
}

#[test]
fn test_retain() {
    #[derive(MartianStruct)]
    struct SimpleVec {
        #[allow(dead_code)]
        #[mro_retain]
        values: Vec<f64>,
    }
    let expected = vec![MroField::retained("values", Array(Float))];
    assert_eq!(expected, SimpleVec::mro_fields())
}

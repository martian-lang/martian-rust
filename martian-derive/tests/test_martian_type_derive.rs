use martian::AsMartianBlanketType;
use martian::MartianBlanketType::*;
use martian::MartianPrimaryType::*;
use martian_derive::MartianType;

#[test]
fn test_named_struct() {
    #[allow(dead_code)]
    #[derive(MartianType)]
    struct Foo {
        f1: u32,
        f2: String,
    }
    assert_eq!(Foo::as_martian_blanket_type(), Primary(Map));
}

#[test]
fn test_named_struct_generic() {
    #[allow(dead_code)]
    #[derive(MartianType)]
    struct Foo<T, U> {
        f1: T,
        f2: U,
    }
    assert_eq!(Foo::<(), ()>::as_martian_blanket_type(), Primary(Map));
}

#[test]
fn test_unit_only_enum() {
    #[allow(dead_code)]
    #[derive(MartianType)]
    enum Chemistry {
        SCvdj,
        SC5Ppe,
        SC3Pv3,
    }
    assert_eq!(Chemistry::as_martian_blanket_type(), Primary(Str));
}

#[test]
fn test_named_only_enum() {
    #[allow(dead_code)]
    #[derive(MartianType)]
    enum ReadData {
        PairedEnd { read1: Vec<u8>, read2: Vec<u8> },
        SingleEnd { read1: Vec<u8> },
    }
    assert_eq!(ReadData::as_martian_blanket_type(), Primary(Map));
}

#[test]
fn test_unnamed_only_enum() {
    #[allow(dead_code)]
    #[derive(MartianType)]
    enum AlignmentParams<T> {
        Hamming(i16, i16),
        Edit(i16, i16, i16, i16),
        Generic(T),
    }
    assert_eq!(
        AlignmentParams::<()>::as_martian_blanket_type(),
        Primary(Map)
    );
}

#[test]
fn test_named_and_unnamed_enum() {
    #[allow(dead_code)]
    #[derive(MartianType)]
    enum AlignmentParams<T> {
        Hamming {
            match_score: i16,
            mismatch_score: i16,
        },
        Generic(T),
    }
    assert_eq!(
        AlignmentParams::<()>::as_martian_blanket_type(),
        Primary(Map)
    );
}

// TODO
// - Make this a proc macro
// - Disallow empty finetype extensions
#[macro_export]
macro_rules! martian_filetype {
    ($struct_name: ident, $extension:expr) => {
        #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
        pub struct $struct_name(std::path::PathBuf);
        impl MartianFileType for $struct_name {
            fn extension() -> &'static str {
                $extension
            }
            fn new(
                file_path: impl AsRef<std::path::Path>,
                file_name: impl AsRef<std::path::Path>,
            ) -> Self {
                let mut path = std::path::PathBuf::from(file_path.as_ref());
                path.push(file_name);
                path.set_extension(Self::extension());
                $struct_name(path)
            }
        }
        impl AsRef<std::path::Path> for $struct_name {
            fn as_ref(&self) -> &std::path::Path {
                &self.0
            }
        }
        impl<T> From<T> for $struct_name
        where
            std::path::PathBuf: From<T>,
        {
            fn from(source: T) -> Self {
                $struct_name(std::path::PathBuf::from(source))
            }
        }
    };
}

#[macro_export]
macro_rules! martian_stages {
    ( $( $x:path ),* ) => {
        {
            let mut stage_registry: ::std::collections::HashMap<String, Box<::martian::RawMartianStage>> = ::std::collections::HashMap::default();
            $(
                stage_registry.insert(::martian::utils::to_exec_name(stringify!($x)), Box::new($x));
            )*
            stage_registry
        }
    };
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use types::MartianFileType;
    #[test]
    fn test_martian_filetype_simple() {
        martian_filetype!(FastqFile, "fastq");
        assert_eq!(<FastqFile as MartianFileType>::extension(), "fastq");
        let some_file: FastqFile = MartianFileType::new("/some/path", "file");
        assert_eq!(some_file, FastqFile(PathBuf::from("/some/path/file.fastq")));

        let some_file = FastqFile::from("/some/path/file.fastq");
        assert_eq!(some_file, FastqFile(PathBuf::from("/some/path/file.fastq")));
    }

    #[test]
    fn test_martian_filetype_double() {
        martian_filetype!(FaiFile, "fasta.fai");
        assert_eq!(<FaiFile as MartianFileType>::extension(), "fasta.fai");

        let some_file: FaiFile = MartianFileType::new("/some/path", "file");
        assert_eq!(
            some_file,
            FaiFile(PathBuf::from("/some/path/file.fasta.fai"))
        );

        let some_file = FaiFile::from("/some/path/file.fasta.fai");
        assert_eq!(
            some_file,
            FaiFile(PathBuf::from("/some/path/file.fasta.fai"))
        );
    }
}

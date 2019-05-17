
#[macro_export]
macro_rules! martian_filetype {
    ($struct_name: ident, $extension:expr) => (
        #[derive(Debug, Clone, Serialize, Deserialize)]
        pub struct $struct_name(std::path::PathBuf);
        impl MartianFileType for $struct_name {
            fn extension() -> &'static str {
                $extension
            }
            fn new(file_path: impl AsRef<std::path::Path>, file_name: impl AsRef<std::path::Path>) -> Self {
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
    )
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
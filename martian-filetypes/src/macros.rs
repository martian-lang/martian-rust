#[doc(hidden)]
#[macro_export]
macro_rules! martian_filetype_inner {
    ($(#[$attr:meta])* pub struct $name:ident, $extension:expr) => (
        $(#[$attr])*
        #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
        // The following attribute ensures that the struct will serialize into a
        // String like a PathBuf would.
        #[serde(transparent)]
        pub struct $name<F>
        where
            F: ::martian::MartianFileType,
        {
            // Skip [de]serializing the inner
            #[serde(skip)]
            inner: ::std::marker::PhantomData<F>,
            path: ::std::path::PathBuf,
        }

        impl<F> ::martian::MartianFileType for $name<F>
        where
            F: ::martian::MartianFileType,
        {
            fn extension() -> String {
                $crate::maybe_add_format(F::extension(), $extension)
            }

            fn new(file_path: impl AsRef<std::path::Path>, file_name: impl AsRef<std::path::Path>) -> Self {
                let path = ::martian::utils::make_path(file_path.as_ref(), file_name.as_ref(), Self::extension());
                $name {
                    inner: ::std::marker::PhantomData,
                    path,
                }
            }
        }
        impl<F> AsRef<std::path::Path> for $name<F>
        where
            F: ::martian::MartianFileType
        {
            fn as_ref(&self) -> &std::path::Path {
                self.path.as_ref()
            }
        }

        impl<F, P> From<P> for $name<F>
        where
            ::std::path::PathBuf: From<P>,
            F: ::martian::MartianFileType,
        {
            fn from(source: P) -> Self {
                let path_buf = ::std::path::PathBuf::from(source);
                Self::from_path(path_buf.as_path())
            }
        }
    )
}

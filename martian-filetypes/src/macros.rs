/// Create a type that wraps another implementation of MartianFileType.
/// Internally stores path data, while delegating reading and writing to the
/// implementation of the underlying type.
/// This makes it easy to create non-type-specific wrappers that handle things
/// like different compression formats.
#[doc(hidden)]
#[macro_export]
macro_rules! martian_filetype_decorator {
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
            /// Coerces this MartianFileType to a Path slice.
            fn as_ref(&self) -> &std::path::Path {
                &self.path
            }
        }

        impl<F> std::ops::Deref for $name<F>
        where
            F: ::martian::MartianFileType
        {
            type Target = ::std::path::Path;
            /// Dereferences this MartianFileType to a Path slice.
            fn deref(&self) -> &::std::path::Path {
                &self.path
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

/// Create a type that wraps another implementation of MartianFileType.
/// Adds an explicit type annotation that can be used to associate a datatype
/// with the wrapped file format.  This is sufficient for handling serialization
/// formats with no additional type parameters.
#[doc(hidden)]
#[macro_export]
macro_rules! martian_filetype_typed_decorator {
    ($(#[$attr:meta])* pub struct $name:ident, $extension:expr) => (
        $(#[$attr])*
        #[derive(Serialize, Deserialize, PartialEq, Eq)]
        // The following attribute ensures that the struct will serialize into a
        // String like a PathBuf would.
        #[serde(transparent)]
        pub struct $name<F, T>
        where
            F: ::martian::MartianFileType,
        {
            // Skip [de]serializing the inner
            #[serde(skip)]
            inner: ::std::marker::PhantomData<(T, F)>,
            path: ::std::path::PathBuf,
        }

        impl<F, T> Clone for $name<F, T>
        where
            F: MartianFileType,
        {
            fn clone(&self) -> Self {
                Self {
                    path: self.path.clone(),
                    inner: Default::default(),
                }
            }
        }

        impl<F, T> Debug for $name<F, T>
        where
            F: MartianFileType,
        {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.debug_struct(stringify!($name))
                    .field("path", &self.path)
                    .finish()
            }
        }

        impl<F, T> ::martian::MartianFileType for $name<F, T>
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

        impl<F, T> AsRef<std::path::Path> for $name<F, T>
        where
            F: ::martian::MartianFileType
        {
            /// Coerces this MartianFileType to a Path slice.
            fn as_ref(&self) -> &std::path::Path {
                &self.path
            }
        }

        impl<F, T> std::ops::Deref for $name<F, T>
        where
            F: ::martian::MartianFileType
        {
            type Target = ::std::path::Path;
            /// Dereferences this MartianFileType to a Path slice.
            fn deref(&self) -> &::std::path::Path {
                &self.path
            }
        }

        impl<F, T, P> From<P> for $name<F, T>
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

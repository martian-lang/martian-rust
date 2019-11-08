#[doc(hidden)]
#[macro_export]
macro_rules! martian_filetype_inner {
    ($(#[$attr:meta])* pub struct $name:ident, $extension:expr) => (
    	$(#[$attr])*
    	#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
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
                if F::extension().ends_with($extension) {
                	F::extension()
                } else {
                	format!("{}.{}", F::extension(), $extension)
                }
            }

            fn new(file_path: impl AsRef<std::path::Path>, file_name: impl AsRef<std::path::Path>) -> Self {
                let mut path = std::path::PathBuf::from(file_path.as_ref());
                path.push(file_name);
                let path = ::martian::utils::set_extension(path, Self::extension());
                $name {
		            inner: PhantomData,
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
		        let file_name = path_buf.file_name().unwrap();
		        match path_buf.parent() {
		            Some(path) => ::martian::MartianFileType::new(path, file_name),
		            None => ::martian::MartianFileType::new("", file_name),
		        }
		    }
		}

		impl<F> ::martian::AsMartianPrimaryType for $name<F>
		where
		    F: ::martian::MartianFileType,
		{
		    fn as_martian_primary_type() -> ::martian::MartianPrimaryType {
		        ::martian::MartianPrimaryType::FileType(<Self as ::martian::MartianFileType>::extension())
		    }
		}
    )
}

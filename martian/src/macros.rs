/// Create a stage and mro registry from a list of stage struct inputs.
///
/// See the `main.rs` file in any of the [examples here](https://github.com/martian-lang/martian-rust/tree/master/martian-lab/examples)
/// to see where this macro is used.
#[macro_export]
macro_rules! martian_stages {
    ( $( $x:path ),* ) => {
        {
            let mut stage_registry: ::std::collections::HashMap<String, Box<::martian::RawMartianStage>> = ::std::collections::HashMap::default();
            $(
                stage_registry.insert(::martian::utils::to_stage_key(stringify!($x)), Box::new($x));
            )*
            let mut mro_registry = vec![
                $(<$x as ::martian::MroMaker>::stage_mro(
                    ::martian::utils::current_executable(),
                    ::martian::utils::to_stage_key(stringify!($x)),
                )),*
            ];
            (stage_registry, mro_registry)
        }
    };
    ( $( $x: path, )*) => ( martian_stages![$($x),*]);
}

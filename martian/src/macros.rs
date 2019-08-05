#[macro_export]
macro_rules! martian_stages {
    ( $( $x:path ),* ) => {
        {
            let mut stage_registry: ::std::collections::HashMap<String, Box<::martian::RawMartianStage>> = ::std::collections::HashMap::default();
            $(
                stage_registry.insert(::martian::utils::to_exec_name(stringify!($x)), Box::new($x));
            )*
            let mut mro_registry = vec![
            	$(<$x as ::martian::MroMaker>::stage_mro(
            		::martian::utils::current_executable(),
            		::martian::utils::to_exec_name(stringify!($x)),
            	)),*
            ];
            (stage_registry, mro_registry)
        }
    };
    ( $( $x: path, )*) => ( martian_stages![$($x),*]);
}

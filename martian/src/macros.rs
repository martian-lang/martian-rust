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

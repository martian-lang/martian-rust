use docopt::Docopt;
use serde::Deserialize;

mod metadata;
mod template;
use metadata::Metadata;

const VERSION: Option<&'static str> = option_env!("CARGO_PKG_VERSION");
const USAGE: &str = "
Generate boiler plate for a martian stage code
Usage:
    cargo martian stage <stagename> [--pkg=<name>] [--main]
    cargo martian adapter <adaptername>
    cargo martian --version
    cargo martian --help
Options:
    --help     Show this screen.
    --version     Show version.
    --pkg=<name>  Which package to target. Needed if the crate has >1 packages.
    stage         Create a new stage template code within the cargo crate (assumes certain dependencies).
    <stagename>   Stage name for the template.
    --main        (Optional) Create a stage template with just main (no split/join)
    adapter       Create a new martian rust adapter executable.
    <adaptername> Name of the adapter executable
";

#[derive(Debug, Deserialize)]
struct Args {
    flag_pkg: Option<String>,
    flag_version: bool,
    cmd_martian: bool,
    cmd_stage: bool,
    cmd_adapter: bool,
    arg_stagename: Option<String>,
    arg_adaptername: Option<String>,
    flag_main: bool,
}

pub fn main() {
    let args: Args = Docopt::new(USAGE)
        .and_then(|d| d.deserialize())
        .unwrap_or_else(|e| e.exit());

    if args.flag_version {
        println!("{}", VERSION.unwrap_or("Unknown"));
        return;
    }

    if args.cmd_stage {
        let stage_name = args.arg_stagename.unwrap();
        let md = Metadata::read(&args.flag_pkg);
        template::new_stage(
            stage_name,
            &md.workspace_root,
            args.flag_pkg,
            args.flag_main,
        );
        return;
    }

    if args.cmd_adapter {
        let adapter_name = args.arg_adaptername.unwrap();
        template::new_adapter(adapter_name);
    }
}

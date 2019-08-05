use serde::Deserialize;

use martian::prelude::*;

use docopt::Docopt;

mod sum_squares;

const USAGE: &'static str = "
Martian adapter for sum_sq executable
Usage:
  sum_sq martian <adapter>...
  sum_sq (-h | --help)
Options:
  -h --help            Show this screen.
";

#[derive(Debug, Deserialize)]
struct Args {
    // Martian interface
    cmd_martian: bool,
    arg_adapter: Vec<String>,
}

fn main() -> Result<(), Error> {
    let args: Args = Docopt::new(USAGE)
        .and_then(|d| d.deserialize())
        .unwrap_or_else(|e| e.exit());

    if !args.cmd_martian {
        panic!("NOT IMPLEMENTED");
    }

    let registry = martian_stages![
        sum_squares::SumSquares
        // TODO: Add the stages here
    ];

    // Call the martian adapter
    martian::martian_main_with_log_level(args.arg_adapter, registry, martian::LevelFilter::Off)?;

    Ok(())
}

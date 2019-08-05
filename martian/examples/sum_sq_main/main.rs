use docopt::Docopt;
use martian::prelude::*;
use serde::Deserialize;

mod sum_squares;

const USAGE: &'static str = "
Martian adapter for sum_sq_no_split executable
Usage:
  sum_sq_no_split martian <adapter>...
  sum_sq_no_split (-h | --help)
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

    let registry = martian_stages![sum_squares::SumSquares];

    // Call the martian adapter
    martian::martian_main(args.arg_adapter, registry)?;

    Ok(())
}

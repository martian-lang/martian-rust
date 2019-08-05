use docopt::Docopt;
use martian::prelude::*;
use serde::Deserialize;

mod sum_squares;

const USAGE: &'static str = "
Martian adapter for sum_sq_no_split executable
Usage:
  sum_sq_main martian <adapter>...
  sum_sq_main mro [--file=<filename>] [--rewrite]
  sum_sq_main (-h | --help)
Options:
  -h --help            Show this screen.
  --file=<filename>    Output filename for the mro.
  --rewrite            Whether to rewrite the file if it exists.
";

#[derive(Debug, Deserialize)]
struct Args {
    // Martian interface
    cmd_martian: bool,
    // Mro generation
    cmd_mro: bool,
    flag_file: Option<String>,
    flag_rewrite: bool,
    arg_adapter: Vec<String>,
}

fn main() -> Result<(), Error> {
    let args: Args = Docopt::new(USAGE)
        .and_then(|d| d.deserialize())
        .unwrap_or_else(|e| e.exit());

    let (stage_registry, mro_registry) = martian_stages![sum_squares::SumSquares];

    // Call the martian adapter
    if args.cmd_martian {
        martian_main(args.arg_adapter, stage_registry)?;
    } else if args.cmd_mro {
        martian_make_mro(args.flag_file, args.flag_rewrite, mro_registry)?;
    } else {
        unimplemented!()
    }

    Ok(())
}

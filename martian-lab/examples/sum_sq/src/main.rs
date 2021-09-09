//! Martian-rust adapter sum_sq

use docopt::Docopt;
use martian::prelude::*;
use serde::Deserialize;

mod sum_squares;

const USAGE: &str = "
Martian adapter for sum_sq executable

Usage:
  sum_sq martian <adapter>...
  sum_sq mro [--file=<filename>] [--rewrite]
  sum_sq --help

Options:
  --help              Show this screen.
  --file=<filename>   Output filename for the mro.
  --rewrite           Whether to rewrite the file if it exists.
";

#[derive(Debug, Deserialize)]
struct Args {
    // Martian interface
    cmd_martian: bool,
    arg_adapter: Vec<String>,
    // Mro generation
    cmd_mro: bool,
    flag_file: Option<String>,
    flag_rewrite: bool,
}

fn main() -> Result<(), Error> {
    let args: Args = Docopt::new(USAGE)
        .and_then(|d| d.deserialize())
        .unwrap_or_else(|e| e.exit());

    let (stage_registry, mro_registry) = martian_stages![sum_squares::SumSquares];

    if args.cmd_martian {
        // Call the martian adapter
        let adapter = MartianAdapter::new(stage_registry);

        // If you want explicit control over the log level use for example:
        // let adapter = adapter.log_level(LevelFilter::Info);

        let retcode = adapter.run(args.arg_adapter);
        std::process::exit(retcode);
    } else if args.cmd_mro {
        // Create the mro for all the stages in this adapter
        martian_make_mro(
            "#
# Copyright (c) 2021 10X Genomics, Inc. All rights reserved.",
            args.flag_file,
            args.flag_rewrite,
            mro_registry,
        )?;
    } else {
        // If you need custom commands, implement them here
        unimplemented!()
    }

    Ok(())
}

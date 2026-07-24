mod cli;
mod db;
mod error;

use clap::Parser;
use cli::Cli;

fn main() {
    let _cli = Cli::parse();
}

use std::fs::File;

use lvm2::{Lvm2, Error};
use snafu::ResultExt;
use tracing::Level;

fn main() -> Result<(), snafu::Whatever> {
    tracing_subscriber::fmt().with_max_level(Level::TRACE).init();
    tracing::trace!("memes");

    let f = File::open("/dev/mapper/mireska").whatever_context("")?;
    let mut lvm = Lvm2::open(f).whatever_context("")?;
    lvm.foo().whatever_context("")?;
    Ok(())
}

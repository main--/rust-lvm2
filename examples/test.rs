use std::cell::RefCell;
use std::fs::File;

use acid_io::{Read, Seek, SeekFrom};
use ext4::Options;
use lvm2::{Lvm2};
use positioned_io::ReadAt;
use snafu::ResultExt;
use tracing::Level;

fn main() -> Result<(), snafu::Whatever> {
    tracing_subscriber::fmt().with_max_level(Level::TRACE).init();
    tracing::trace!("memes");

    let mut f = File::open("/dev/mapper/mireska").whatever_context("")?;
    let lvm = Lvm2::open(&mut f).whatever_context("")?;
    let lv = lvm.lvs().skip(3).next().unwrap();
    tracing::info!("LV {}", lv.name());
    let mut olv = lvm.open_lv(lv, &mut f);

    let mut buf = [0u8; 1024];
    olv.read_exact(&mut buf).unwrap();
    for _ in 0..16 {
    let mut buf2 = [0u8; 16];
    olv.read_exact(&mut buf2).unwrap();
    tracing::info!("{buf2:x?}");
    }

    let mut options = Options::default();
    options.checksums = ext4::Checksums::Enabled;
    let e4 = ext4::SuperBlock::new_with_options(Wrapper(RefCell::new(olv)), &options).whatever_context("ext4")?;
    let root = e4.root().whatever_context("root")?;
    e4.walk(&root, "/", &mut |_, path, _, _| {
        tracing::info!(%path);
        Ok(true)
    }).unwrap();

    Ok(())
}

struct Wrapper<T>(RefCell<T>);

impl<T: Read + Seek> ReadAt for Wrapper<T> {
    fn read_at(&self, pos: u64, buf: &mut [u8]) -> acid_io::Result<usize> {
        let mut this = self.0.borrow_mut();
        this.seek(SeekFrom::Start(pos))?;
        this.read(buf)
    }
}

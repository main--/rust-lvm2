
use acid_io::{Read, Seek, SeekFrom};

use crate::Lvm2;
use crate::metadata::LVDesc;

#[derive(Clone, Copy)]
pub struct LV<'a> {
    pub(crate) name: &'a str,
    pub(crate) desc: &'a LVDesc,
}
impl<'a> LV<'a> {
    pub fn name(&self) -> &'a str {
        &self.name
    }
    pub fn id(&self) -> &'a str {
        &self.desc.id
    }
    pub fn size_in_extents(&self) -> u64 {
        self.desc.segments.0.values().map(|x| x.start_extent + x.extent_count).max()
            .expect("LV has no segments??")
    }

    pub fn raw_metadata(&self) -> &'a LVDesc {
        self.desc
    }
}

pub struct OpenLV<'a, T> {
    pub(crate) lv: LV<'a>,
    pub(crate) lvm: &'a Lvm2,
    pub(crate) reader: T,

    pub(crate) position: u64,
    pub(crate) current_segment_end: u64,
}
impl<'a, T: Read + Seek> Read for OpenLV<'a, T> {
    fn read(&mut self, mut buf: &mut [u8]) -> acid_io::Result<usize> {
        if self.position == self.current_segment_end {
            // trigger a seek to load the next segment
            self.seek(SeekFrom::Current(0))?;
        }

        let max_read = self.current_segment_end - self.position;
        if u64::try_from(buf.len()).unwrap() > max_read {
            buf = &mut buf[..(max_read as usize)];
        }
        self.reader.read(buf)
    }
}
impl<'a, T: Read + Seek> Seek for OpenLV<'a, T> {
    fn seek(&mut self, pos: SeekFrom) -> acid_io::Result<u64> {
        let pos = match pos {
            SeekFrom::Start(x) => x,
            SeekFrom::End(x) => ((self.lv.size_in_extents() * self.lvm.extent_size()) as i64 - x) as u64,
            SeekFrom::Current(x) => ((self.position as i64) + x) as u64,
        };
        let target_extent = pos / self.lvm.extent_size();

        let segment = self.lv.desc.segments.0.values().find(|x| x.extents().contains(&target_extent)).ok_or(acid_io::Error::new(acid_io::ErrorKind::Other, "no suitable segment found at this place"))?;
        if segment.r#type != "striped" || segment.stripe_count != Some(1) {
            return Err(acid_io::Error::new(acid_io::ErrorKind::Other, "segment is not linear"));
        }

        let offs_in_segment = pos - (segment.start_extent * self.lvm.extent_size());

        let (pv, loc) = segment.stripes.as_ref().ok_or(acid_io::Error::new(acid_io::ErrorKind::Other, "segment has no stripes"))?;
        if pv != self.lvm.pv_name() {
            return Err(acid_io::Error::new(acid_io::ErrorKind::Other, "data is not on this PV"));
        }

        let mut seek_target = loc * self.lvm.extent_size() + offs_in_segment;
        let mut found = false;
        for dd in &self.lvm.pvh.data_descriptors {
            if dd.size == 0 || dd.size > seek_target {
                seek_target += dd.offset;
                found = true;
                break;
            } else {
                seek_target -= dd.size;
            }
        };
        if !found {
            return Err(acid_io::Error::new(acid_io::ErrorKind::Other, "data is beyond the end of this PV"));
        }

        // TODO: what is the purpose of this value if it's not needed for this?
        //let pe_start = self.lvm.vg_config.physical_volumes.get(pv).unwrap().pe_start;

        self.reader.seek(SeekFrom::Start(seek_target))?;

        self.current_segment_end = segment.extents().end * self.lvm.extent_size();
        self.position = pos;
        Ok(pos)
    }
}


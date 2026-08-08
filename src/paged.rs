//! Page-backed read sessions for on-demand element parsing.

use std::collections::{BTreeSet, HashMap};
use std::path::{Path, PathBuf};

use aios_core::pdms_types::RefU64;
use aios_core::types::db_info::PdmsDatabaseInfo;
use anyhow::{Context, Result, anyhow, ensure};
use pdmsdb_engine_v2::{DbHandle, EngineOptions, EngineV2, PageId, RefNo};

use crate::parse::{EleData, parse_ele_data_with_info};

pub use pdmsdb_engine_v2::PageReadStats;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PagedSnapshot {
    pub path: PathBuf,
    pub sesno: u32,
    pub session_page: PageId,
    pub index_root: PageId,
    pub page_size: usize,
    pub extent_count: usize,
}

/// A read-only database session pinned to the index root that was latest when
/// the file was opened. Only index and requested record pages are retained.
pub struct PagedDbSession {
    handle: DbHandle,
    snapshot: PagedSnapshot,
}

impl PagedDbSession {
    pub fn open(path: &Path) -> Result<Self> {
        let handle = EngineV2::open_read(path, EngineOptions::default())
            .with_context(|| format!("打开页式数据库失败: {}", path.display()))?;
        let latest = handle
            .latest_session()
            .with_context(|| format!("读取最新会话失败: {}", path.display()))?;
        let snapshot = PagedSnapshot {
            path: path.to_path_buf(),
            sesno: latest.sesno,
            session_page: latest.page,
            index_root: latest.index_root,
            page_size: handle.page_size(),
            extent_count: handle.extent_count(),
        };
        Ok(Self { handle, snapshot })
    }

    pub fn snapshot(&self) -> &PagedSnapshot {
        &self.snapshot
    }

    pub fn stats(&self) -> PageReadStats {
        self.handle.read_stats()
    }

    pub fn scan_ref0s(&mut self) -> Result<Vec<u32>> {
        let mut ref0s = BTreeSet::new();
        self.handle
            .scan_refnos_from_root(self.snapshot.index_root, |entry| {
                let ref0 = entry.refno.hi();
                if ref0 != 0 && ref0 != 0x8000_0001 {
                    ref0s.insert(ref0);
                }
                Ok(())
            })
            .with_context(|| {
                format!("扫描页式 RefNo 索引失败: {}", self.snapshot.path.display())
            })?;
        Ok(ref0s.into_iter().collect())
    }

    pub fn read_raw_records(&mut self, refnos: &[RefU64]) -> Result<HashMap<RefU64, Vec<u8>>> {
        let engine_refnos = refnos
            .iter()
            .map(|refno| RefNo::from_parts(refno.get_0(), refno.get_1()))
            .collect::<Vec<_>>();
        let records = self
            .handle
            .read_elements_from_root(self.snapshot.index_root, &engine_refnos)
            .with_context(|| format!("按 RefNo 读取记录失败: {}", self.snapshot.path.display()))?;

        let mut out = HashMap::with_capacity(records.len());
        for (refno, raw) in records {
            out.insert(RefU64::from_two_nums(refno.hi(), refno.lo()), raw);
        }
        Ok(out)
    }

    pub async fn parse_elements_with_info(
        &mut self,
        refnos: &[RefU64],
        info: &PdmsDatabaseInfo,
    ) -> Result<HashMap<RefU64, EleData>> {
        // Finish all synchronous page I/O before the first await. This keeps the
        // DbHandle's RefCell borrows out of the asynchronous attribute decoder.
        let records = self.read_raw_records(refnos)?;
        let mut parsed = HashMap::with_capacity(records.len());
        for (requested, raw) in records {
            let payload =
                record_payload(&raw).with_context(|| format!("记录前导损坏: {requested}"))?;
            let element = parse_ele_data_with_info(payload, info)
                .await
                .with_context(|| format!("页式元素解析失败: {requested}"))?;
            ensure!(
                element.refno == requested,
                "页式索引返回错误元素: 请求 {requested}, 得到 {}",
                element.refno
            );
            parsed.insert(requested, element);
        }
        Ok(parsed)
    }
}

fn record_payload(raw: &[u8]) -> Result<&[u8]> {
    let mut prefix = 0usize;
    while prefix + 4 <= raw.len() {
        let word = &raw[prefix..prefix + 4];
        if word == [0, 0, 0, 0] || word == [0, 0, 0, 7] {
            prefix += 4;
        } else {
            break;
        }
    }
    ensure!(raw.len().saturating_sub(prefix) >= 24, "元素记录长度不足");
    let impl_words = i32::from_be_bytes(
        raw[prefix..prefix + 4]
            .try_into()
            .map_err(|_| anyhow!("读取 impl_len 失败"))?,
    );
    ensure!(impl_words > 0, "impl_len 非法: {impl_words}");
    ensure!(
        (impl_words as usize).saturating_mul(4) <= raw.len() - prefix,
        "impl_len 超出记录边界: words={impl_words}, bytes={}",
        raw.len() - prefix
    );
    Ok(&raw[prefix..])
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};

    use aios_core::pdms_types::RefU64;
    use pdmsdb_engine_v2::ElementRecordView;

    use super::{PagedDbSession, record_payload};

    #[test]
    fn strips_aligned_zero_and_seven_prefixes() {
        let mut raw = vec![0, 0, 0, 0, 0, 0, 0, 7];
        raw.extend_from_slice(&6i32.to_be_bytes());
        raw.extend_from_slice(&[0u8; 20]);
        let payload = record_payload(&raw).unwrap();
        assert_eq!(&payload[..4], &6i32.to_be_bytes());
        assert_eq!(payload.len(), 24);
    }

    #[test]
    fn rejects_declared_implicit_region_beyond_record() {
        let mut raw = 100i32.to_be_bytes().to_vec();
        raw.extend_from_slice(&[0u8; 20]);
        assert!(record_payload(&raw).is_err());
    }

    #[test]
    fn large_cata_locator_scan_stays_below_fifteen_percent_io() {
        let path = Path::new(r"D:\AVEVA\Projects\E3D3.1\AvevaCatalogue\acp000\acp7320_0001");
        if !path.exists() {
            return;
        }

        let file_len = std::fs::metadata(path).unwrap().len();
        let mut session = PagedDbSession::open(path).unwrap();
        let ref0s = session.scan_ref0s().unwrap();
        let stats = session.stats();

        assert!(!ref0s.is_empty());
        assert_eq!(stats.record_pages_read, 0);
        assert!(
            stats.bytes_read <= file_len * 15 / 100,
            "paged locator read {} of {} bytes",
            stats.bytes_read,
            file_len
        );
    }

    #[tokio::test]
    #[ignore = "requires the AMS real database fixture and performs one legacy full read"]
    async fn paged_record_matches_legacy_record_identity() {
        let path = PathBuf::from(r"D:\work\plant-code\pdms-io-fork\test-file\ams1112_0001");
        if !path.exists() {
            return;
        }
        let refno = RefU64::from_two_nums(17496, 171138);
        let mut paged = PagedDbSession::open(&path).unwrap();
        let mut raw_records = paged.read_raw_records(&[refno]).unwrap();
        let raw = raw_records.remove(&refno).unwrap();
        let payload = record_payload(&raw).unwrap();
        let view = ElementRecordView::from_raw(&raw).unwrap();

        let legacy = crate::parse::parse_file_db_index_data(&path).unwrap();
        let pos = legacy.refno_table_map.get(&refno).unwrap().pos;
        let legacy_payload = &legacy.bytes[pos - 4..];

        assert_eq!(&payload[..24], &legacy_payload[..24]);
        assert_eq!(
            (view.refno.hi(), view.refno.lo()),
            (refno.get_0(), refno.get_1())
        );
        assert_eq!(
            view.noun_hash,
            u32::from_be_bytes(payload[12..16].try_into().unwrap())
        );
        assert_eq!(
            view.owner.hi(),
            u32::from_be_bytes(payload[16..20].try_into().unwrap())
        );
        assert_eq!(
            view.owner.lo(),
            u32::from_be_bytes(payload[20..24].try_into().unwrap())
        );
    }
}

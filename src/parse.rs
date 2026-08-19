use crate::consts::*;
use crate::parse_explict_tools::*;
use aios_core::AttrVal::*;
use aios_core::SUL_DB;
use aios_core::basic::info::RefnoInfo;
use aios_core::consts::{EXPR_ATT_SET, NAME_HASH, TYPE_HASH};
use aios_core::db::*;
use aios_core::get_db_option;
use aios_core::get_default_pdms_db_info;
use aios_core::helper::*;
use aios_core::parse::*;
use aios_core::pdms_types::*;
use aios_core::petgraph::PetRefnoNode;
use aios_core::tool::db_tool::*;
use aios_core::types::WholeAttMap;
use aios_core::types::db_info::PdmsDatabaseInfo;
use aios_core::types::*;
use anyhow::*;
use cached::proc_macro::cached;
use core::result::Result::Ok;
#[allow(unused_mut)]
use core::slice::SlicePattern;
use dashmap::{DashMap, DashSet};
use itertools::Itertools;
use memchr::memmem;
use memchr::memmem::rfind_iter;
use nom::IResult;
use nom::bytes::complete::take_until;
use nom::character::complete::alpha1;
use nom::combinator::verify;
use nom::error::ErrorKind;
use nom::multi::count;
use nom::multi::many_till;
use nom::number::complete::{be_i16, be_i32, be_u16, be_u32, be_u64};
use nom::sequence::tuple;
use phf::phf_map;
use pretty_hex::{pretty_hex, simple_hex};
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use rayon::prelude::IntoParallelIterator;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt::Debug;
use std::fs;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, Write};
use std::ops::{Deref, Range};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;
use tokio::io::AsyncReadExt;

//00 00 00 05 00 CC 47 DF 00 00 00 00 00 00 00 02
// const REFNO_ALL_INDEX_PAGE: [u8; 11] = [0x00u8, 0x00, 0x00, 0x05, 0x00, 0xCC, 0x47, 0xDF, 0x00, 0x00, 0x00];
const REFNO_ALL_INDEX_PAGE: [u8; 8] = [0x0u8, 0xCC, 0x47, 0xDF, 0x0, 0x0, 0x0, 0x0];

///一个pdms db的整体数据
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PdmsDbData {
    /// 按noun类型分类的参考号
    pub type_ele_map: DashMap<u32, HashSet<RefU64>>,
    /// 完整属性数据的存储
    pub total_attr_map: DashMap<RefU64, NamedAttrMap>,
    /// 所有包含子节点的map
    pub children_map: HashMap<RefU64, RefU64Vec>,
    ///数据文件名
    pub filename: String,
    ///数据文件的ses pgno
    pub ses_pgno: u32,
    ///数据文件的db type（DESI、CATA、SYS等等）
    pub db_type: String,
    /// 数据文件的db 名称（SYS里用的名称）
    pub db_name: String,
    ///数据文件的 db number
    pub db_no: u32,
    ///数据文件的field no
    pub field_no: u32,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RoomCode {
    pub refno: RefU64,
    pub name_hash: AiosStrHash,
}

///解析pdms的目录
pub async fn parse_pdms_dir(
    dir: &str,
    project: &str,
    config_path: Option<&str>,
    need_parsed_files: &Option<Vec<String>>,
) -> Result<DashMap<String, PdmsDbData>> {
    let dir = PathBuf::from(dir);
    let mut pdms_project_data_map = DashMap::new();
    let mut children_files = fs::read_dir(dir)?
        .into_iter()
        .filter_map(|entry| entry.ok().map(|e| e.path()))
        .filter(|p| is_pdms_db_file(p) && has_valid_db_header(p))
        .collect::<Vec<PathBuf>>();
    let mut database_info = None;

    if config_path.is_some() {
        if let Ok(mut file) = File::open(config_path.unwrap()) {
            let mut attr_buf: Vec<u8> = Vec::new();
            file.read_to_end(&mut attr_buf)
                .context("read database_info config")?;
            database_info = bincode::deserialize(&attr_buf).ok();
        }
    }

    let mut pdms_db_name_map = DashMap::new(); //file_name->db_name
    let mut sys_file = None;
    for path in &children_files {
        let file_name = path.file_name().unwrap().to_str().unwrap();
        //sys file 信息的处理
        if file_name.ends_with("sys") {
            if need_parsed_files.is_some()
                && !need_parsed_files
                    .as_ref()
                    .unwrap()
                    .contains(&file_name.to_string())
            {
                continue;
            }
            let mut pdms_db_data = parse_file(&path, &database_info, file_name, project).await?;
            pdms_db_data
                .total_attr_map
                .iter()
                .try_for_each::<_, Result<()>>(|m| {
                    let map = m.value();
                    let num = map
                        .get_u32("NUMBDB")
                        .ok_or(anyhow!("NUMBDB not exist".to_string()))?;
                    let fnum = map
                        .get_u32("FINO")
                        .ok_or(anyhow!("FINO not exist".to_string()))?;
                    let name = map
                        .get_as_string("NAME")
                        .ok_or(anyhow!("NAME not exist".to_string()))?
                        .to_string();
                    let db_no = if fnum == 0 { num } else { fnum };
                    pdms_db_name_map.insert(db_no, name);
                    Ok(())
                })?;
            if pdms_db_name_map.contains_key(&pdms_db_data.db_no) {
                pdms_db_data.db_name = pdms_db_name_map.get(&pdms_db_data.db_no).unwrap().clone();
            } else {
                pdms_db_data.db_name = file_name.into();
            }
            pdms_db_data.filename = file_name.into();
            pdms_project_data_map.insert(pdms_db_data.filename.clone(), pdms_db_data);
            sys_file = Some(path);
        }
    }

    if let Some(sys_file) = sys_file {
        children_files.remove(children_files.iter().position(|x| x == sys_file).unwrap());
    }

    for path in children_files {
        let file_name = path.file_name().unwrap().to_str().unwrap().to_string();
        if !file_name.ends_with("com") && !file_name.ends_with("mis") {
            if need_parsed_files.is_none()
                || need_parsed_files.as_ref().unwrap().contains(&file_name)
            {
                let file_name = file_name.as_str();
                println!("path={:?}", file_name);
                if let Ok(mut pdms_db_data) =
                    parse_file(&path, &database_info, file_name, project).await
                {
                    pdms_db_data.filename = file_name.into();
                    let cur_dbno = pdms_db_data.db_no.to_string();
                    if pdms_db_data.filename.contains(&cur_dbno) {
                        if pdms_db_name_map.contains_key(&pdms_db_data.db_no) {
                            pdms_db_data.db_name =
                                pdms_db_name_map.get(&pdms_db_data.db_no).unwrap().clone();
                        }
                    } else {
                        if pdms_db_name_map.contains_key(&pdms_db_data.field_no) {
                            pdms_db_data.db_name = pdms_db_name_map
                                .get(&pdms_db_data.field_no)
                                .unwrap()
                                .clone();
                        } else {
                            pdms_db_data.db_name = file_name.into();
                        }
                    }
                    pdms_project_data_map.insert(pdms_db_data.filename.clone(), pdms_db_data);
                }
            }
        }
    }

    return Ok(pdms_project_data_map);
}

///解析db文件的chidlren部分，得到参考号和对应的类型集合
pub fn parse_file_db_basic_data(
    path: &PathBuf,
    file_name: &str,
    project: &str,
) -> Result<DbBasicData> {
    if !is_pdms_db_file(path) || !has_valid_db_header(path) {
        return Err(anyhow!("skip non-db file: {:?}", path));
    }
    let time_start = Instant::now();
    let mut file = File::open(path)?;
    let mut buf: Vec<u8> = Vec::new();
    file.read_to_end(&mut buf)?;
    let time = time_start.elapsed();
    println!("read file {:?} finished in {:?}", path, time);
    //使用默认的配置信息
    let mut basic_data = parse_db_basic_data(buf, file_name, project)?;
    Ok(basic_data)
}

/// 只含 refno 索引的解析产物：够按 refno 随机定位再逐元素解析，不含 owner→children 全量树。
///
/// 刻意与 [`DbBasicData`] 分成两个类型，而不是给后者留一个空的 `children_map`：留空的话
/// 任何漏改的读取点都会静默拿到空表、少件而不报错，而这种错没有声音。分成独立类型后，
/// 「谁还在读成员树」由编译器回答。
#[derive(Default, Debug)]
pub struct DbIndexData {
    pub ses_pgno: u32,
    pub bytes: Vec<u8>,
    pub world_refno: RefU64,
    pub refno_table_map: DashMap<RefU64, EleDataEntry>,
}

/// 只建 refno 索引表的解析：读文件 + [`gen_ref_type_pos_table`]，**跳过整棵成员树的展开**。
///
/// 展开成员树占 [`parse_db_basic_data`] 约四分之三的耗时（7997 的 acp7320 一个文件就是
/// 117 万元素、单次 21 秒），而按 refno 随机取件的调用方只要能定位到记录就够了——children
/// 由元素记录自带，不必先有一棵全量树。
pub fn parse_file_db_index_data(path: &PathBuf) -> Result<DbIndexData> {
    if !is_pdms_db_file(path) || !has_valid_db_header(path) {
        return Err(anyhow!("skip non-db file: {:?}", path));
    }
    let time_start = Instant::now();
    let mut file = File::open(path)?;
    let mut buf: Vec<u8> = Vec::new();
    file.read_to_end(&mut buf)?;
    println!(
        "read file {:?} finished in {:?}",
        path,
        time_start.elapsed()
    );
    Ok(parse_db_index_data(buf))
}

/// 见 [`parse_file_db_index_data`]。
pub fn parse_db_index_data(input: Vec<u8>) -> DbIndexData {
    let gen_ref_time = Instant::now();
    let (refno_table_map, world_refno) = gen_ref_type_pos_table(&input);
    println!(
        "gen_ref_type_pos_table: {} ms",
        gen_ref_time.elapsed().as_millis()
    );
    let DbBasicInfo { ses_pgno, .. } = parse_file_basic_info(&input);
    DbIndexData {
        ses_pgno,
        bytes: input,
        world_refno,
        refno_table_map,
    }
}

///解析db文件
pub async fn parse_file(
    path: &PathBuf,
    database_info: &Option<PdmsDatabaseInfo>,
    file_name: &str,
    project: &str,
) -> Result<PdmsDbData> {
    if !is_pdms_db_file(path) {
        return Err(anyhow!("skip non-db file: {:?}", path));
    }
    let time_start = Instant::now();
    let mut file = File::open(path)?;
    let mut buf: Vec<u8> = Vec::new();
    file.read_to_end(&mut buf).context("read db file")?;
    let input = &buf[..];
    let time = time_start.elapsed();
    println!("read file {:?} finished in {:?}", path, time);
    if database_info.is_none() {
        //使用默认的配置信息
        let db_info = get_default_pdms_db_info();
        parse_db(input, &db_info, file_name, project).await
    } else {
        parse_db(input, database_info.as_ref().unwrap(), file_name, project).await
    }
}

///解析db文件
#[inline]
pub async fn parse_file_with_chunk(
    db_basic_data: Arc<DbBasicData>,
    file_name: &str,
    project: &str,
    chunk_refnos: &[RefU64],
    ses_range_map: &BTreeMap<i32, Range<u32>>,
    ignore_world_refno: bool,
) -> Result<PdmsDbData> {
    let db_info = get_default_pdms_db_info();
    parse_db_with_chunk_with_info(
        db_basic_data,
        &db_info,
        file_name,
        project,
        chunk_refnos,
        ses_range_map,
        ignore_world_refno,
    )
    .await
}

/// 解析db文件的chidlren部分，得到参考号和对应的类型集合（显式指定数据库信息）
pub async fn parse_file_with_chunk_with_info(
    db_basic_data: Arc<DbBasicData>,
    database_info: &PdmsDatabaseInfo,
    file_name: &str,
    project: &str,
    chunk_refnos: &[RefU64],
    ses_range_map: &BTreeMap<i32, Range<u32>>,
    ignore_world_refno: bool,
) -> Result<PdmsDbData> {
    parse_db_with_chunk_with_info(
        db_basic_data,
        database_info,
        file_name,
        project,
        chunk_refnos,
        ses_range_map,
        ignore_world_refno,
    )
    .await
}

#[derive(Debug, Clone, Default)]
pub struct EleData {
    pub refno: RefU64,
    pub owner: RefU64,
    pub noun: u32,
    pub whole_attmap: WholeAttMap,
    pub children: RefU64Vec,
    pub name: String,
}

/// Element identity decoded from the fixed dabacon record header.
///
/// Unlike [`EleData`], this type does not require the noun attribute dictionary
/// to contain the decoded noun.  Callers can therefore classify a record even
/// when canonical element decoding fails later.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawElementIdentity {
    pub refno: RefU64,
    pub noun_hash: i32,
    pub noun_name: String,
    pub owner: RefU64,
}

/// Decode only the fixed identity fields at the beginning of an element record.
pub fn parse_raw_element_identity(input: &[u8]) -> Result<RawElementIdentity> {
    if input.len() < 24 {
        return Err(anyhow!(
            "element record identity requires 24 bytes, got {}",
            input.len()
        ));
    }

    let implicit_words = try_parse_to_i32(&input[0..4])?;
    if implicit_words < 6 {
        return Err(anyhow!(
            "element record implicit header is too short: {implicit_words} words"
        ));
    }

    let refno = RefU64::from(&input[4..12]);
    let noun_hash = try_parse_to_i32(&input[12..16])?;
    let owner = RefU64::from(&input[16..24]);

    Ok(RawElementIdentity {
        refno,
        noun_hash,
        noun_name: db1_dehash(noun_hash as u32),
        owner,
    })
}

impl EleData {
    #[inline]
    pub fn att_map(&self) -> &NamedAttrMap {
        self.whole_attmap.att_map()
    }

    #[inline]
    pub fn explicit_attmap(&self) -> &NamedAttrMap {
        self.whole_attmap.explicit_attmap()
    }

    #[inline]
    pub fn explicit_attmap_mut(&mut self) -> &mut NamedAttrMap {
        self.whole_attmap.explicit_attmap_mut()
    }

    #[inline]
    pub fn uda_atts(&self) -> &Vec<ExplicitAttr> {
        self.whole_attmap.uda_atts()
    }

    #[inline]
    pub fn uda_atts_mut(&mut self) -> &mut Vec<ExplicitAttr> {
        self.whole_attmap.uda_atts_mut()
    }

    #[inline]
    pub fn att_map_mut(&mut self) -> &mut NamedAttrMap {
        self.whole_attmap.att_map_mut()
    }

    #[inline]
    pub fn refno_enum(&self) -> RefnoEnum {
        self.att_map().ses_refno().into()
    }

    #[inline]
    pub fn refno(&self) -> RefU64 {
        self.att_map().latest_refno()
    }

    #[inline]
    pub fn latest_refno_enum(&self) -> RefnoEnum {
        self.att_map().latest_refno().into()
    }
}

//只是获得RefU64, 用于多线程找到所有需要处理的参考号
pub fn parse_ele_membs(input: &[u8]) -> Vec<RefU64> {
    let mut members = vec![];
    if input.len() < 12 {
        return members;
    }
    let impl_len = parse_to_i32(&input[0..4]); //隐含数据长度  0-4
    if impl_len < 0 {
        return members;
    }
    let refno = RefI32Tuple::from(&input[4..12]);
    let Some(declared_impl_len) = (impl_len as usize).checked_mul(4) else {
        return members;
    };
    let Ok(a) = padded_implicit_end(input, declared_impl_len) else {
        return members;
    };
    //隐藏属性得数据切片
    let membs_data = &input[a..];
    if membs_data.len() < 12 {
        return members;
    }
    let maybe_refno: RefI32Tuple = (&membs_data[4..12]).into();
    let mut memb_bytes_len = 0;

    if maybe_refno == refno {
        if &membs_data[0..2] == [0x0, 0x2].as_slice() {
            memb_bytes_len = parse_to_u16(&membs_data[2..4]) as usize * 4;
            let merged_data = get_merged_data(membs_data, &mut memb_bytes_len, 0x2);
            // dbg!(merged_data.len());
            if let Ok((_, c)) = parse_attr_members(&merged_data) {
                members = c.0;
            }
        }
    }
    members
}

/// 解析元素的子元素
///
/// # 参数
/// * `input` - 输入的字节数组切片
///
/// # 返回值
/// * `(RefU64, RefU64Vec)` - 返回一个元组,包含:
///   - 当前元素的引用号(RefU64)
///   - 子元素引用号的向量(RefU64Vec)
#[inline]
pub fn parse_ele_children(input: &[u8]) -> (RefU64, RefU64Vec) {
    let children = RefU64Vec::default();
    if input.len() < 16 {
        return (RefU64::default(), children);
    }
    let impl_len = parse_to_i32(&input[0..4]); //隐含数据长度  0-4
    let refno = RefI32Tuple::from(&input[4..12]);
    let type_hash = parse_to_i32(&input[12..16]);
    let noun = type_hash as u32;
    if impl_len < 0 {
        return (refno.into(), children);
    }
    let Some(origin_impl_len) = (impl_len as usize).checked_mul(4) else {
        return (refno.into(), children);
    };
    let Ok(actual_impl_len) = padded_implicit_end(input, origin_impl_len) else {
        return (refno.into(), children);
    };
    //隐藏属性得数据切片
    let implicit_data = &input[0..actual_impl_len];
    let membs_pos = actual_impl_len;
    let membs_data = &input[membs_pos..];
    if membs_data.len() < 12 {
        return (refno.into(), children);
    }
    let maybe_refno: RefI32Tuple = (&membs_data[4..12]).into();
    let mut memb_bytes_len = 0;

    if maybe_refno == refno {
        if &membs_data[0..2] == [0x0, 0x2].as_slice() {
            memb_bytes_len = parse_to_u16(&membs_data[2..4]) as usize * 4;
            let merged_data = get_merged_data(membs_data, &mut memb_bytes_len, 0x2);
            if let Ok((_, c)) = parse_attr_members(&merged_data) {
                return (refno.into(), c);
            }
        }
    }

    (refno.into(), children)
}

/// 解析元素的基础数据（同步函数，不进行异步操作）
fn padded_implicit_end(input: &[u8], mut end: usize) -> Result<usize> {
    if end > input.len() {
        return Err(anyhow!("implicit region exceeds record"));
    }
    while input.len().saturating_sub(end) >= 4 {
        let value = parse_to_i32(&input[end..end + 4]);
        if value != 0 && value != 7 {
            break;
        }
        end += 4;
    }
    Ok(end)
}

pub fn parse_raw_ele_data_with_info(
    input: &[u8],
    database_info: &PdmsDatabaseInfo,
) -> Result<EleData> {
    let mut implicit_attmap = NamedAttrMap::default();
    let mut explicit_attmap = NamedAttrMap::default();
    let mut children = RefU64Vec::default();
    let data_len = input.len();
    let impl_len = try_parse_to_i32(&input[0..4])?; //隐含数据长度  0-4
    if impl_len < 0 {
        return Err(anyhow!("impl_len < 0"));
    }
    let origin_impl_len = impl_len
        .checked_mul(4)
        .ok_or_else(|| anyhow!("impl_len overflow"))?;
    let mut actual_impl_len = usize::try_from(origin_impl_len)
        .map_err(|_| anyhow!("impl_len cannot be represented as usize"))?;
    if actual_impl_len > data_len {
        return Err(anyhow!("impl_len > data_len"));
    }
    let refno: RefU64 = RefU64::from(&input[4..12]);
    let type_hash = try_parse_to_i32(&input[12..16])?;
    let noun = type_hash as u32;
    let noun_name = db1_dehash(noun); //类型hash  12-16
    let cur_type_info_map = database_info
        .named_attr_info_map
        .get(&noun_name)
        .ok_or(anyhow!("{} not exist in attr_info_map", &noun_name))?;
    let hash_type_info_map = database_info
        .noun_attr_info_map
        .get(&type_hash)
        .ok_or(anyhow!("{} not exist in attr_info_map", &noun_name))?;
    let owner = RefU64::from(&input[16..24]);
    actual_impl_len = padded_implicit_end(input, actual_impl_len)?;
    //隐藏属性得数据切片
    let implicit_data = &input[0..actual_impl_len];
    let membs_pos = actual_impl_len;
    let membs_data = &input[membs_pos..];
    let maybe_refno: Option<RefU64> = if membs_data.len() > 12 {
        Some(RefU64::from(&membs_data[4..12]))
    } else {
        None
    };
    let mut memb_bytes_len = 0;

    if maybe_refno.is_some() && maybe_refno.unwrap() == refno {
        if &membs_data[0..2] == [0x0, 0x2].as_slice() {
            memb_bytes_len = parse_to_u16(&membs_data[2..4]) as usize * 4;
            let merged_data = get_merged_data(membs_data, &mut memb_bytes_len, 0x2);
            if let Ok((_, c)) = parse_attr_members(&merged_data) {
                children = c;
            }
        }
    }

    let explicit_start = actual_impl_len + memb_bytes_len;
    if explicit_start > input.len() {
        return Err(anyhow!("explicit_start > input.len()"));
    }
    let explicit_data = &input[explicit_start..];
    let sorted_noun_hash = sort_offsets(&hash_type_info_map);
    let mut cur_offset: i32 = 0;
    let mut is_f32 = false;

    if sorted_noun_hash.len() > 0 {
        let last_key = sorted_noun_hash.last().unwrap();
        let last_att_info = hash_type_info_map.get(last_key).unwrap();
        let last_step = match last_att_info.att_type {
            DbAttributeType::DIRECTION
            | DbAttributeType::POSITION
            | DbAttributeType::ORIENTATION
            | DbAttributeType::Vec3Type => 3 * 2,
            DbAttributeType::ELEMENT => 2,
            //填的最小的数量，最少有两个数据
            DbAttributeType::INTVEC | DbAttributeType::FLOATVEC | DbAttributeType::DOUBLEVEC => 2,
            _ => 1,
        };
        is_f32 = last_att_info.offset + last_step > (origin_impl_len / 4) as u32;
    }
    //如果发现是f32的数据，就需要重新算偏移
    let mut f32_neg_offset = 0usize;
    for i in 0..sorted_noun_hash.len() {
        let noun_hash = sorted_noun_hash[i];
        let noun_name = db1_dehash(noun_hash as _);
        let attr_info = cur_type_info_map.get(&noun_name).unwrap();
        if cur_offset == 0 {
            cur_offset = (attr_info.offset & 0xFFFFF) as i32;
        }
        let mut step_w = 0;
        if i >= 1 {
            let prev_attr_info = hash_type_info_map.get(&sorted_noun_hash[i - 1]).unwrap();
            let b_expr = check_is_expr(prev_attr_info.hash);
            if is_f32 && !b_expr {
                match prev_attr_info.att_type {
                    DbAttributeType::DOUBLE => {
                        f32_neg_offset += 1;
                    }
                    DbAttributeType::DIRECTION
                    | DbAttributeType::POSITION
                    | DbAttributeType::ORIENTATION
                    | DbAttributeType::Vec3Type => {
                        f32_neg_offset += 3;
                    }
                    _ => {}
                }
            }
        }
        if i < sorted_noun_hash.len() - 1 {
            let next_attr_info = hash_type_info_map.get(&sorted_noun_hash[i + 1]).unwrap();
            step_w =
                ((next_attr_info.offset & 0xFFFFF) as i32 - (attr_info.offset & 0xFFFFF) as i32);
        } else {
            step_w = origin_impl_len / 4 - (attr_info.offset as i32 & 0xFFFFF);
        }
        let step = step_w.max(1) as usize;
        if let Ok((_, att_val)) =
            parse_implicit_attr_value(&implicit_data, &attr_info, is_f32, f32_neg_offset, step)
        {
            match &att_val {
                RefU64Type(value) => {
                    if attr_info.name.to_lowercase() != "owner" && value.get_0() != 0 {
                        // foreign_refnos.insert(attr_info.name.to_string(), *value);
                    }
                }
                InvalidType => {
                    dbg!(&refno);
                    dbg!(&noun_name);
                }
                _ => {}
            }
            // unset 是pdms数据中存在info文件里没有的offset数据，手动在info文件里面加的这个 unset 占位
            if attr_info.name != "unset" {
                implicit_attmap.insert(attr_info.name.clone(), att_val.into());
            }
        }
    }
    let final_explicit_data = collect_explict_data(explicit_data, refno);

    //添加遗漏的属性
    implicit_attmap.insert("OWNER".into(), NamedAttrValue::RefU64Type(owner));
    implicit_attmap.insert("TYPE".into(), NamedAttrValue::StringType(noun_name));
    implicit_attmap.insert("REFNO".into(), NamedAttrValue::RefU64Type(refno));
    let name = implicit_attmap.get_name_or_default();

    let explicit_attrs =
        match parse_raw_explicit_attrs(&final_explicit_data, &cur_type_info_map, refno) {
            Ok(result) => result.1,
            Err(e) => {
                println!("解析显式属性失败: {:?}, refno={:?}", e, refno);
                Vec::new()
            }
        };

    // 将ExplicitAttr分成UDA属性和普通显式属性
    let mut uda_atts = Vec::new();
    for attr in explicit_attrs {
        if attr.is_uda {
            uda_atts.push(attr);
        } else {
            explicit_attmap.insert(attr.name, attr.value.into());
        }
    }

    // 创建基础 EleData，包含UDA属性列表
    let ele_data = EleData {
        refno,
        owner,
        noun,
        whole_attmap: WholeAttMap {
            attmap: implicit_attmap,
            explicit_attmap,
            uda_atts,
        },
        children,
        name,
    };

    Ok(ele_data)
}

/// 解析元素的基础数据（使用默认数据库配置）
pub fn parse_raw_ele_data(input: &[u8]) -> Result<EleData> {
    let db_info = get_default_pdms_db_info();
    parse_raw_ele_data_with_info(input, &db_info)
}

/// 解析元素数据，包含异步处理
pub async fn parse_ele_data_with_info(
    input: &[u8],
    database_info: &PdmsDatabaseInfo,
) -> Result<EleData> {
    // 使用同步函数解析基础数据
    let mut ele_data = parse_raw_ele_data_with_info(input, database_info)?;

    // 获取需要的信息用于异步调用
    let refno = ele_data.refno;
    let noun_name = db1_dehash(ele_data.noun);
    let cur_type_info_map = database_info
        .named_attr_info_map
        .get(&noun_name)
        .ok_or(anyhow!("{} not exist in attr_info_map", &noun_name))?;

    // 解析显式属性
    // 异步处理显式属性
    let explicit_attmap = &mut ele_data.whole_attmap.explicit_attmap;

    // 如果存在UDA属性，进行异步处理
    if !ele_data.whole_attmap.uda_atts.is_empty() {
        let _ = process_explicit_attrs(
            std::mem::take(&mut ele_data.whole_attmap.uda_atts),
            explicit_attmap,
        )
        .await;
    }
    // 精炼属性
    ele_data.whole_attmap = ele_data.whole_attmap.refine(&cur_type_info_map);

    Ok(ele_data)
}

/// 解析元素数据，包含异步处理（使用默认数据库配置）
pub async fn parse_ele_data(input: &[u8]) -> Result<EleData> {
    let db_info = get_default_pdms_db_info();
    parse_ele_data_with_info(input, &db_info).await
}

//移除00 00 00 007，保留后面的数据
//
// gen-model-9 / ADR-006：显式数据可能跨多个记录块(block)，块之间可能夹有其它块头或
// 0x07 追加段(continuation segment)。旧实现遇到「块头 flag!=1 / self-ref 不匹配 / 长度非法」
// 会直接 break，导致像 CURD/DBLS 这类跨块的长引用列表属性主体被丢弃（表现为收集缓冲区
// 正好停在属性 hash 处、解析报「显式属性退出」，进而 SYS 元数据的设计 MDB/CURD 建不起来）。
// 现按 pdms-io-fork 的做法：遇到不匹配块按 word 对齐 resync 继续找下一个匹配块，并收集 0x07
// 追加段的 payload；主段起点用「可解析性」自适应判定(12/20)。旧实现保留为 *_legacy 作对照/兜底。
pub fn collect_explict_data(mut input: &[u8], refno: RefU64) -> Vec<u8> {
    const MAX_RESYNC: usize = 64;
    let mut bytes: Vec<u8> = Vec::new();
    if input.len() < 4 {
        return bytes;
    }
    // 自适应判定：buf 是否像一个属性流的起点（用于区分主段是否含 8 字节保留区）
    let looks_like_attr_stream_start = |buf: &[u8]| -> bool {
        if buf.len() < 4 {
            return false;
        }
        let hash_val = convert_to_hash(&buf[..4]);
        if hash_val == 0 {
            return false;
        }
        if check_is_expr(hash_val) && parse_expression_attr_nom(buf, refno).is_ok() {
            return true;
        }
        if buf.len() < 8 {
            return false;
        }
        let type_code = parse_to_u16(&buf[4..6]);
        if get_explicit_attr_type(type_code).is_none() {
            return false;
        }
        let data_len = parse_to_u16(&buf[6..8]) as usize * 4;
        data_len <= buf.len() - 8
    };
    let mut resync = 0usize;
    while input.len() >= 4 {
        let v = parse_to_i32(&input[..4]);
        match v {
            0 | 7 => {
                input = &input[4..];
            }
            5 => {
                if input.len() < 8 {
                    break;
                }
                let page_type = parse_to_i32(&input[4..8]);
                #[cfg(feature = "debug_parse")]
                println!("Found {refno} attr may be in page type {:#4X?}", page_type);
                if page_type == 0xCC47DF && input.len() > 0x2C {
                    //一直找到为0的为止
                    input = &input[0x2C..];
                    while input.len() >= 4 && parse_to_i32(&input[0..4]) != 0 {
                        input = &input[4..];
                    }
                    if input.len() > 4 {
                        input = &input[4..];
                    } else {
                        break;
                    }
                } else {
                    break;
                }
            }
            _ => {
                let flag = parse_to_u16(&input[0..2]) as u8;
                if flag != 1 {
                    // 不能直接 break：显式区后面可能混入其它块/下一条记录开头，
                    // 按 word 对齐继续向后寻找下一个 0x0001 块头。
                    resync += 1;
                    if resync > MAX_RESYNC {
                        break;
                    }
                    input = &input[4..];
                    continue;
                }
                let len_words = parse_to_u16(&input[2..4]) as usize;
                let declared = len_words * 4;
                if declared < 12 || declared > input.len() {
                    resync += 1;
                    if resync > MAX_RESYNC {
                        break;
                    }
                    input = &input[4..];
                    continue;
                }
                let maybe_refno = RefU64::from(&input[4..12]);
                //必须要检查是否跟的是 refno
                if len_words < 5 || maybe_refno != refno {
                    resync += 1;
                    if resync > MAX_RESYNC {
                        break;
                    }
                    input = &input[4..];
                    continue;
                }
                resync = 0;
                let (rest, mut payload) = collect_explicit_segmented_payload(input, declared, 0x01);
                // 主段可能含 8 字节保留区（不一定全 0）：自适应裁掉，兼容 offset 12/20 两种布局
                if payload.len() >= 8 && payload[..8].iter().all(|&b| b == 0) {
                    let overlap = payload.len() >= 12
                        && bytes.len() >= 4
                        && bytes[bytes.len() - 4..] == payload[8..12];
                    if overlap {
                        payload.drain(..12);
                    } else {
                        payload.drain(..8);
                    }
                } else if payload.len() >= 16 {
                    let ok0 = looks_like_attr_stream_start(&payload);
                    let ok8 = looks_like_attr_stream_start(&payload[8..]);
                    if !ok0 && ok8 {
                        payload.drain(..8);
                    }
                }
                bytes.extend(payload);
                input = rest;
            }
        }
    }
    bytes
}

/// 合并「主段 + 0x07 追加段」的显式块 payload（移植自 pdms-io-fork collect_segmented_payload）。
/// 主段 payload 从 offset 12（flag+len+self_ref 之后）开始；0x07 追加段 payload 从 offset 24 开始。
fn collect_explicit_segmented_payload(
    input: &[u8],
    declared_len_bytes: usize,
    flag: u8,
) -> (&[u8], Vec<u8>) {
    const MEMBERS_BASE_PAYLOAD_OFFSET: usize = 12;
    const SEGMENT_PAYLOAD_OFFSET: usize = 24;
    if declared_len_bytes < MEMBERS_BASE_PAYLOAD_OFFSET || input.len() < declared_len_bytes {
        return (input, Vec::new());
    }
    let mut payload = input[MEMBERS_BASE_PAYLOAD_OFFSET..declared_len_bytes].to_vec();
    let mut cursor = declared_len_bytes;
    while cursor + 6 <= input.len()
        && input[cursor..].starts_with(&[0x00, 0x00, 0x00, 0x07])
        && input[cursor + 4] == 0x00
        && input[cursor + 5] == flag
    {
        if cursor + 8 > input.len() {
            break;
        }
        let seg_len_words = parse_to_u16(&input[cursor + 6..cursor + 8]) as usize;
        let seg_len_bytes = seg_len_words * 4;
        if seg_len_bytes == 0 {
            break;
        }
        // 与旧逻辑一致：总长 = len_bytes + 4（含前导长度字段）
        let seg_end = cursor + seg_len_bytes + 4;
        if seg_end > input.len() {
            break;
        }
        let mut seg_payload_start = cursor + SEGMENT_PAYLOAD_OFFSET;
        if seg_payload_start > seg_end {
            break;
        }
        // 仅 packed 表达式条目会有「重复上一个 word」的续接怪癖；对普通段套用会破坏数据
        if flag == 0x01
            && payload.len() >= 4
            && seg_end - seg_payload_start >= 4
            && payload[payload.len() - 4..] == input[seg_payload_start..seg_payload_start + 4]
            && has_unfinished_packed_expression_entry(&payload)
        {
            seg_payload_start += 4;
        }
        payload.extend_from_slice(&input[seg_payload_start..seg_end]);
        cursor = seg_end;
    }
    (&input[cursor..], payload)
}

/// payload 末尾是否有一个未完成的 packed 表达式条目
/// （word[1] = (dab_type<<26)|len_words；dab_type==7 表示表达式）。
fn has_unfinished_packed_expression_entry(payload: &[u8]) -> bool {
    const PACKED_EXPRESSION_DAB_TYPE: u32 = 7;
    let mut cursor = 0usize;
    while cursor + 8 <= payload.len() {
        let packed_header = u32::from_be_bytes([
            payload[cursor + 4],
            payload[cursor + 5],
            payload[cursor + 6],
            payload[cursor + 7],
        ]);
        let dab_type = packed_header >> 26;
        let payload_len_words = (packed_header & 0x03ff_ffff) as usize;
        let Some(payload_len_bytes) = payload_len_words.checked_mul(4) else {
            return false;
        };
        let Some(total_len) = 8usize.checked_add(payload_len_bytes) else {
            return false;
        };
        if cursor + total_len > payload.len() {
            return dab_type == PACKED_EXPRESSION_DAB_TYPE;
        }
        cursor += total_len;
    }
    false
}

//移除00 00 00 007，保留后面的数据（旧实现：遇不匹配块直接 break；保留作对照/兜底）
#[allow(dead_code)]
pub fn collect_explict_data_legacy(mut input: &[u8], refno: RefU64) -> Vec<u8> {
    let mut has_next = input.len() >= 4 * 3;
    let mut bytes = vec![];
    if !has_next {
        return bytes;
    }
    while has_next && input.len() > 4 {
        //还可能遇到各种page，需要跳过，暂时假定只有遇到INDEX Page的情况
        let v = parse_to_i32(&input[..4]);
        match v {
            0 | 7 => {
                input = &input[4..];
            }
            5 => {
                let page_type = parse_to_i32(&input[4..8]);
                #[cfg(feature = "debug_parse")]
                println!("Found {refno} attr may be in page type {:#4X?}", page_type);
                if page_type == 0xCC47DF && input.len() > 0x2C {
                    //一直找到为0的为止
                    input = &input[0x2C..];
                    while parse_to_i32(&input[0..4]) != 0 {
                        input = &input[4..];
                        if input.len() < 4 {
                            break;
                        }
                    }
                    if input.len() > 4 {
                        input = &input[4..];
                    } else {
                        break;
                    }
                } else {
                    break;
                }
            }
            _ => {
                let flag = parse_to_u16(&input[0..2]) as usize;
                if flag != 1 {
                    break;
                }
                let len = parse_to_u16(&input[2..4]) as usize;
                let maybe_refno = RefU64::from(&input[4..12]);
                //必须要检查是否跟的是 refno
                if len < 5 || len > input.len() || maybe_refno != refno {
                    break;
                }
                if len * 4 > input.len() {
                    bytes.extend(&input[20..]);
                    break;
                }
                bytes.extend(&input[20..len * 4]);
                input = &input[len * 4..];
                has_next = input.len() >= 4 * 3;
            }
        }
    }
    bytes
}

pub fn take_off_007_explicit(mut input: &[u8]) -> &[u8] {
    while input.len() >= 4 {
        let head = &input[..4];
        if head == &[0, 0, 0, 0][..] || head == &[0, 0, 0, 7][..] {
            input = &input[4..];
        }
        if &head[..2] != &[0, 1][..] {
            return input;
        }
        let explicit_len = parse_to_u16(&head[2..4]) as usize;
        let b_007 = &input[explicit_len * 4..explicit_len * 4 + 4];
        if b_007 != &[0, 0, 0, 7] {
            return &input[..explicit_len * 4];
        }
    }
    input
}

/// 从 `pending_refnos` 出发按成员表逐层展开，把每个元素的**有序**成员写进
/// `children_map`。成员顺序是 PDMS 语义的一部分（BRAN 组件次序等），只能取自元素
/// 自己的成员块，不能重排。
fn expand_members_from(
    input: &[u8],
    refno_table_map: &DashMap<RefU64, EleDataEntry>,
    pending_refnos: &mut Vec<RefU64>,
    all_refnos: &mut HashSet<RefU64>,
    children_map: &mut HashMap<RefU64, Vec<RefU64>>,
) {
    while let Some(refno) = pending_refnos.pop() {
        if !all_refnos.insert(refno) {
            continue;
        }
        let Some(pos) = refno_table_map.get(&refno).map(|entry| entry.pos) else {
            continue;
        };
        let membs = parse_ele_membs(&input[pos - 4..]);
        let children = membs
            .iter()
            .filter(|memb| refno_table_map.contains_key(*memb))
            .copied()
            .collect::<Vec<_>>();
        for memb in &membs {
            if !all_refnos.contains(memb) {
                pending_refnos.push(*memb);
            }
        }
        children_map.insert(refno, children);
    }
}

/// 用每条元素记录自带的 owner（记录内偏移 16..24）补回缺失的父子边：owner 的成员表
/// 里没列出该元素时，把它追加到末尾。只追加不删除、不重排，已有成员的次序不受影响。
fn relink_children_by_owner(
    input: &[u8],
    refno_table_map: &DashMap<RefU64, EleDataEntry>,
    children_map: &mut HashMap<RefU64, Vec<RefU64>>,
) {
    let mut linked = HashSet::with_capacity(children_map.values().map(Vec::len).sum());
    for (owner, children) in children_map.iter() {
        for child in children {
            linked.insert((*owner, *child));
        }
    }
    for entry in refno_table_map.iter() {
        let refno = *entry.key();
        let pos = entry.value().pos;
        if pos < 4 || pos + 20 > input.len() {
            continue;
        }
        let owner = RefU64::from(&input[pos + 12..pos + 20]);
        if owner.get_0() == 0 || owner == refno || !refno_table_map.contains_key(&owner) {
            continue;
        }
        if linked.insert((owner, refno)) {
            children_map.entry(owner).or_default().push(refno);
        }
    }
}

///解析db文件的chidlren部分，得到参考号和对应的类型集合
pub fn parse_db_basic_data(input: Vec<u8>, file_name: &str, project: &str) -> Result<DbBasicData> {
    let mut gen_ref_time = Instant::now();
    let (refno_table_map, world_refno) = gen_ref_type_pos_table(&input);
    println!(
        "gen_ref_type_pos_table: {} ms",
        gen_ref_time.elapsed().as_millis()
    );

    let mut root_refno = world_refno;
    let mut children_map = HashMap::new();

    let (refno, children_refnos) = {
        if let Some(entry) = refno_table_map.get(&root_refno) {
            parse_ele_children(&input[entry.pos - 4..])
        } else {
            (root_refno, Default::default())
        }
    };
    let children = children_refnos
        .iter()
        .filter(|&x| refno_table_map.contains_key(x))
        .map(|&x| x)
        .collect::<Vec<_>>();

    children_map.insert(refno, children);

    let mut memb_time = Instant::now();
    let mut pending_refnos = vec![root_refno.clone()];
    let mut all_refnos = HashSet::new();

    expand_members_from(
        &input,
        &refno_table_map,
        &mut pending_refnos,
        &mut all_refnos,
        &mut children_map,
    );

    // 同一元素在库文件里存在多条物理记录，建表选中的那条不保证带成员块。一旦
    // 某个 owner 选中了不带成员的记录，自顶向下的成员展开就在那里断掉；断在
    // WORL 上时整库会静默解析出 0 个元素（TEST 项目的 DESI 即如此：WORL 有 160
    // 条物理记录、157 条带成员，选中的最后一条为空）。所以再补一轮：把表内尚未
    // 访问到的元素也当作根展开，各自的成员顺序仍由自己的记录决定；随后按记录
    // 自带的 owner 把断链处缺失的父子边补回去。
    let unreached = refno_table_map
        .iter()
        .map(|entry| *entry.key())
        .filter(|refno| !all_refnos.contains(refno))
        .collect::<Vec<_>>();
    if !unreached.is_empty() {
        pending_refnos.extend(unreached);
        expand_members_from(
            &input,
            &refno_table_map,
            &mut pending_refnos,
            &mut all_refnos,
            &mut children_map,
        );
        relink_children_by_owner(&input, &refno_table_map, &mut children_map);
    }
    println!(
        "Parsing children members cost: {} ms",
        memb_time.elapsed().as_millis()
    );
    println!("All refnos count: {}", all_refnos.len());

    let DbBasicInfo {
        db_type,
        ses_pgno,
        db_no,
    } = parse_file_basic_info(&input);

    Ok(DbBasicData {
        ses_pgno,
        bytes: input,
        world_refno,
        refno_table_map,
        children_map,
    })
}

#[inline]
fn get_sesno(ses_range_map: &BTreeMap<i32, Range<u32>>, pgno: u32) -> Option<i32> {
    for (sesno, range) in ses_range_map {
        if range.contains(&pgno) {
            return Some(*sesno);
        }
    }
    None
}

///解析db文件，因为有可能db文件会很大，所以需要做一个分段运行的策略
pub async fn parse_db_with_chunk(
    db_basic_data: Arc<DbBasicData>,
    filename: &str,
    project: &str,
    chunk_refnos: &[RefU64],
    ses_range_map: &BTreeMap<i32, Range<u32>>,
    ignore_world_refno: bool,
) -> Result<PdmsDbData> {
    let db_info = get_default_pdms_db_info();
    parse_db_with_chunk_with_info(
        db_basic_data,
        &db_info,
        filename,
        project,
        chunk_refnos,
        ses_range_map,
        ignore_world_refno,
    )
    .await
}

///解析db文件，因为有可能db文件会很大，所以需要做一个分段运行的策略（显式指定数据库信息）
pub async fn parse_db_with_chunk_with_info(
    db_basic_data: Arc<DbBasicData>,
    database_info: &PdmsDatabaseInfo,
    filename: &str,
    project: &str,
    chunk_refnos: &[RefU64],
    ses_range_map: &BTreeMap<i32, Range<u32>>,
    ignore_world_refno: bool,
) -> Result<PdmsDbData> {
    let input = &db_basic_data.bytes;
    let type_ele_map = Arc::new(DashMap::new());
    let total_att_map: Arc<DashMap<RefU64, NamedAttrMap>> = Arc::new(DashMap::new());
    let mut field_no = 0;
    let test_refno = get_db_option().get_test_refno().map(|x| x.into());

    let DbBasicInfo {
        db_type,
        ses_pgno,
        db_no,
    } = parse_file_basic_info(input);
    let db_no_str = db_no.to_string();
    if db_type.as_str() != "SYST" && !filename.contains(&db_no_str) {
        let _chars_len = db_no_str.len();
        let l = filename.len();
        let end = filename.chars().position(|x| x == '_').unwrap_or(l);
        if end < project.len() {
            return Err(anyhow!("Not a valid db file"));
        }
        field_no = filename[project.len()..end]
            .parse::<u32>()
            .unwrap_or_default();
    }

    let root_refno = db_basic_data.world_refno;
    let _refno_info_map = Arc::new(DashMap::new());
    let mut children_map: HashMap<RefU64, RefU64Vec> = db_basic_data
        .children_map
        .iter()
        .map(|(k, v)| (*k, RefU64Vec(v.clone())))
        .collect();
    // 只保留当前分块相关的 children，避免超大 children_map 占用
    if !chunk_refnos.is_empty() {
        let keep_set: HashSet<RefU64> = chunk_refnos.iter().cloned().collect();
        children_map
            .retain(|k, _| keep_set.contains(k) || (!ignore_world_refno && *k == root_refno));
        for (_, v) in children_map.iter_mut() {
            v.retain(|child| {
                keep_set.contains(child) || (!ignore_world_refno && *child == root_refno)
            });
        }
    }
    //如果忽略world_refno，则不解析world_refno的数据
    // dbg!(&ignore_world_refno);
    if !ignore_world_refno {
        let entry = &*db_basic_data
            .refno_table_map
            .get(&root_refno)
            .ok_or(anyhow!("Not found refno in entry"))?;

        let mut pgno = entry.pos / 0x800;
        let mut sesno = get_sesno(&ses_range_map, pgno as _).unwrap_or_default();
        let EleData {
            refno,
            noun,
            whole_attmap,
            ..
        } = parse_ele_data_with_info(&input[entry.pos - 4..], database_info)
            .await
            .unwrap_or_default();
        let mut named_attmap: NamedAttrMap = whole_attmap.merge().into();
        named_attmap.set_sesno(sesno as _);

        dbg!(&refno);
        total_att_map.insert(refno, named_attmap);
        type_ele_map
            .entry(noun)
            .or_insert(HashSet::default())
            .insert(refno);
        let ref_0 = refno.get_0();
        _refno_info_map
            .entry(ref_0)
            .or_insert(RefnoInfo { ref_0, db_no });
    }

    for source_refno in chunk_refnos.iter() {
        let mut is_debug = test_refno.is_some() && test_refno == Some(*source_refno);
        if let Some(entry) = db_basic_data.refno_table_map.get(source_refno) {
            let pos = entry.pos;
            let total_attmap_clone = total_att_map.clone();
            let type_ele_map = type_ele_map.clone();
            let pgno = entry.pos / 0x800;
            if let Ok(EleData {
                refno,
                noun,
                whole_attmap,
                ..
            }) = parse_ele_data_with_info(&input[pos - 4..], database_info).await
            {
                let sesno = get_sesno(&ses_range_map, pgno as _).unwrap_or_default();
                let mut named_attmap: NamedAttrMap = whole_attmap.merge().into();
                named_attmap.set_sesno(sesno as _);
                //页数就是所在的位置除以0x800
                total_attmap_clone.insert(refno, named_attmap);
                type_ele_map
                    .entry(noun)
                    .or_insert(HashSet::default())
                    .insert(refno);
            } else {
                if is_debug {
                    println!(
                        "parse ele data failed: {:?}, loc: {:#4X}",
                        source_refno, pos
                    );
                }
            }
        }
    }

    Ok(PdmsDbData {
        type_ele_map: Arc::try_unwrap(type_ele_map).unwrap(),
        total_attr_map: Arc::try_unwrap(total_att_map).unwrap(),
        children_map,
        filename: filename.into(),
        ses_pgno,
        db_type,
        db_name: Default::default(),
        db_no,
        field_no,
    })
}

///解析db文件，因为有可能db文件会很大，所以需要做一个分段运行的策略
pub async fn parse_db(
    input: &[u8],
    database_info: &PdmsDatabaseInfo,
    file_name: &str,
    project: &str,
) -> Result<PdmsDbData> {
    let mut type_ele_map = Arc::new(DashMap::new());
    let mut total_attr_map: Arc<DashMap<RefU64, NamedAttrMap>> = Arc::new(DashMap::new());
    let mut field_no = 0;

    let DbBasicInfo {
        db_type,
        ses_pgno,
        mut db_no,
    } = parse_file_basic_info(input);
    dbg!(&(db_type.as_str(), ses_pgno, db_no, file_name));
    let db_no_str = db_no.to_string();
    if db_type.as_str() != "SYST" && !file_name.contains(&db_no_str) {
        let _chars_len = db_no_str.len();
        let l = file_name.len();
        let end = file_name.chars().position(|x| x == '_').unwrap_or(l);
        if end < project.len() {
            return Err(anyhow!("Not a valid db file"));
        }
        field_no = file_name[project.len()..end]
            .parse::<u32>()
            .unwrap_or_default();
    }

    let mut gen_ref_time = Instant::now();
    let (refno_table_map, world_refno) = gen_ref_type_pos_table(input);
    println!(
        "gen_ref_type_pos_table: {} ms",
        gen_ref_time.elapsed().as_millis()
    );

    let mut root_refno = world_refno;
    let mut refno_info_map = Arc::new(DashMap::new());
    let mut children_map = HashMap::new();
    let entry = &*refno_table_map
        .get(&root_refno)
        .ok_or(anyhow!("Not found refno in entry"))?;

    let EleData {
        refno,
        owner,
        noun,
        whole_attmap,
        children,
        name,
    } = parse_ele_data_with_info(&input[entry.pos - 4..], database_info).await?;

    total_attr_map.insert(refno, whole_attmap.merge().into());
    type_ele_map
        .entry(noun)
        .or_insert(HashSet::default())
        .insert(refno);
    let ref_0 = refno.get_0();
    refno_info_map
        .entry(ref_0)
        .or_insert(RefnoInfo { ref_0, db_no });
    if children.len() > 0 {
        children_map.insert(refno, children.clone());
    }

    let memb_time = Instant::now();
    let mut pending_refnos = vec![root_refno.clone()];
    let mut all_refnos = HashSet::new();

    while !pending_refnos.is_empty() {
        let refno = pending_refnos.pop().unwrap();
        if all_refnos.contains(&refno) {
            continue;
        }
        all_refnos.insert(refno);
        if refno_table_map.contains_key(&refno) {
            let entry = &*refno_table_map.get(&refno).unwrap();
            dbg!(entry);
            let pos = entry.pos;
            //解析到members数据
            let membs = parse_ele_membs(&input[pos - 4..]);
            for memb in &membs {
                if !all_refnos.contains(&memb) {
                    pending_refnos.push(*memb);
                }
            }
            children_map.insert(refno, RefU64Vec(membs));
        }
    }
    println!(
        "Parsing children members cost: {} ms",
        memb_time.elapsed().as_millis()
    );
    println!("All refnos count: {}", all_refnos.len());
    let _eles_time = Instant::now();
    println!("Begin parse attributes");
    // all_refnos.iter().for_each(|refno| {
    for refno in all_refnos {
        if refno_table_map.contains_key(&refno) {
            let entry = &*refno_table_map.get(&refno).unwrap();
            let pos = entry.pos;
            let whole_attr_dashmap = total_attr_map.clone();
            let type_ele_map = type_ele_map.clone();
            if let Ok(EleData {
                refno,
                noun,
                whole_attmap,
                ..
            }) = parse_ele_data_with_info(&input[pos - 4..], database_info).await
            {
                whole_attr_dashmap.insert(refno, whole_attmap.merge().into());
                type_ele_map
                    .entry(noun)
                    .or_insert(HashSet::default())
                    .insert(refno);
            }
        }
    }
    // println!("解析属性所耗时间: {:?} ms", eles_time.elapsed().as_millis());
    // println!("带有外键属性的参考号个数为 {}", foreign_refnos_map.len());
    // println!("DB {} attrs count: {}", file_name, total_attr_map.len());
    // println!(
    //     "解析db: {} 所耗时间: {:?}ms",
    //     file_name,
    //     time_start.elapsed().as_millis()
    // );

    Ok(PdmsDbData {
        type_ele_map: Arc::try_unwrap(type_ele_map).unwrap(),
        total_attr_map: Arc::try_unwrap(total_attr_map).unwrap(),
        children_map,
        filename: file_name.into(),
        ses_pgno,
        db_type,
        db_name: Default::default(),
        db_no,
        field_no,
    })
}

/// 获取隐式属性, input为分段数据，已经限制了长度
#[inline]
pub fn parse_implicit_attr_value<'a>(
    origin_bytes: &'a [u8],
    attr_info: &'a AttrInfo,
    f32_flag: bool,
    f32_neg_offset: usize,
    step: usize, //dword 即 4字节数量
) -> IResult<&'a [u8], AttrVal> {
    let mut val = InvalidType;
    let b_expr = check_is_expr(attr_info.hash);
    // dbg!(attr_info);
    let offset = ((attr_info.offset & 0xFFFF) as usize - f32_neg_offset) * 4;
    // if attr_info.name.as_str() == "BANG" || attr_info.name.as_str() == "DRNS"{
    //     dbg!(offset);
    // }
    if offset > origin_bytes.len() {
        return Err(nom::Err::Error(nom::error::make_error(
            origin_bytes,
            ErrorKind::Eof,
        )));
    }
    let bytes = &origin_bytes[offset..];
    // println!("{}", pretty_hex::pretty_hex(&bytes));
    if b_expr {
        // dbg!(attr_info);
        //既然当作表达式，而且又在隐含属性里，这里需要把bytes的长度锁定
        let (_, string_val) =
            parse_to_expression(&bytes[0..step * 4], attr_info.default_val.clone())?;
        val = string_val.clone();
        match attr_info.default_val {
            IntegerType(_) => {
                if let AttrVal::StringType(s) = string_val
                    && let Ok(v) = s.parse::<i32>()
                {
                    val = IntegerType(v);
                }
            }
            DoubleType(_) => {
                if let AttrVal::StringType(s) = string_val
                    && let Ok(v) = s.parse::<f64>()
                {
                    val = DoubleType(v);
                }
            }
            _ => {}
        }
    } else {
        // 隐式属性LEVEL 需要做特殊处理 map给定的是IntegerType 但其实是Vec<Int>
        if attr_info.hash == ATT_LEVE || attr_info.hash == ATT_PTS {
            let (bytes, len) = be_u32(bytes)?;
            let (_, result) = count(be_i32, len as usize)(bytes)?;
            val = IntArrayType(result);
        } else {
            match attr_info.default_val {
                IntegerType(_) => {
                    let (_, r) = be_i32(bytes)?;
                    val = IntegerType(r);
                }
                DoubleType(_) => {
                    if f32_flag || step == 1 {
                        if bytes.len() >= 4 {
                            let d = parse_to_f32(&bytes[..4]) as f64;
                            val = DoubleType(d as _);
                        } else {
                            dbg!(step);
                            dbg!(f32_flag);
                            dbg!(f32_neg_offset);
                            dbg!(attr_info);
                            println!("parse double 有问题的数据：{:#04X?}", origin_bytes);
                        }
                    } else {
                        if bytes.len() >= 8 {
                            let d = parse_to_f64(&bytes[..8]);
                            if d > f32::MAX as f64 {
                                val = DoubleType(0.0);
                            } else {
                                val = DoubleType(d);
                            }
                        } else {
                            dbg!(step);
                            dbg!(f32_flag);
                            dbg!(attr_info);
                        }
                    }
                }
                BoolType(_) => {
                    //直接在定位的byte上执行
                    let o = (attr_info.offset >> 0x14) as usize;
                    let (_, r) = be_u32(bytes)?;
                    let result = r >> o & 1;
                    val = BoolType(result == 1);
                }
                StringType(_) => {
                    let (_, str_len) = be_i32(bytes)?;
                    let str_len = str_len as usize;
                    //有些应该是没有分清类型的数据？
                    if step == 1 && str_len != 0 {
                        if &bytes[..2] == &[0, 0] || &bytes[..2] == &[0xFF, 0xFF] {
                            let d = parse_to_i32(&bytes[..4]);
                            val = IntegerType(d);
                        } else {
                            let d = parse_to_f32(&bytes[..4]) as f64;
                            val = DoubleType(d);
                        }
                    } else if str_len < bytes.len() && bytes.len() >= 4 {
                        if str_len + 4 <= bytes.len() {
                            let (decode_string, _b_chi) = decode_chars_data(&bytes[4..str_len + 4]);
                            val = StringType(decode_string.into());
                        }
                    } else {
                        val = StringType("".into());
                    }
                }
                ElementType(_) | RefU64Type(_) => {
                    let (_, (ref_0, ref_1)) = tuple((be_u32, be_u32))(bytes)?;
                    if ref_0 == 0 {
                        val = RefU64Type(Default::default());
                    } else {
                        val = RefU64Type(RefU64::from_two_nums(ref_0, ref_1));
                    }
                }
                WordType(_) => {
                    let (_, v) = be_i32(bytes)?;
                    if v > 0x81BF1 {
                        val = WordType(db1_dehash(v as u32).into());
                    } else {
                        val = IntegerType(v);
                    }
                }
                //todo need to find if exist invalid data
                Vec3Type(_) => {
                    // let mut data = [0f64; 3];
                    let (l, cnt) = be_i32(bytes)?;
                    let data_len = l.len() / 4; //WORD个数
                    if f32_flag {
                        if data_len >= 3 {
                            let data = parse_to_f32_arr(l, 3).try_into().unwrap();
                            val = Vec3Type(data);
                        } else {
                            dbg!(step);
                            dbg!(data_len);
                            dbg!(f32_flag);
                            dbg!(cnt);
                            dbg!(attr_info);
                            println!("parse vec3 有问题的数据：{:#04X?}", origin_bytes);
                        }
                    } else {
                        if data_len >= 6 {
                            let data = parse_to_f64_arr(l, 3).try_into().unwrap();
                            val = Vec3Type(data);
                        } else {
                            dbg!(step);
                            dbg!(data_len);
                            dbg!(f32_flag);
                            dbg!(cnt);
                            dbg!(attr_info);
                        }
                    }
                }
                DoubleArrayType(_) => {
                    let (l, cnt) = be_i32(bytes)?;
                    let data = parse_to_f32_arr(l, cnt as _);
                    val = DoubleArrayType(data);
                }
                // DbAttributeType::DATETIME => {}
                _ => {}
            }
        }
    }
    // #[cfg(debug_assertions)]
    // dbg!(&val);
    Ok((origin_bytes, val))
}

///获得uda 名称
async fn get_uda_full_name(hash: i32) -> Option<String> {
    if let Ok(mut response) = SUL_DB
        .query("select value UDNA from only UDA where UKEY=$key limit 1")
        .bind(("key", hash))
        .await
    {
        let name: Option<String> = response.take(0).unwrap();
        return name;
    }
    None
}

async fn get_uda_short_name(hash: i32) -> Option<String> {
    get_uda_full_name(hash).await.map(|x| {
        if x.len() < 4 {
            x.to_uppercase()
        } else {
            x[..4].to_uppercase()
        }
    })
}

/// 获取已知显式属性
pub async fn process_explicit_attrs(
    uda_attrs: Vec<ExplicitAttr>,
    attr_data_map: &mut NamedAttrMap,
) -> anyhow::Result<()> {
    // 处理解析结果，包括UDA的异步处理
    for attr in uda_attrs {
        if attr.is_uda {
            // UDA 异步处理
            if let Some(uda_refno) = aios_core::get_uda_refno(attr.hash_val).await {
                //要加UDA:表达区分
                attr_data_map.insert(format!("UDA:{uda_refno}"), attr.value.into());
            }
        } else {
            //覆盖可能在隐含属性里出现过的数据
            attr_data_map.insert(attr.name.clone(), attr.value.into());
        }
    }

    Ok(())
}

/// 获取已知显式属性的原始数据解析（不包含UDA异步处理）
pub fn parse_raw_explicit_attrs<'a>(
    input: &'a [u8],
    attr_info_map: &DashMap<String, AttrInfo>,
    refno: RefU64,
) -> IResult<&'a [u8], Vec<ExplicitAttr>> {
    let mut residual = input;
    let test_refno = get_db_option().get_test_refno().map(|x| x.refno());
    let mut is_debug = test_refno == Some(refno);
    let mut attr_values = Vec::new();

    while !residual.is_empty() {
        let mut att_value = None;
        let hash_val = convert_to_hash(&residual[..4]);
        if hash_val == 0 {
            break;
        }
        let is_uda = is_uda(hash_val);
        let att_name = if is_uda {
            //UDA 单独处理
            "_UDAS".into()
        } else {
            db1_dehash(hash_val.abs() as _)
        };
        // println!("hex value is {:#4X?}, att name is {}", &residual[..4], &att_name);
        if is_debug {
            if is_uda {
                dbg!(&att_name);
            }
        }
        if check_is_expr(hash_val) {
            // dbg!(&att_name);
            match parse_expression_attr(residual, refno) {
                Ok((input, (_, value))) => {
                    if value.is_empty() {
                        att_value = None;
                    } else {
                        att_value = Some(StringType(value));
                    }
                    if is_debug {
                        dbg!(&att_value);
                    }
                    residual = input;
                }
                Err(e) => {
                    println!(
                        "解析{} 表达式属性退出: {:?}, {:#4X?}",
                        refno.to_e3d_id(),
                        &att_name,
                        &residual[..]
                    );
                    break;
                }
            }
        } else {
            let (l, (explict_hash, attr_type_num, type_len)) = match tuple((
                be_i32::<_, nom::error::Error<_>>,
                be_u16, //属性的类型
                be_u16, //属性的长度
            ))(&residual[..])
            {
                Ok(result) => result,
                Err(e) => {
                    println!(
                        "解析{} 显式属性退出: {:?}, {:#4X?}",
                        refno.to_e3d_id(),
                        &att_name,
                        &residual[..]
                    );
                    if att_name == "CURD" || att_name == "MDB" {
                        let off = input.len() - residual.len();
                        println!(
                            "[CURD_DUMP] refno={} att={} 失败偏移={} 全长={} 全量bytes={:#04X?}",
                            refno.to_e3d_id(),
                            &att_name,
                            off,
                            input.len(),
                            input
                        );
                    }
                    break;
                }
            };
            let type_len = type_len as usize;
            if refno.to_e3d_id().contains("24575/1309") {
                println!(
                    "[ATTR] off={} name={:?} hash={:#X} type={:#X} type_len={} in_map={}",
                    input.len() - residual.len(),
                    &att_name,
                    explict_hash,
                    attr_type_num,
                    type_len,
                    attr_info_map.contains_key(&att_name)
                );
            }
            if type_len * 4 <= l.len() {
                residual = &l[type_len * 4..];
                // 显式属性有可能他给了type但是超了01 后面得长度 所以还要做一层判断
                let tmp_input = &l[..type_len * 4];
                // println!("{:#4X}", explict_hash);
                // dbg!(db1_dehash(explict_hash as u32));
                if attr_info_map.contains_key(&att_name) {
                    let attr_info = attr_info_map.get_mut(&att_name).unwrap();
                    // dbg!(&attr_info.value());
                    // 根据获取到的type hash值，拿到需要的类型
                    match attr_info.default_val {
                        InvalidType => {}
                        IntegerType(_) => {
                            let (_, val) = be_i32(tmp_input)?;
                            att_value = Some(IntegerType(val));
                        }
                        StringType(_) => {
                            let (_, a) = be_u32(tmp_input)?;
                            let len_a = a as usize;
                            if tmp_input.len() > 4 && 4 + len_a <= tmp_input.len() {
                                let (decode_string, _b_chi) =
                                    decode_chars_data(&tmp_input[4..4 + len_a]);
                                att_value = Some(StringType(decode_string.into()));
                            } else {
                                // println!("len_a={:#04X?}", len_a);
                                // println!("error refno={:?}", refno);
                                // println!("error 显示 tmp_input={:#04X?}", tmp_input);
                            }
                        }
                        DoubleType(_) => {
                            let dou_len = tmp_input.len() / 4;
                            if dou_len == 1 {
                                let (_, val) = be_i32(tmp_input)?;
                                att_value = Some(IntegerType(val));
                            } else {
                                let val = parse_to_f64(&tmp_input[..8]);
                                att_value = Some(DoubleType(val));
                            }
                        }
                        DoubleArrayType(_) => {
                            let mut bytes_len = tmp_input.len() / 4;
                            if bytes_len >= 3 {
                                bytes_len -= 1; //去掉一个自身
                                let (tmp_input, data_len) = be_i32(tmp_input)?;
                                let len = data_len as usize;
                                let double_or_float = bytes_len / len;
                                let mut tmp_input = tmp_input;

                                if double_or_float == 2 {
                                    if tmp_input.len() >= 8 {
                                        let mut data = vec![];
                                        for _ in 0..len {
                                            data.push(parse_to_f64(&tmp_input[..8]));
                                            tmp_input = &tmp_input[8..];
                                        }
                                        att_value = Some(DoubleArrayType(data));
                                    } else {
                                        att_value = Some(DoubleArrayType(vec![]));
                                    }
                                } else if double_or_float == 1 {
                                    if tmp_input.len() > 4 {
                                        let mut data = vec![];
                                        for _ in 0..len {
                                            data.push(parse_to_f32(&tmp_input[..4]) as f64);
                                            tmp_input = &tmp_input[4..];
                                        }
                                        att_value = Some(DoubleArrayType(data));
                                    } else {
                                        att_value = Some(DoubleArrayType(vec![]));
                                    }
                                }
                            }
                        }
                        StringArrayType(_) => {
                            let (tmp_input, len) = be_u32(tmp_input)?;
                            let len = len as usize;
                            let mut tmp_input = tmp_input;
                            let mut dehash_strs = vec![];
                            for _ in 0..len {
                                let (remain_input, val) = be_i32(tmp_input)?;
                                dehash_strs.push(db1_dehash(val.abs() as _));
                                tmp_input = remain_input;
                            }
                            att_value = Some(StringArrayType(dehash_strs));
                            // if is_debug && att_name == "ELEL" {
                            //     dbg!(&att_value);
                            // }
                        }
                        BoolArrayType(_) => {}
                        IntArrayType(_) => {
                            let (tmp_input, len) = be_u32(tmp_input)?;
                            let len = len as usize;
                            let mut tmp_input = tmp_input;
                            let mut data = vec![];
                            for _ in 0..len {
                                let (remain_input, val) = be_i32(tmp_input)?;
                                data.push(val);
                                tmp_input = remain_input;
                            }
                            att_value = Some(IntArrayType(data));
                        }
                        BoolType(_) => {
                            let (_, val) = be_u32(tmp_input)?;
                            att_value = Some(BoolType(val != 0));
                        }
                        Vec3Type(_) => {
                            let (l, v) = be_i32(tmp_input)?;
                            let _len = v as usize;
                            let data = parse_to_f64_arr(l, 3).try_into().unwrap();
                            att_value = Some(Vec3Type(data));
                        }
                        ElementType(_) => {
                            let (_, (ref_0, ref_1)) = tuple((be_u32, be_u32))(tmp_input)?;
                            let refno = RefU64::from_two_nums(ref_0, ref_1);
                            att_value = Some(RefU64Type(refno));
                        }
                        WordType(_) => {
                            let (tmp_bytes, val) = be_i32(tmp_input)?;
                            if val >= 0x81BF1 {
                                let val_word = db1_dehash(val as u32);
                                att_value = Some(WordType(val_word.into()));
                            } else if val == 1 {
                                //如果为1时，有个长度信息, 特别是TYPEX
                                let (_, val) = be_i32(tmp_bytes)?;
                                let val_word = db1_dehash(val as u32);
                                att_value = Some(WordType(val_word.into()));
                            }
                        }
                        RefU64Type(_) => {
                            let (_, (ref_0, ref_1)) = tuple((be_u32, be_u32))(tmp_input)?;
                            let refno = RefU64::from_two_nums(ref_0, ref_1);
                            att_value = Some(RefU64Type(refno));
                        }
                        StringHashType(_) => {}
                        RefU64Array(_) => {
                            let (tmp_input, len) = be_u32(tmp_input)?;
                            let len = len as usize;
                            let mut tmp_input = tmp_input;
                            let mut data = vec![];
                            for _ in 0..len {
                                let (remain_input, val) = be_u64(tmp_input)?;
                                data.push(RefU64(val));
                                tmp_input = remain_input;
                            }
                            att_value = Some(RefU64Array(RefU64Vec(data)));
                        }
                    }
                } else {
                    // 这里的逻辑改了一下，先判断是否为表达式，所以之前在这里的表达式判断就注释掉了
                    // 如果DashMap没有对应属性的hash 则调用get_explicit_attr_type进行解析
                    if let Some(attr_type) = get_explicit_attr_type(attr_type_num) {
                        // 根据获取到的type hash值，拿到需要的类型
                        match attr_type {
                            DbAttributeType::INTEGER => {
                                let (_, val) = be_i32(tmp_input)?;
                                att_value = Some(IntegerType(val));
                            }
                            DbAttributeType::DOUBLE => {
                                if tmp_input.len() >= 8 {
                                    let val = parse_to_f64(&tmp_input[..8]);
                                    att_value = Some(DoubleType(val));
                                } else {
                                    let (_, val) = be_i32(tmp_input)?;
                                    att_value = Some(IntegerType(val));
                                }
                            }
                            DbAttributeType::BOOL => {
                                let (_, val) = be_u32(tmp_input)?;
                                att_value = Some(BoolType(val != 0));
                            }
                            DbAttributeType::STRING => {
                                let (_, a) = be_u32(tmp_input)?;
                                let len_a = a as usize;
                                if tmp_input.len() > 4 {
                                    let (decode_string, _b_chi) =
                                        decode_chars_data(&tmp_input[4..4 + len_a]);
                                    // let name_hash = string_lookup.add_str(decode_string.as_str());
                                    // att_value = Some(StringHashType(name_hash));
                                    att_value = Some(StringType(decode_string.into()));
                                } else {
                                    println!("len_a={:#04X?}", len_a);
                                    println!("error refno={:?}", refno);
                                    println!("error 显示 input={:#04X?}", tmp_input);
                                }
                            }
                            DbAttributeType::ELEMENT => {
                                let (_, (ref_0, ref_1)) = tuple((be_u32, be_u32))(tmp_input)?;
                                let refno = RefU64::from_two_nums(ref_0, ref_1);
                                att_value = Some(RefU64Type(refno));
                            }
                            DbAttributeType::WORD => {
                                let (_, val) = be_i32(tmp_input)?;
                                if val >= 0x81BF1 {
                                    let val_word = db1_dehash(val as u32);
                                    att_value = Some(WordType(val_word.into()));
                                } else {
                                    att_value = Some(IntegerType(val));
                                }
                            }
                            DbAttributeType::DIRECTION
                            | DbAttributeType::POSITION
                            | DbAttributeType::ORIENTATION => {
                                let (l, v) = be_i32(tmp_input)?;
                                let _len = v as usize;
                                let data = parse_to_f64_arr(l, 3).try_into().unwrap();
                                att_value = Some(Vec3Type(data));
                            }

                            DbAttributeType::DOUBLEVEC => {
                                let array_len = tmp_input.len() / 4;
                                if array_len >= 2 {
                                    let (mut tmp_input, data_len) = be_i32(tmp_input)?;
                                    let len = data_len as usize;
                                    let double_or_float = (array_len - 1) / len;
                                    if double_or_float == 2 {
                                        let mut data = vec![];
                                        for _ in 0..len {
                                            data.push(parse_to_f64(&tmp_input[..8]));
                                            tmp_input = &tmp_input[8..];
                                        }
                                        att_value = Some(DoubleArrayType(data));
                                    } else if double_or_float == 1 {
                                        let mut data = vec![];
                                        for _ in 0..len {
                                            data.push(parse_to_f32(&tmp_input[..4]) as f64);
                                            tmp_input = &tmp_input[4..];
                                        }
                                        att_value = Some(DoubleArrayType(data));
                                    }
                                }
                            }
                            DbAttributeType::INTVEC => {
                                let (tmp_input, len) = be_u32(tmp_input)?;
                                let len = len as usize;
                                let mut tmp_input = tmp_input;
                                let mut data = vec![];
                                for _ in 0..len {
                                    let (remain_input, val) = be_i32(tmp_input)?;
                                    data.push(val);
                                    tmp_input = remain_input;
                                }
                                att_value = Some(IntArrayType(data));
                            }
                            DbAttributeType::TYPEX => {
                                let (tmp_input, len) = be_u32(tmp_input)?;
                                if len == 1 {
                                    let (_, typex) = be_u32(&tmp_input[..4])?;
                                    // let typex = db1_dehash(typex);
                                    att_value = Some(IntegerType(typex as _));
                                } else {
                                }
                            }
                            DbAttributeType::RefU64Vec => {
                                let (tmp_input, len) = be_u32(tmp_input)?;
                                let len = len as usize;
                                let mut tmp_input = tmp_input;
                                let mut data = vec![];
                                for _ in 0..len {
                                    let (remain_input, val) = be_u64(tmp_input)?;
                                    data.push(RefU64(val));
                                    tmp_input = remain_input;
                                }
                                att_value = Some(RefU64Array(RefU64Vec(data)));
                            }
                            _ => {}
                        }
                    }
                }
            } else {
                break;
            }
        }

        // 如果解析出属性值，添加到结果中，并保存原始hash_val
        if let Some(v) = att_value {
            attr_values.push(ExplicitAttr {
                name: att_name,
                value: v.into(),
                is_uda,
                hash_val,
            });
        }
    }

    Ok((residual, attr_values))
}

fn parse_param_with_index(input: i32) -> String {
    let mut val = String::new();
    if input >= 50 && input < 0x65 {
        let value = input - 50;
        val = format!("DESIGN PARAM {}", value);
    } else if input >= 0x65 && input < 0x1F5 {
        let value = ((((input - 0x64) as f32 + 0.005) * 100.0).round() / 100.0) as i32;
        val = format!("IPARAM {}", value);
    } else if input >= 0x1F5 {
        let value = input - 0x1F4;
        match value {
            0..50 => {
                val = format!("TWICE PARAM {}", value);
            }
            50..0x65 => {
                val = format!("TWICE DESIGN PARAM {}", value - 50);
            }
            0x65..0x1F5 => {
                let value = ((((input - 0x64) as f32 + 0.005) * 100.0).round() / 100.0) as i32;
                val = format!("IPARAM {}", value);
            }
            _ => {}
        }
    } else if input <= 0xFFFFFFFFu32 as i32 {
        val = match_angle_or_return_number(input);
    } else {
        val = format!("PARAM {}", input);
    }
    val
}

/// 获取所有的members
#[inline]
pub fn parse_attr_members(input: &[u8]) -> IResult<&[u8], RefU64Vec> {
    let mut members = RefU64Vec::default();
    let mut residual = input;
    while residual.len() > 4 {
        let (l, v) = be_u64(residual)?;
        residual = l;
        members.push(RefU64(v));
    }
    Ok((input, members))
}

/// 获取该节点的owner
pub fn parse_attr_owner(input: &[u8]) -> IResult<&[u8], String> {
    let (_, (owner0, owner1)) = tuple((be_i32, be_i32))(input)?;
    let owner = RefI32Tuple::new(owner0, owner1).into();
    Ok((input, owner))
}

#[inline]
pub fn round_f32(input: f32) -> f32 {
    (input * 100.0).round() / 100.0
}

#[inline]
fn convert_int_to_axis_str(n: i32) -> &'static str {
    match n {
        1 => "X",
        2 => "Y",
        3 => "Z",
        4 => "-X",
        5 => "-Y",
        6 => "-Z",
        _ => "",
    }
}

/// 特殊处理AXIS隐式属性
pub fn parse_to_expression(input: &[u8], default: AttrVal) -> IResult<&[u8], AttrVal> {
    let (res_input, flag) = be_i32(input)?;
    let cnt = input.len() / 4;
    if cnt == 1 {
        let f = flag as f32 / 100.0;
        return Ok((input, StringType(f.to_string())));
    }
    // if cnt < 3 {
    //     return Err(nom::Err::Incomplete(nom::Needed::Unknown));
    // }
    let mut val = default;
    // dbg!(flag);
    if flag == 2 {
        match &res_input[..8] {
            &[0x0, 0x0, 0x0, 0x1, 0x0, 0x0, 0x0, 0x1] => val = AttrVal::StringType("X".into()),
            &[0x0, 0x0, 0x0, 0x1, 0x0, 0x0, 0x0, 0x2] => val = AttrVal::StringType("Y".into()),
            &[0x0, 0x0, 0x0, 0x1, 0x0, 0x0, 0x0, 0x3] => val = AttrVal::StringType("Z".into()),
            &[0x0, 0x0, 0x0, 0x2, 0x0, 0x0, 0x0, 0x1] => val = AttrVal::StringType("-X".into()),
            &[0x0, 0x0, 0x0, 0x2, 0x0, 0x0, 0x0, 0x2] => val = AttrVal::StringType("-Y".into()),
            &[0x0, 0x0, 0x0, 0x2, 0x0, 0x0, 0x0, 0x3] => val = AttrVal::StringType("-Z".into()),

            &[0x0, 0x0, 0x0, 0x3, 0x0, 0x0, 0x0, 0x0] => val = AttrVal::StringType("P0".into()),
            &[0x0, 0x0, 0x0, 0x3, 0x0, 0x0, 0x0, 0x1] => val = AttrVal::StringType("P1".into()),
            &[0x0, 0x0, 0x0, 0x3, 0x0, 0x0, 0x0, 0x2] => val = AttrVal::StringType("P2".into()),
            &[0x0, 0x0, 0x0, 0x3, 0x0, 0x0, 0x3, 0xE8] => val = AttrVal::StringType("-P0".into()),
            &[0x0, 0x0, 0x0, 0x3, 0x0, 0x0, 0x3, 0xE9] => val = AttrVal::StringType("-P1".into()),
            &[0x0, 0x0, 0x0, 0x3, 0x0, 0x0, 0x3, 0xEA] => val = AttrVal::StringType("-P2".into()),

            &_ => {
                match &res_input[..3] {
                    &[0xFF, 0xFF, 0xFF] => {
                        let (_, radius) = be_i32(&res_input[4..8])?;
                        let radius = radius / 100;
                        let mut result = String::new();
                        match &res_input[3..4] {
                            &[0xF4] => {
                                result = format!("X {} Y", radius);
                            }
                            &[0xF3] => {
                                result = format!("X {} Z", radius);
                            }
                            &[0xF1] => {
                                result = format!("X {} -Y", radius);
                            }
                            &[0xF0] => {
                                result = format!("X {} -Z", radius);
                            }
                            &[0xEB] => {
                                result = format!("Y {} X", radius);
                            }
                            &[0xE9] => {
                                result = format!("Y {} Z", radius);
                            }
                            &[0xE8] => {
                                result = format!("Y {} -X", radius);
                            }
                            &[0xE6] => {
                                result = format!("Y {} -Z", radius);
                            }
                            &[0xE1] => {
                                result = format!("Z {} X", radius);
                            }
                            &[0xE0] => {
                                result = format!("Z {} Y", radius);
                            }
                            &[0xDE] => {
                                result = format!("Z {} -X", radius);
                            }
                            &[0xDD] => {
                                result = format!("Z {} -Y", radius);
                            }

                            &[0xD6] => {
                                result = format!("-X {} Y", radius);
                            }
                            &[0xD5] => {
                                result = format!("-X {} Z", radius);
                            }
                            &[0xD3] => {
                                result = format!("-X {} -Y", radius);
                            }
                            &[0xD2] => {
                                result = format!("-X {} -Z", radius);
                            }
                            &[0xCD] => {
                                result = format!("-Y {} X", radius);
                            }
                            &[0xCB] => {
                                result = format!("-Y {} Z", radius);
                            }
                            &[0xCA] => {
                                result = format!("-Y {} -X", radius);
                            }
                            &[0xC8] => {
                                result = format!("-Y {} -Z", radius);
                            }
                            &[0xC3] => {
                                result = format!("-Z {} X", radius);
                            }
                            &[0xC2] => {
                                result = format!("-Z {} Y", radius);
                            }
                            &[0xC0] => {
                                result = format!("-Z {} -X", radius);
                            }
                            &[0xBF] => {
                                result = format!("-Z {} -Y", radius);
                            }
                            &_ => {}
                        }
                        val = StringType(result.into());
                    }
                    &_ => {}
                }
                //todo use dynfmt
                match &res_input[..4] {
                    &[0x0, 0x0, 0x0, 0x3] => {
                        // let (_, value) = be_u8(&tmp_input[7..8])?;
                        let (_, value) = be_u32(&res_input[4..8])?;
                        val = StringType(format!("P{}", value).into());
                        if value > 1000 {
                            let value = value - 1000;
                            val = StringType(format!("-P{}", value).into());
                        }
                    }
                    &[0x0, 0x0, 0x0, 0xC] => {
                        let value = get_expression_angle_or_param(&res_input[4..8])?.1;
                        let result = format!("X {} Y", value);
                        val = StringType(result.into());
                    }
                    &[0x0, 0x0, 0x0, 0xD] => {
                        let value = get_expression_angle_or_param(&res_input[4..8])?.1;
                        let result = format!("X {} Z", value);
                        val = StringType(result.into());
                    }
                    &[0x0, 0x0, 0x0, 0xE] => {
                        let value = get_expression_angle_or_param(&res_input[4..8])?.1;
                        let result = format!("X {} -X", value);
                        val = StringType(result.into());
                    }
                    &[0x0, 0x0, 0x0, 0xF] => {
                        let value = get_expression_angle_or_param(&res_input[4..8])?.1;
                        let result = format!("X {} -Y", value);
                        val = StringType(result.into());
                    }
                    &[0x0, 0x0, 0x0, 0x10] => {
                        let value = get_expression_angle_or_param(&res_input[4..8])?.1;
                        let result = format!("X {} -Z", value);
                        val = StringType(result.into());
                    }
                    &[0x0, 0x0, 0x0, 0x15] => {
                        let value = get_expression_angle_or_param(&res_input[4..8])?.1;
                        let result = format!("Y {} X", value);
                        val = StringType(result.into());
                    }
                    &[0x0, 0x0, 0x0, 0x17] => {
                        let value = get_expression_angle_or_param(&res_input[4..8])?.1;
                        let result = format!("Y {} Z", value);
                        val = StringType(result.into());
                    }
                    &[0x0, 0x0, 0x0, 0x18] => {
                        let value = get_expression_angle_or_param(&res_input[4..8])?.1;
                        let result = format!("Y {} -X", value);
                        val = StringType(result.into());
                    }
                    &[0x0, 0x0, 0x0, 0x19] => {
                        let value = get_expression_angle_or_param(&res_input[4..8])?.1;
                        let result = format!("Y {} -Y", value);
                        val = StringType(result.into());
                    }
                    &[0x0, 0x0, 0x0, 0x1A] => {
                        let value = get_expression_angle_or_param(&res_input[4..8])?.1;
                        let result = format!("Y {} -Z", value);
                        val = StringType(result.into());
                    }
                    &[0x0, 0x0, 0x0, 0x1F] => {
                        let value = get_expression_angle_or_param(&res_input[4..8])?.1;
                        let result = format!("Z {} X", value);
                        val = StringType(result.into());
                    }
                    &[0x0, 0x0, 0x0, 0x20] => {
                        let value = get_expression_angle_or_param(&res_input[4..8])?.1;
                        // dbg!(&value);
                        let result = format!("Z {} Y", value);
                        val = StringType(result.into());
                    }
                    &[0x0, 0x0, 0x0, 0x22] => {
                        let value = get_expression_angle_or_param(&res_input[4..8])?.1;
                        let result = format!("Z {} -X", value);
                        val = StringType(result.into());
                    }
                    &[0x0, 0x0, 0x0, 0x23] => {
                        let value = get_expression_angle_or_param(&res_input[4..8])?.1;
                        let result = format!("Z {} -Y", value);
                        val = StringType(result.into());
                    }
                    &[0x0, 0x0, 0x0, 0x24] => {
                        let value = get_expression_angle_or_param(&res_input[4..8])?.1;
                        let result = format!("Z {} -Z", value);
                        val = StringType(result.into());
                    }
                    &[0x0, 0x0, 0x0, 0x29] => {
                        let value = get_expression_angle_or_param(&res_input[4..8])?.1;
                        let result = format!("-X {} -X", value);
                        val = StringType(result.into());
                    }
                    &[0x0, 0x0, 0x0, 0x2A] => {
                        let value = get_expression_angle_or_param(&res_input[4..8])?.1;
                        let result = format!("-X {} Y", value);
                        val = StringType(result.into());
                    }
                    &[0x0, 0x0, 0x0, 0x2B] => {
                        let value = get_expression_angle_or_param(&res_input[4..8])?.1;
                        let result = format!("-X {} Z", value);
                        val = StringType(result.into());
                    }
                    &[0x0, 0x0, 0x0, 0x2D] => {
                        let value = get_expression_angle_or_param(&res_input[4..8])?.1;
                        let result = format!("-X {} -Y", value);
                        val = StringType(result.into());
                    }
                    &[0x0, 0x0, 0x0, 0x2E] => {
                        let value = get_expression_angle_or_param(&res_input[4..8])?.1;
                        let result = format!("-X {} -Z", value);
                        val = StringType(result.into());
                    }
                    &[0x0, 0x0, 0x0, 0x33] => {
                        let value = get_expression_angle_or_param(&res_input[4..8])?.1;
                        let result = format!("-Y {} X", value);
                        val = StringType(result.into());
                    }
                    &[0x0, 0x0, 0x0, 0x34] => {
                        let value = get_expression_angle_or_param(&res_input[4..8])?.1;
                        let result = format!("-Y {} Y", value);
                        val = StringType(result.into());
                    }
                    &[0x0, 0x0, 0x0, 0x35] => {
                        let value = get_expression_angle_or_param(&res_input[4..8])?.1;
                        let result = format!("-Y {} Z", value);
                        val = StringType(result.into());
                    }
                    &[0x0, 0x0, 0x0, 0x36] => {
                        let value = get_expression_angle_or_param(&res_input[4..8])?.1;
                        let result = format!("-Y {} -X", value);
                        val = StringType(result.into());
                    }
                    &[0x0, 0x0, 0x0, 0x38] => {
                        let value = get_expression_angle_or_param(&res_input[4..8])?.1;
                        let result = format!("-Y {} -Z", value);
                        val = StringType(result.into());
                    }
                    &[0x0, 0x0, 0x0, 0x3D] => {
                        let value = get_expression_angle_or_param(&res_input[4..8])?.1;
                        let result = format!("-Z {} X", value);
                        val = StringType(result.into());
                    }
                    &[0x0, 0x0, 0x0, 0x3E] => {
                        let value = get_expression_angle_or_param(&res_input[4..8])?.1;
                        let result = format!("-Z {} Y", value);
                        val = StringType(result.into());
                    }
                    &[0x0, 0x0, 0x0, 0x3F] => {
                        let value = get_expression_angle_or_param(&res_input[4..8])?.1;
                        let result = format!("-Z {} Z", value);
                        val = StringType(result.into());
                    }
                    &[0x0, 0x0, 0x0, 0x40] => {
                        let value = get_expression_angle_or_param(&res_input[4..8])?.1;
                        let result = format!("-Z {} -X", value);
                        val = StringType(result.into());
                    }
                    &[0x0, 0x0, 0x0, 0x41] => {
                        let value = get_expression_angle_or_param(&res_input[4..8])?.1;
                        let result = format!("-Z {} -Y", value);
                        val = StringType(result.into());
                    }
                    _ => {}
                }
            }
        }
    } else if flag == 4 {
        let mut param1 = String::new();
        // 目前的推论是 0x28代表符号部分 ，0x1代表数字部分
        match &res_input[..2] {
            &[0x0, 0x0] => {
                let (_, times) = be_i16(&res_input[2..4])?;
                let times = times_keep_f32_three_decimal_place(times as i32);
                if times == 1.0 {
                    param1 = "PARAM".to_string();
                } else if times == 0.0 {
                    param1 = "".to_string();
                } else {
                    param1 = format!("{} TIMES PARAM", times);
                }
            }
            &[0xFF, 0xFF] => {
                let (_, times) = be_i16(&res_input[2..4])?;
                if times > 0xFFFFu16 as i16 {
                    let mut times = ((0xFFFFu16 as i16) as f32 - times as f32 - 1.0) / 40.0f32;
                    times = (times * 100.0_f32).round() / 100.0;
                    param1 = format!("{} TIMES PARAM", times);
                } else {
                    let mut times = (times as f32 - (0xFFFFu16 as i16) as f32 - 1.0) / 40.0f32;
                    times = (times * 100.0_f32).round() / 100.0;
                    param1 = format!("{} TIMES PARAM", times);
                }
            }
            _ => {}
        }
        match &res_input[4..8] {
            &[0x0, 0x0, 0x0, 0x1] => {
                let (_, value) = be_i32(&res_input[8..12])?;
                if value >= 50 && value < 0x65 {
                    let value = value - 50;
                    param1 = param1.replace("PARAM", ""); // 防止出现两个para
                    param1 = format!("{} DESIGN PARAM {}", param1, value);
                } else if value >= 500 && value < 0x3E9 {
                    let value = value - 500;
                    if value < 50 {
                        param1 = format!("TWICE PARAM {}", value);
                    } else {
                        let value = value - 100;
                        param1 = format!("TWICE IPARAM {}", value);
                    }
                } else if value >= 0x65 && value < 0x3E9 {
                    // PARAM 数值大于 0x65 就是 IPARAM
                    let value = value - 0x64;
                    param1 = format!("IPARAM {}", value);
                } else if value >= 0x3E9 {
                    let value = value - 0x3E8;
                    if value >= 0x65 {
                        let value = value - 0x64;
                        param1 = format!("- IPARAM {}", value);
                    } else {
                        param1 = format!("- {} {}", param1, value);
                    }
                } else if value <= 0xFFFFFFFFu32 as i32 {
                    let (_, t) = be_i32(&res_input[..4])?;
                    // times是除以0x28的倍数
                    if t == 0x28 {
                        param1 = match_angle_or_return_number(parse_to_i32(&res_input[8..12]));
                    } else {
                        let mut value = "".to_string();
                        let v = t as f32 / 40.0;
                        let times = round_f32(v);
                        value = get_implicit_angle_expression(&res_input[8..12]);
                        if value == "" {
                            let v = parse_to_i32(&res_input[8..12]);
                            if v != 0 {
                                value = (-v as f32 / 10.0).to_string();
                            }
                        }
                        param1 = format!("{} TIMES {}", times, value);
                    }
                } else {
                    param1 = format!("{} {}", param1, value);
                }
            }
            &[0x0, 0x0, 0x0, 0x2] => {
                let (_, value) = be_i32(&res_input[8..12])?;
                if value > 1000 {
                    let value = value - 1000;
                    match &res_input[12..16] {
                        &[0xFF, 0xFF, 0xFF, 0xFB] => {
                            param1 = format!("TANF - {} {} DDHEIGHT", param1, value);
                        }
                        &[0xFF, 0xFF, 0xFF, 0xFC] => {
                            param1 = format!("TANF - {} {} DDANGLE", param1, value);
                        }
                        _ => {}
                    }
                } else {
                    let angle = get_implicit_angle_expression(&res_input[8..12]);
                    if angle != "" {
                        match &res_input[12..16] {
                            &[0xFF, 0xFF, 0xFF, 0xFB] => {
                                param1 = format!("TANF {} DDHEIGHT", angle);
                            }
                            &[0xFF, 0xFF, 0xFF, 0xFC] => {
                                param1 = format!("TANF {} DDANGLE", angle);
                            }
                            _ => {}
                        }
                    } else {
                        match &res_input[12..16] {
                            &[0xFF, 0xFF, 0xFF, 0xFB] => {
                                param1 = format!("TANF {} {} DDHEIGHT", param1, value);
                            }
                            &[0xFF, 0xFF, 0xFF, 0xFC] => {
                                param1 = format!("TANF {} {} DDANGLE", param1, value);
                            }
                            _ => {}
                        }
                    }
                }
            }
            &[0x0, 0x0, 0x0, 0x3] => {
                let (_, times) = be_i32(&res_input[..4])?;
                let times = times_keep_f32_three_decimal_place(times);
                let (_, (value1, value2)) = tuple((be_i32, be_i32))(&res_input[8..16])?;
                param1 = parse_param_with_index(value1);
                let result = parse_param_with_index(value2);
                if times != 1.0 {
                    param1 = format!("{} TIMES DIFFERENCE {} {}", times, param1, result);
                } else {
                    param1 = format!("DIFFERENCE {} {}", param1, result);
                }
            }

            &[0x0, 0x0, 0x0, 0x4] => {
                let (_, times) = be_i32(&res_input[..4])?;
                let times = times_keep_f32_three_decimal_place(times);
                let (_, (value1, value2)) = tuple((be_i32, be_i32))(&res_input[8..16])?;

                param1 = parse_param_with_index(value1);
                let param2 = parse_param_with_index(value2);
                if times != 1.0 {
                    param1 = format!("{} TIMES SUM {} {}", times, param1, param2);
                } else {
                    param1 = format!("SUM {} {}", param1, param2);
                }
            }
            //代表是WORD
            &[0x0, 0x0, 0x0, 0x7] => {
                let (_, n0) = be_u32(&res_input[8..12])?;
                let (_, n1) = be_u32(&res_input[12..16])?;
                let hash: u32 = format!("{n0}{n1}").parse().unwrap_or_default();
                param1 = db1_dehash(hash);
            }
            &[0x0, 0x0, 0x0, 0x8] => {
                let (_, times) = be_i32(&res_input[..4])?;
                let times = times_keep_f32_three_decimal_place(times);
                let (_, (value1, value2)) = tuple((be_i32, be_i32))(&res_input[8..16])?;
                param1 = parse_param_with_index(value1);
                let result = parse_param_with_index(value2);
                if times != 1.0 {
                    param1 = format!("{} TIMES SUM {} {}", times, param1, result);
                } else {
                    param1 = format!("MULT {} {}", param1, result);
                }
            }
            &[0x0, 0x0, 0x0, 0x9] => {
                let (_, times) = be_i32(&res_input[..4])?;
                let times = times_keep_f32_three_decimal_place(times);
                let (_, (value1, value2)) = tuple((be_i32, be_i32))(&res_input[8..16])?;
                param1 = parse_param_with_index(value1);
                let result = parse_param_with_index(value2);
                if times != 1.0 {
                    param1 = format!("{} TIMES SUM {} {}", times, param1, result);
                } else {
                    param1 = format!("DIV {} {}", param1, result);
                }
            }

            _ => {}
        }
        if res_input.len() > 24 {
            let value = get_implicit_angle_expression(&res_input[16..20]);
            param1 = format!("{} {}", param1, value);
        }
        return Ok((input, StringType(param1.into())));
    }
    Ok((input, val))
}

/// 特殊处理AXIS显式属性
pub fn convert_to_explicit_axis_string(input: &[u8], refno: RefU64) -> IResult<&[u8], AttrVal> {
    let mut result = StringType("".into());
    #[cfg(debug_parse_expr)]
    println!("explicit axis: {}", pretty_hex(&input));
    if input.len() < 20 {
        let (_, val) = parse_to_expression(input, StringType("".to_owned()))?;
        result = val;
    } else {
        // 检测是否以 1A 1A 05 02 17 开头
        let (mut tmp_input, (a, b, c, d, e)) =
            tuple((be_u32, be_u32, be_u32, be_u32, be_u32))(input)?;
        match [d, e] {
            // 0x16 开头就是 X () Y ... 两个坐标的类型
            // 0x2 0x16 后面第一个就是 X Y Z 这三种坐标
            [0x2, 0x16] => {
                let (tmp_input, mut first_data) = parse_xyz_data(tmp_input, refno, false)?;
                if first_data.starts_with("-") {
                    first_data = format!("AXIS {}", first_data);
                }
                let second = match_axis(parse_to_u32(&tmp_input[..4]));
                result = StringType((format!("{}{}", first_data, second)));
            }
            // 0x17 开头代表是 X () Y () Z 这种类型
            [0x2, 0x17] => {
                let (tmp_input, mut first_data) = parse_xyz_data(tmp_input, refno, false)?;
                if first_data.starts_with("-") {
                    first_data = format!("AXIS {}", first_data);
                }
                let (tmp_input, second_data) = parse_xyz_data(tmp_input, refno, false)?;
                let third = match_axis(parse_to_u32(&tmp_input[..4]));
                result = StringType((format!("{}{}{}", first_data, second_data, third)));
            }
            [0x2, 0x22] => {
                let (_, (a, b, c, d)) = tuple((be_u32, be_u32, be_u32, be_u32))(tmp_input)?;
                // dbg!((a, b, c, d));
                if a == 0x52 && b == 0x3 && c == 0x7 {
                    result = StringType((format!("PP {}", d)));
                }
            }
            [0x2, 0x34] => {
                if let Some((func, count)) = match_to_dir(parse_to_u32(&tmp_input[4..8])) {
                    let mut v = func.to_string();
                    v.push_str(" ");
                    let mut axis_data = &tmp_input[8..];
                    for i in 0..count {
                        let (residual, mut coord) = parse_xyz_data(axis_data, refno, true)?;
                        // dbg!(&coord);
                        v.push_str(&coord);
                        axis_data = residual;
                    }
                    // dbg!(&v);
                    result = StringType(v);
                }
            }
            _ => match &tmp_input[..8] {
                &[0x0, 0x0, 0x0, 0xB, 0x0, 0x0, 0x0, 0x3D] => result = StringType("X".into()),
                &[0x0, 0x0, 0x0, 0xC, 0x0, 0x0, 0x0, 0x3D] => result = StringType("-X".into()),
                &[0x0, 0x0, 0x0, 0xD, 0x0, 0x0, 0x0, 0x3D] => result = StringType("Y".into()),
                &[0x0, 0x0, 0x0, 0xE, 0x0, 0x0, 0x0, 0x3D] => result = StringType("-Y".into()),
                &[0x0, 0x0, 0x0, 0xF, 0x0, 0x0, 0x0, 0x3D] => result = StringType("Z".into()),
                &[0x0, 0x0, 0x0, 0x10, 0x0, 0x0, 0x0, 0x3D] => result = StringType("-Z".into()),
                _ => {}
            },
        }
    }
    Ok((input, result))
}

/// match ptcdirection 的方法
pub fn match_to_dir(key: u32) -> Option<(&'static str, usize)> {
    match key {
        0x1F => Some(("TO", 1)),
        0x20 => Some(("TO", 2)),
        0x21 => Some(("TO", 3)),
        _ => None,
    }
}

/// match AXIS显式属性对应的值
#[inline]
pub fn match_axis(key: u32) -> String {
    match key {
        0xB => "X".to_string(),
        0xC => "-X".to_string(),
        0xD => "Y".to_string(),
        0xE => "-Y".to_string(),
        0xF => "Z".to_string(),
        0x10 => "-Z".to_string(),
        _ => " ".to_string(),
    }
}

/// 检查是否是Axis属性
#[inline]
pub fn check_is_expr(noun: i32) -> bool {
    //todo make stable
    if EXPR_ATT_SET.contains(&noun) || noun < 0 {
        true
    } else {
        false
    }
}

/// 隐式表达式解析，给一个字符串返回DDHEIGHT这种表达式
#[inline]
pub fn get_implicit_angle_expression(input: &[u8]) -> String {
    let mut val = String::new();
    match input {
        &[0xFF, 0xFF, 0xFF, 0xFB] => {
            val = "DDHEIGHT".to_string();
        }
        &[0xFF, 0xFF, 0xFF, 0xFC] => {
            val = "DDANGLE".to_string();
        }
        &[0xFF, 0xFF, 0xFF, 0xFD] => {
            val = "DDRADIUS".to_string();
        }
        _ => {}
    }
    // db1_dehash(convert_to_hash)
    val
}

#[inline]
pub fn is_desi_noun(bytes: &[u8]) -> bool {
    bytes == [0x0, 0xB, 0x6, 0x92].as_slice()
}

#[inline]
pub fn is_cata_noun(bytes: &[u8]) -> bool {
    bytes == [0x0, 0x8, 0xA1, 0xE6].as_slice()
}

///保存 noun_hash->refno  map
pub fn save_type_hash_file(dir: &str, out_name: &str) -> Result<()> {
    let mut unique_hash_refno_map = DashMap::new();
    let mut path_buf = fs::read_dir(dir)?
        .into_iter()
        .filter_map(|entry| entry.ok().map(|e| e.path()))
        .filter(|p| is_pdms_db_file(p))
        .collect::<Vec<PathBuf>>();
    path_buf.sort_by(|a, b| {
        fs::metadata(b)
            .map(|m| m.len())
            .unwrap_or_default()
            .partial_cmp(&fs::metadata(a).map(|m| m.len()).unwrap_or_default())
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    for path in path_buf {
        let meta = fs::metadata(&path)?;
        if meta.len() < 36 {
            println!("skip short file (len < 36): {:?}", path);
            continue;
        }
        let mut file = File::open(&path).context(format!("open db file {:?}", path))?;
        let mut buf = vec![0u8; 36];
        if let Err(e) = file.read_exact(&mut buf) {
            println!("skip unreadable file {:?}, err: {}", path, e);
            continue;
        }
        let _input = &buf[32..36];
        let start = Instant::now();
        println!("path={:?}", path);
        let mut buf: Vec<u8> = Vec::new();
        file.read_to_end(&mut buf).context("read db body")?;
        let time = start.elapsed();
        println!("read {:?} finished in {:?}", path, time);
        process_type_hash(&buf[..], &mut unique_hash_refno_map, &path);
        println!("noun_hash_refnos len = {:?}", unique_hash_refno_map.len());
        let encode = bincode::serialize(&unique_hash_refno_map)?;
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(out_name)
            .context(format!("open output file {}", out_name))?;
        file.write(&encode)?;
    }
    Ok(())
}

/// 简单判定是否为 PDMS/E3D 数据文件，避免解析非目标文件
fn is_pdms_db_file(path: &Path) -> bool {
    // 跳过常见的非 db 扩展名
    if let Some(ext) = path.extension().and_then(|s| s.to_str()) {
        let ext = ext.to_ascii_lowercase();
        if matches!(ext.as_str(), "com" | "mis" | "txt" | "log" | "json" | "rs") {
            return false;
        }
        if matches!(ext.as_str(), "db" | "sys" | "mdb" | "pdms") {
            return true;
        }
    }
    // 无扩展名：只要文件足够大（后续再按长度/头部过滤）
    true
}

/// 检查文件头是否包含 PDMS/E3D DbType 标识
fn has_valid_db_header(path: &Path) -> bool {
    if let Ok(mut file) = File::open(path) {
        let mut header = [0u8; 64];
        if let Ok(len) = file.read(&mut header) {
            if len >= 36 {
                // 偏移 32..36 是 db type hash（参考现有解析逻辑）
                let db_type_hash = parse_to_i32(&header[32..36]);
                let db_type = db1_dehash(db_type_hash as u32).to_ascii_uppercase();
                // 已知类型字符串判定
                const DB_TYPES: [&str; 6] = ["DESI", "CATA", "DICT", "SYST", "GLB", "GLOB"];
                if DB_TYPES.contains(&db_type.as_str()) {
                    return true;
                }
            }
        }
    }
    false
}

///处理type_hash对应的refno位置信息
fn process_type_hash<'a>(
    input: &'a [u8],
    type_hash: &mut DashMap<i32, (RefU64, String)>,
    path: &PathBuf,
) -> bool {
    let refno_0_set = get_total_refno_0s(input);
    let path = Path::new(path);
    let file_name: String = path
        .file_name()
        .unwrap()
        .to_owned()
        .to_string_lossy()
        .to_string()
        .into();
    let noun_map = get_default_pdms_db_info();
    refno_0_set.par_iter().for_each(|ref_0| {
        let pos_iter = rfind_iter(&input, ref_0);
        for p in pos_iter {
            if let Some(refno_entry) = get_refno_entry(input, p) {
                type_hash
                    .entry(refno_entry.1.noun_hash)
                    .or_insert((refno_entry.0, file_name.clone()));
                break; //if found, just break
            }
        }
    });
    true
}

///获取所有不同的 refno_0
pub fn get_total_refno_0s(input: &[u8]) -> HashSet<&[u8]> {
    let mut refno_0_set = HashSet::new();
    let mut pos_iter = rfind_iter(&input, &REFNO_ALL_INDEX_PAGE[..]);
    while let Some(i) = pos_iter.next() {
        let mut j = i + 0x6 * 4; //偏移6 dword
        let mut d = &input[j..j + 4];
        while d != [0, 0, 0, 0].as_slice() {
            refno_0_set.insert(d);
            j += 0x4 * 4;
            d = &input[j..j + 4];
        }
    }
    refno_0_set
}

pub fn get_expression_angle_or_param(input: &[u8]) -> IResult<&[u8], String> {
    let mut value = get_implicit_angle_expression(input);
    if value == "".to_string() {
        value = parse_param_with_index(parse_to_i32(input));
    }
    Ok((input, value))
}

#[derive(Debug, Default)]
pub struct DbBasicInfo {
    pub db_type: String,
    pub ses_pgno: u32,
    pub db_no: u32,
}

/// 获取文件的type和ses_pgno, db number
pub fn parse_db_basic_info(path: PathBuf) -> DbBasicInfo {
    let mut file = File::open(&path).unwrap();
    let mut buf = vec![0u8; 60];
    file.read_exact(&mut buf).unwrap();
    parse_file_basic_info(&buf)
}

/// 获取文件的type和ses_pgno, db number
pub fn parse_file_basic_info(input: &[u8]) -> DbBasicInfo {
    let t = parse_to_u32(&input[32..36]);
    let mut file_type = "".to_string();
    if t >= 0x81BF1 {
        file_type = db1_dehash(t);
    }
    let db_no = parse_to_u32(&input[8..12]);
    let ses_pgno = parse_to_u32(&input[40..44]);
    DbBasicInfo {
        db_type: file_type,
        ses_pgno,
        db_no,
    }
}

//直接使用session的会话，读取所有的EleDataEntry, 靠这个搜索出来的不

///获得参考号对应的Entry
#[inline]
fn get_refno_entry(input: &[u8], offset: usize) -> Option<(RefU64, EleDataEntry)> {
    let input = &input[offset - 4..];
    let noun_hash = parse_to_i32(&input[12..16]);
    let mut refno_entry = None;
    let mut is_ok = true;
    if NOUN_TYPES_MAP.contains_key(&noun_hash) {
        let (_, (len, refno)) = tuple::<_, _, nom::error::Error<&[u8]>, _>((
            be_i32, //len
            be_u64,
        ))(&input[0..12])
        .ok()?;
        let len = parse_to_u32(&input[0..4]);
        let refno = RefU64::from(&input[4..12]);
        let test_refno = get_db_option().get_test_refno().map(|x| x.refno());
        let is_debug = test_refno == Some(refno);
        if len != 0 && (len & 0xFFFF000 == 0) {
            let tmp_pos = len as usize * 4; //隐含属性理论结束点
            let found_0_7 = memmem::find(
                &input[tmp_pos..tmp_pos + 20],
                &[0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x7],
            );
            if is_debug {
                dbg!(found_0_7);
            }

            if found_0_7.is_some() {
                //允许一定范围去查找
                if let Some(next_pos) = memmem::find(&input[12..tmp_pos + 20], &input[4..12]) {
                    let end_pos = (next_pos + 12); //隐含属性实际结束点
                    if end_pos >= tmp_pos + 4 {
                        let diff_len = end_pos - tmp_pos - 4;
                        if is_debug {
                            dbg!(diff_len);
                        }
                        is_ok = diff_len == 0;
                        if diff_len > 0 && diff_len % 4 == 0 && end_pos > tmp_pos {
                            let s: IResult<&[u8], (Vec<i32>, i32)> = many_till(
                                verify(be_i32, |&x| x == 0),
                                verify(be_i32, |&x| x == 7),
                            )(
                                &input[tmp_pos..end_pos]
                            );
                            if is_debug {
                                dbg!(&s);
                            }
                            if s.is_ok() {
                                is_ok = (diff_len / 4) == (s.unwrap().1.0.len() + 1);
                            }
                        }
                    }
                }
            } else {
                let mem_flag = parse_to_u16(&input[tmp_pos..tmp_pos + 2]);
                if mem_flag == 2 || mem_flag == 1 {
                    is_ok = (&input[tmp_pos + 4..tmp_pos + 12]) == &input[4..12];
                }
            }
        }
        if is_ok {
            refno_entry = Some((
                refno,
                EleDataEntry {
                    pos: offset,
                    noun_hash,
                },
            ));
        }
    }
    refno_entry
}

/// map中将所有offset不为0的值进行排序, 返回Noun hash 的排序
pub fn sort_offsets(map: &DashMap<i32, AttrInfo>) -> Vec<i32> {
    let mut off_map = BTreeMap::new();
    for kv in map {
        let v = kv.value();
        let h = *kv.key();
        if v.offset > 0xFFFF {
            let o = (v.offset >> 0x14) as u32;
            off_map.insert((v.offset & 0xFFFFF) * 100 + o, h);
        } else if v.offset != 0 {
            off_map.insert(v.offset * 100, h);
        }
    }
    off_map.values().cloned().collect()
}

pub fn get_offset_map(map: DashMap<i32, AttrInfo>) -> HashMap<u32, (u32, DbAttributeType)> {
    let mut offset_map = HashMap::new();
    for (_, a) in map {
        if a.offset != 0 {
            offset_map.insert(a.offset, (a.offset, a.att_type));
        }
    }
    offset_map
}

///通过offset获取某个隐式属性的长度
pub fn get_implicit_len_by_offset(count: &Vec<u32>, offset: u32) -> usize {
    if let Some(index) = count.iter().position(|o| *o == offset) {
        return (count[index + 1] - count[index]) as usize;
    }
    0
}

/// 根据i32数据match DDHEIGHT这种表达式，若没有则返回数据
pub fn match_angle_or_return_number(input: i32) -> String {
    let mut result = get_implicit_angle_expression(&input.to_be_bytes());
    if result == "" {
        let value =
            (((0xFFFFFFFFu32 as i32 - input) as f32 / 0xA as f32 + 0.1) * 10.0).round() / 10.0;
        result = value.to_string();
    }
    result
}

pub fn parse_pdms_project_name(input: &str) -> IResult<&str, &str> {
    let (_, name) = take_until("sys")(input)?;
    Ok((input, name))
}

pub const WORLD_NOUN: i32 = 0xBEB83;

pub fn gen_ref_type_pos_table(input: &[u8]) -> (DashMap<RefU64, EleDataEntry>, RefU64) {
    // 索引优先：走会话 B-tree 索引 O(log n) 建表（得最新会话存活集）；解码失败回退全缓冲扫描（全物理记录集）。语义：干净库相等、编辑库索引更权威，见 ADR-005。
    if let Some(indexed) = crate::refno_index::gen_ref_type_pos_table_from_index(input) {
        return indexed;
    }
    gen_ref_type_pos_table_scan(input)
}

pub fn gen_ref_type_pos_table_scan(input: &[u8]) -> (DashMap<RefU64, EleDataEntry>, RefU64) {
    let refno_0_set = get_total_refno_0s(input);
    let mut refno_table = DashMap::new();
    let mut word_refno_hashset = DashSet::new();
    refno_0_set.par_iter().for_each(|ref_0| {
        let pos_iter = rfind_iter(&input, ref_0);
        for p in pos_iter {
            //需要检查是否满足要求，前面基本是 0x 00 00 00 xx
            let t = &input[p - 4..p];
            if !(t[0] == 0 && t[1] == 0 && t[2] == 0 && t[3] >= 0x8) {
                continue;
            }
            if let Some((refno, entry)) = get_refno_entry(input, p) {
                //判断是否是World
                if entry.noun_hash == WORLD_NOUN {
                    word_refno_hashset.insert(refno);
                }
                refno_table.entry(refno).or_insert(entry);
            }
        }
    });
    let world_refno = word_refno_hashset.into_iter().next().unwrap_or_default();
    (refno_table, world_refno)
}

/// 获取 ref_no + type 的索引位置表  和 world的参考号
/// 根据get_last_index_position返回的hashset获取所有的ref_no + type的位置
/// 返回值是hashmap k:所有的ref_no v:(ref_no的position,type的hash)
/// 利用这个层级关系去解析数据，加快速度
pub fn gen_ref_type_pos_table_parallel(
    input: &[u8],
    noun_attr_info_map: &DashMap<String, DashMap<String, AttrInfo>>,
) -> (DashMap<RefU64, EleDataEntry>, RefU64) {
    let get_total_timer = Instant::now();
    let refno_0_set = get_total_refno_0s(input);
    println!(
        "Get total refnos {} ms",
        get_total_timer.elapsed().as_millis()
    );

    let refno_0_set_timer = Instant::now();
    // let mut world_refno = Arc::new(Mutex::new(RefU64::default()));
    let mut refno_table: DashMap<RefU64, EleDataEntry> = DashMap::new();
    let mut word_refno_hashset = DashSet::new();
    //todo 需要根据文件大小去优化
    let segs = 4;
    let step_size = input.len() / segs; //处理分段正好落在分割的地方的情况， 前后扩展多 20个 bytes吧
    refno_0_set.par_iter().for_each(|ref_0| {
        //分成32段
        (0..segs).into_par_iter().for_each(|x| {
            let start = if x != 0 { step_size * x - 20 } else { 100 };
            if start < input.len() {
                let data = if x == segs - 1 {
                    &input[start..input.len()] //最后一段
                } else {
                    &input[start..start + step_size]
                };
                if data.len() > 100 {
                    let pos_iter = rfind_iter(data, ref_0);
                    for p in pos_iter {
                        if p < start {
                            continue;
                        }
                        if let Some(refno_entry) = get_refno_entry(data, p - start) {
                            //判断是否是World
                            if refno_entry.1.noun_hash == 0xBEB83 {
                                word_refno_hashset.insert(refno_entry.0);
                            }
                            if refno_table.contains_key(&refno_entry.0) {
                                let d = &*refno_table.get(&refno_entry.0).unwrap();
                                if d.pos < refno_entry.1.pos {
                                    refno_table.insert(refno_entry.0, refno_entry.1);
                                }
                            } else {
                                refno_table.insert(refno_entry.0, refno_entry.1);
                            }
                            // refno_table.entry(refno_entry.0).or_insert(refno_entry.1);
                        }
                    }
                }
            }
        });
    });

    println!(
        "refno_0_set.par_iter costs {} ms",
        refno_0_set_timer.elapsed().as_millis()
    );
    let world_refno = word_refno_hashset.into_iter().next().unwrap_or_default();
    // dbg!(world_refno.to_refno_str());
    (refno_table, world_refno)
}

// 02 代表 members, 01 代表 显示属性
//00 00 00 07 00 02(maybe 01) 00 xx  (REF0)  (REF1)  00 00 00 00  00 00 00 00
fn get_merged_data(input: &[u8], len: &mut usize, flag: u8) -> Vec<u8> {
    let input_len = input.len();
    if input_len < 20 {
        return Vec::new();
    }
    if *len < 20 || *len > input_len || *len + 4 >= input_len {
        return input[20..].to_vec();
    }
    let mut data = input[20..*len].to_vec();
    // if *len + 4 > input_len {
    //     return data;
    // }
    let mut t = *len;
    while t + 8 <= input_len && &input[t..t + 6] == &[0x0, 0x0, 0x0, 0x7, 0x0, flag] {
        let seg_bytes_len = parse_to_u16(&input[t + 6..t + 8]) as usize * 4;
        //跳过6个byte
        let mut s = t + 4 * 6;
        let end = (t + seg_bytes_len + 4).min(input_len);
        // println!("07 的membs范围： {:#4X}..{:#4X}", s, end);
        if end <= s {
            println!("{:#4X}..{:#4X} merged data出错", s, end);
            break;
        }
        let next_seg = &input[s..end];
        data.extend_from_slice(next_seg);
        t += seg_bytes_len + 4;
    }
    *len = t;
    data
}

pub fn get_project_name_from_filename(filename: &str) -> IResult<&str, &str> {
    let (input, p) = alpha1(filename)?;
    Ok((input, p))
}

pub(crate) static NOUN_TYPES_MAP: phf::Map<i32, &'static str> = phf_map! {

0xCC12Di32 => "SPLOAD",
0xE2C6Bi32 => "UDET",
0xA9A9F4Fi32 => "CLNTIL",
0xCD82Bi32 => "SRTORUS",
0x89E42i32 => "PTRACK",
0x1CC376Fi32 => "WTHTAB",
0xE2873i32 => "DUCTING",
0xB15DBi32 => "BOXING",
0xE55C0i32 => "RRST",
0xC10C9i32 => "GRDMODEL",
0x8D80Ai32 => "LALB",
0x11A118D0i32 => "HPRNOT",
0xAB9A3i32 => "SDSH",
0xC7A33i32 => "TRNN",
0x10169B35i32 => "FMEXTR",
0xBD94Bi32 => "CELL",
0xB9FFCi32 => "TASK",
0xBF8C8i32 => "RFWL",
0xE40D2i32 => "FILTER",
0x4FE43C1i32 => "TABQUESTION",
0x1672C1i32 => "TATTA",
0x9F801i32 => "UDEFINITION",
0xF7C27i32 => "ABOX",
0x7DBE404i32 => "HYSBDI",
0xD115C20i32 => "SUPNFO",
0x9D572i32 => "CATEGORY",
0x14BD9A20i32 => "TESTEXPRESSION",
0xB551474i32 => "ASITEM",
0xAE47FF7i32 => "CYMWRL",
0x112C974Bi32 => "EXTDAT",
0x11865E5Ci32 => "STRFLT",
0x12EFAF34i32 => "VMSUBV",
0x9076Di32 => "TRACING",
0x4E1225i32 => "GLYPH",
0x1CC21C9i32 => "PDATAB",
0x149355Ei32 => "HICPLA",
0x54E18Bi32 => "GRPLI",
0x17B3F44i32 => "CTSTRA",
0xE551Ei32 => "RLST",
0xB0F2Bi32 => "REVISION",
0xC0E7676i32 => "GIPPAN",
0xBE3AEi32 => "PVOLUME",
0x9E2963Ai32 => "HYTANK",
0x11E75A8Di32 => "TOPEXT",
0xB3F26i32 => "PALJOINT",
0x4EDDA5Ci32 => "HRKPSE",
0x787A326i32 => "MRESTH",
0x410426Ei32 => "MTPGSD",
0x97BC1i32 => "SNODE",
0x4C0DB04i32 => "WLPANEL",
0xB070E47i32 => "GLOCWL",
0xCB14440i32 => "LDRRUN",
0x11C16D9Fi32 => "SCINSTRUMENT",
0x9CA1Fi32 => "TAPER",
0x95743i32 => "SSBDOCU",
0xB9FEFi32 => "GASKET",
0x34F774i32 => "SPINE",
0x83E57i32 => "DBL",
0xF563Ei32 => "PTAXIS",
0xA7D77F0i32 => "HOLDFL",
0xF9BA5CCi32 => "TRUSER",
0x1220244Ci32 => "CTREDU",
0x2C3704i32 => "AWELD",
0xAF3C0i32 => "SOLID",
0x8499Fi32 => "CAP",
0x3208DE3i32 => "SPMSPC",
0x88CC0i32 => "PPLANE",
0xF8ECEF6i32 => "RLADDR",
0xAAD58D2i32 => "STWALL",
0xC1D97i32 => "RDIMENSION",
0x10A52720i32 => "REVLKS",
0xDC369DDi32 => "COLMAP",
0x6E2493i32 => "HRSOL",
0xCC0A81Ci32 => "DSXOWN",
0x47CD642i32 => "CSCREED",
0xF7A4B26i32 => "SETPARAMETER",
0x88B61i32 => "PCLAMP",
0x2B0F8Ci32 => "OBJHD",
0x13713FE8i32 => "GFCURV",
0xD2786i32 => "COUPLING",
0x1C9F20Di32 => "MPTLAB",
0x9D299i32 => "CASE",
0xC5FECi32 => "PLENUM",
0x9C469i32 => "PANEL",
0xAB2ADi32 => "SSPHERE",
0x11C801E4i32 => "BPFITTING",
0xCF4FEFBi32 => "HYDACO",
0xBEACDi32 => "CIRLIST",
0x11BABD8i32 => "SPMZFA",
0xE4CFDi32 => "PPPT",
0xB3C6351i32 => "HYCSBM",
0x8BAB0i32 => "DTABLE",
0x1CC4AB8i32 => "SNOTAB",
0xAC63DCEi32 => "ATTCOLUMN",
0xAE474i32 => "REGISTRY",
0x3974CE8i32 => "LSWIDD",
0x8F3A6i32 => "FTUBE",
0xE2D3Di32 => "OLET",
0xE556Di32 => "POST",
0xBFE23i32 => "LCYLINDER",
0xBE488i32 => "RCPL",
0xCA761i32 => "COCO",
0xAAD58C2i32 => "CTWALL",
0x11C17783i32 => "MPLNST",
0xC839D3Di32 => "GBRAPN",
0xC87C8i32 => "NLSNOUT",
0xCB2D2i32 => "POGON",
0xACBF2D3i32 => "RUTVOLUME",
0x67A1421i32 => "STRLNG",
0xAF28Di32 => "IDLIST",
0xE71A4i32 => "CMBU",
0x3DC3FB8i32 => "HBLWLD",
0x12A0B565i32 => "ASTATU",
0xDA7D6i32 => "SPLR",
0x98D7E13i32 => "GBLOCK",
0xEC7A4i32 => "NREVOLUTION",
0xB04D3i32 => "PORI",
0xA783DA0i32 => "CPANEL",
0xA94F5EAi32 => "SLRAIL",
0x2C9E50i32 => "LCOMD",
0xDB1DBi32 => "SCPROPERTY",
0xF76A635i32 => "APPDAREA",
0xF9AAE0Di32 => "TROPERATION",
0x1821637i32 => "SPMPSA",
0xE5461i32 => "RESTRAINT",
0x9C1FEi32 => "REMENTRY",
0x4E55936i32 => "SCCORE",
0x8738Bi32 => "PTCAR",
0x28E713i32 => "HBEAD",
0xC89B3i32 => "SCTN",
0x1049D9Ai32 => "SYSCDA",
0xC3F08i32 => "TXTM",
0xC0E5171i32 => "GICPAN",
0x90B48i32 => "HACC",
0xBBA1DA3i32 => "HRTERM",
0x674709i32 => "FMWSK",
0xAC7B826i32 => "HPRHOL",
0x553BD39i32 => "MBRDEF",
0x12898F6Ci32 => "ASREQU",
0x8A1E7i32 => "DATA",
0xE518Ci32 => "VERTEX",
0xB0C33ADi32 => "DBSTWL",
0x1E9E0Bi32 => "GLYTB",
0x25292Ei32 => "ASSOC",
0xE768Di32 => "REDUCER",
0x10154095i32 => "STRSTR",
0xE2963i32 => "ACDT",
0xDEAE5i32 => "NDISH",
0x89EC9i32 => "PYRAMID",
0x11C80096i32 => "SCFITTING",
0x14984104i32 => "GRIDAXIS",
0x4E7730i32 => "HYFRH",
0x977E4i32 => "BEND",
0xAF299CAi32 => "POLPTLIST",
0xA967CD1i32 => "ATTFILTER",
0x9E5AD64i32 => "STALNK",
0x13245E26i32 => "ARCHIV",
0x4F1699Ci32 => "HRGATE",
0xBF1CFi32 => "OUTLINE",
0x10E3D433i32 => "TRMESSAGE",
0x4B1B247i32 => "SRCELEMENT",
0xF4336i32 => "DBVW",
0x98D7E19i32 => "MBLOCK",
0x140209i32 => "MRPLA",
0x1C9E715i32 => "MTPLAB",
0xAD37022i32 => "PBSTPL",
0xC7DC96Di32 => "REGION",
0xC63FC31i32 => "GRIDLN",
0x8A30Ei32 => "BLTABLE",
0x4EDE56Fi32 => "HOOPSE",
0xD224Bi32 => "NSSPHERE",
0x106513D9i32 => "HYLOCS",
0x852D116i32 => "RESTRIC",
0x4975D8Ci32 => "POLYHEDRON",
0x11E75F0Ei32 => "LDREXT",
0x8738Ei32 => "STCATEGORY",
0x112C8F97i32 => "DERDAT",
0x10152770i32 => "HYISTR",
0xAC7B821i32 => "CPRHOL",
0x54ED8Fi32 => "EXTLI",
0xE22B5i32 => "STATUS",
0xD70673Ei32 => "TABGROUP",
0xC1D86i32 => "ADIMENSION",
0xCEE9Bi32 => "LOAPOINT",
0xCF8F40Ei32 => "TRINCOMMAND",
0xCA78Ci32 => "SPCOMPONENT",
0xA783DA5i32 => "HPANEL",
0xFE7B70Fi32 => "CABCORE",
0x346DE1i32 => "HHOLE",
0xE4B8C99i32 => "SYSGRP",
0x4B6A987i32 => "ACRULE",
0x3DC2AE1i32 => "STDWLD",
0xDFCC8i32 => "CLOSURE",
0x55D6111i32 => "HSTIFF",
0xF60C0i32 => "FLEXIBLE",
0x7F4725i32 => "MBURN",
0xC2EE419i32 => "BPOPEN",
0x8A21Ci32 => "CCTABLE",
0xB0658A3i32 => "AREAWLD",
0xE579Ai32 => "FITTING",
0x88CC3i32 => "SPLANE",
0xFA8B6i32 => "NSCYLINDER",
0xDF12C20i32 => "REVCGP",
0x7FC770i32 => "XCLTN",
0x3DC38F4i32 => "DSIWLD",
0x4FCC2B2i32 => "VVALUE",
0x3D60DEEi32 => "REVBLD",
0xAC78CB2i32 => "HICHOL",
0x34481Ai32 => "CABLE",
0xD943Ai32 => "USER",
0xF139Ci32 => "VIEW",
0xAAD2A4i32 => "GSTAT",
0xE4011i32 => "BBOLT",
0x8A1E6i32 => "CATALOGUE",
0x112CBBE7i32 => "HTFEAT",
0x5C7CD3Ai32 => "HYBMSF",
0x3DC4A19i32 => "SSOWLD",
0xEC08A23i32 => "SEGSEQ",
0x6D0B2Ai32 => "CWALL",
0x112CBB1Ei32 => "WLFEAT",
0x11977344i32 => "IJOINT",
0x4C1A264i32 => "FMEDNE",
0xC67825Ei32 => "FMBPLN",
0xE5599EAi32 => "OPENSPACE",
0xC2E93i32 => "SCOMPONENT",
0x84B6Ei32 => "GRP",
0xD2099i32 => "LCSPHERICAL",
0x9D1B1i32 => "NSREVOLUTION",
0x112CBBF1i32 => "RTFEAT",
0x4E947F2i32 => "AREASET",
0x8DA99i32 => "SYLB",
0x117D59D1i32 => "HBRCKT",
0x9E3B8i32 => "LAYER",
0xE21DAi32 => "PLATE",
0x1655E79i32 => "HYWAPA",
0x557440i32 => "GENNI",
0x161383i32 => "SPMSA",
0xF2AF3i32 => "COMW",
0xC7C2A77i32 => "FMWCON",
0x9AB88i32 => "SHEET",
0x66F4662i32 => "INSCMG",
0xA1217i32 => "CINF",
0x2C72F59i32 => "GLYRECT",
0x7EC3536i32 => "HVACFITTING",
0xE4B9A25i32 => "DSXGRP",
0x55D6110i32 => "GSTIFF",
0xD122Ei32 => "TANPOINT",
0x320775i32 => "RNODE",
0xE4C9Fi32 => "CMPTYPE",
0xAD375E5i32 => "FCUTPLANE",
0x6711797i32 => "EXTIMG",
0xCEF26i32 => "PTAPPING",
0xAF4BEi32 => "CYLINDER",
0xF96F243i32 => "LADDER",
0xD1679i32 => "LOOP",
0xDBEEFi32 => "SSTRESS",
0xE4628i32 => "VENT",
0x2237278i32 => "DRTMLB",
0x9D99006i32 => "DRSYLK",
0xC6B50i32 => "PLINE",
0xE4CF1i32 => "DPPT",
0x9BF25i32 => "RELEASE",
0x84F85i32 => "ACR",
0xDB0BDi32 => "CTORUS",
0x10BC8026i32 => "SCOINSTRUMENT",
0x320766i32 => "CNODE",
0x3DC5614i32 => "HYSWLD",
0x2C4233i32 => "BUILDING",
0xAFBC1i32 => "PJOINT",
0xCEE6Ci32 => "SMAP",
0xBFA36i32 => "FONTWORLD",
0xCEEF4i32 => "TRAP",
0x4E1EA6Ci32 => "MNRCRE",
0xE358ADEi32 => "POLOOP",
0xEC09AC8i32 => "NAMSEQ",
0xE05B1i32 => "PORSET",
0x9C802i32 => "SHOE",
0x9D42Fi32 => "DPSET",
0xB6EF78i32 => "SPBOU",
0x886AB6i32 => "AREVOLUTION",
0x9D65Ai32 => "SITE",
0x982DDi32 => "CARD",
0x5538EBCi32 => "STADEF",
0xCC10Di32 => "NOLOAD",
0xE26D1i32 => "RECTANGLE",
0xDF63EA8i32 => "RESTGP",
0x10843670i32 => "HYCKGS",
0x142EE321i32 => "WINDOW",
0x11C801E8i32 => "FPFITTING",
0x3DC50D4i32 => "NBRWLD",
0x112C72E6i32 => "CCHDAT",
0x3E695Ei32 => "CSURF",
0x112C96DBi32 => "ATTDAT",
0xE55A534i32 => "POINSP",
0xEBEADBi32 => "SPMBAA",
0xE554Bi32 => "INSTRUMENT",
0xBE598i32 => "TMPLATE",
0x676C46Bi32 => "DBRANG",
0x2CDA24i32 => "SCIND",
0x4F70D29i32 => "HPATTERN",
0xCBFE9i32 => "SDLOAD",
0x3DC3E08i32 => "HMKWLD",
0xDB38Ei32 => "VSPR",
0x3E6F451i32 => "FMBEND",
0xC274Fi32 => "VOLMODEL",
0x9D4A7i32 => "PTSET",
0x4F01D0i32 => "RPATH",
0xD1203E1i32 => "GSUPFO",
0x8221Ci32 => "MDB",
0x380A7B2i32 => "HYLOAD",
0xB0AE071i32 => "SYGPWL",
0x4EB2F47i32 => "RUNGSET",
0xDFD41i32 => "PPOS",
0xE629Fi32 => "SEXTRUSION",
0x60F9892i32 => "SCDIAGRAM",
0x9D5D3i32 => "SDTEXT",
0x6E1904i32 => "SPOOL",
0x926DAi32 => "SSLCYLINDER",
0x88CB6i32 => "FPLANE",
0xFEDE3FEi32 => "SEQWOR",
0xAFC67i32 => "TPOISSON",
0xB1C6B97i32 => "LBSTYL",
0x88B64i32 => "SCLAMP",
0xF30B122i32 => "MNRNSQ",
0xB711E3i32 => "ASNOUT",
0x11C4BACEi32 => "HYHYST",
0xD8914i32 => "BVAREA",
0xD9546i32 => "SBFRAMEWORK",
0x3DC4A18i32 => "RSOWLD",
0xE49F4i32 => "VNOTE",
0xC2E94i32 => "TCOMPONENT",
0x17F623Ci32 => "SPMGSA",
0x4E232C1i32 => "HYPDRE",
0x17660DBi32 => "HANDRA",
0xAD910i32 => "RECIPIENT",
0xE55B6i32 => "HRST",
0x3DC63ABi32 => "DSXWLD",
0x3E72234i32 => "HPREND",
0x856061i32 => "HIBLO",
0x1575B697i32 => "INSLAY",
0x128A46B2i32 => "TABHQUESTION",
0x25DA268i32 => "SPMRSB",
0x8DC790i32 => "STAMP",
0xF37EAi32 => "ACRW",
0xC6BA1i32 => "POINT",
0x468C7ABi32 => "PPIECE",
0xA7B2280i32 => "STRWELL",
0xC551Ci32 => "BRANCH",
0x862B8E1i32 => "HBRSTI",
0x10140C8Di32 => "CPROTR",
0xCDC7Di32 => "REVOLUTION",
0x117776Ei32 => "SPMLFA",
0xAFC66i32 => "SPOISSON",
0x3D79E3i32 => "MPROF",
0x10B33F76i32 => "AITEMS",
0x2C358Fi32 => "FIELD",
0xAE398C7i32 => "CTMTRL",
0x3778A4i32 => "CURVE",
0x11C1D0B8i32 => "HYPOST",
0xC7C26D2i32 => "REVCON",
0x9D13Ai32 => "CORE",
0x82AC9i32 => "TEE",
0xCF0C10Ei32 => "CPANBO",
0x17E2EB0i32 => "SPMCSA",
0xC5F17i32 => "SDENSITY",
0xF7C34i32 => "NBOX",
0x334936Ei32 => "LNDESC",
0xE3803i32 => "SFITTING",
0xB054Fi32 => "ETRIANGLE",
0x12A08721i32 => "JLDATUM",
0xE355969i32 => "REVNOP",
0xDCCD4i32 => "LPYRAMID",
0xF8ECEF7i32 => "SLADDR",
0x4F3D045i32 => "ENGITE",
0x11C8017Bi32 => "ELFITTING",
0x3D9C8D2i32 => "LNFOLD",
0x13E497i32 => "HIFLA",
0x8703Ai32 => "DPBA",
0x4F177A8i32 => "MPLATE",
0xB0932i32 => "ACTI",
0xCD152i32 => "UGROUP",
0xAFB3BD6i32 => "CRERULES",
0x8219495i32 => "HYTRLI",
0x10188151i32 => "GENCUR",
0xFD22CC0i32 => "HPILLR",
0x256660i32 => "GENPC",
0x9E5E79Bi32 => "REVLNK",
0xDB051i32 => "CPORT",
0xA79A155i32 => "TMRRELEMENT",
0x54F991i32 => "ACYLINDER",
0xBC174i32 => "BVCL",
0xE4B7B73i32 => "CYMGRP",
0xE52FEi32 => "NSRTORUS",
0xA5A3570i32 => "STAVAL",
0x10AA32ADi32 => "MTPBLS",
0xAD1596Bi32 => "SHTMPL",
0xB3F95i32 => "SELJOINT",
0x912EEi32 => "VSECTION",
0xFA85Bi32 => "DPCYLINDRICAL",
0xBF45Ai32 => "RRULE",
0x81DB5i32 => "TP",
0x3DC32BCi32 => "ENGWLD",
0xAB9B8i32 => "MESH",
0x144A2A3Ei32 => "CPINRW",
0x986AAC5i32 => "FMSSBK",
0xF3A28E9i32 => "MRESTQ",
0x81F4Bi32 => "UDA",
0x3D6F83Ci32 => "FMWELD",
0xD2F14i32 => "TEXTPRIMITIVE",
0x11C21AC2i32 => "HYOPST",
0xE97FEi32 => "TYOUNG",
0x1014D202i32 => "HYFRTR",
0x6FE3111i32 => "HNOTCH",
0x17DE1CDi32 => "SPMBSA",
0x45D624i32 => "GENPG",
0xDFD48i32 => "WPOS",
0x13D81D37i32 => "APPLDWORLD",
0xB725Ai32 => "BACKINGSHEET",
0x24825Fi32 => "LCOMC",
0x34F708i32 => "SLINE",
0xE5479i32 => "OFST",
0xBFE2Ai32 => "SCYLINDER",
0xEC37BEi32 => "SPMCAA",
0xAF254i32 => "FBLIND",
0x54BC3Ai32 => "LOCLI",
0xAC0189i32 => "DBSET",
0x87313i32 => "DPCARTESIAN",
0xD0B67i32 => "MARKPRIMITIVE",
0xB1488i32 => "NBOXING",
0xC1F0Fi32 => "PRIM",
0x57503Ci32 => "HISTI",
0xA0699F8i32 => "CPRMRK",
0xAD08F4Fi32 => "KICKPL",
0x1093817Bi32 => "STAHIS",
0xEC0FB43i32 => "HYSTEQ",
0xB1C6CB8i32 => "DMSTYL",
0x14116FAEi32 => "STLNKW",
0xD9485i32 => "OVERLAY",
0x116CF9A5i32 => "HYCCIT",
0x121E3833i32 => "HYGZCU",
0x4C0CFFCi32 => "GPLANE",
0x11EF67i32 => "HISEA",
0xCFA2299i32 => "HYGRCO",
0x9D2EBi32 => "DDSE",
0x11CE72B5i32 => "HPRCUT",
0xFA248i32 => "OLAYER",
0xFA4CDi32 => "LIBY",
0xAD15A6Ai32 => "DRTMPL",
0xE55F4i32 => "PTST",
0x9EC031i32 => "FLOOR",
0x1465E7Ai32 => "HBRFLA",
0xF7A283Di32 => "SYGPAR",
0xF9D398Ai32 => "VLAYER",
0xBD8F3i32 => "WALL",
0x10AF7620i32 => "HYCTLS",
0x1074F4F5i32 => "OLINESTYLE",
0xA708F98i32 => "FEMODL",
0x1575B385i32 => "FLRLAY",
0xF01F45i32 => "SPMPAA",
0x9CAF3i32 => "PIPE",
0x121BC5B7i32 => "HYCRCU",
0x4EDEDD0i32 => "TMRPSET",
0x16052A61i32 => "GRIDSYSTEM",
0x4B1AB18i32 => "PDAELE",
0x127D1E20i32 => "SCGROUP",
0x3E6962i32 => "GSURF",
0x90736i32 => "SPACER",
0x128E1317i32 => "MNSTQU",
0x907CDi32 => "HVAC",
0x13C18B5Ei32 => "MPLRAW",
0x98D7E0Ei32 => "BBLOCK",
0x97C22i32 => "HROD",
0xEEEBB9i32 => "SPMLAA",
0x3DC5847i32 => "DSTWLD",
0xE4B6936i32 => "ENGGRP",
0x9B855i32 => "BVIEW",
0xBD063i32 => "RAIL",
0x1CC4262i32 => "RPLTAB",
0x1495703i32 => "HDOPLA",
0x3C080FAi32 => "UVALID",
0xDB1A5i32 => "SAPROPERTY",
0xEA001i32 => "STRUCTURE",
0x67B3B61i32 => "CLNPNGRID",
0xAF361i32 => "ELLIPSE",
0xA099AAAi32 => "MNRWRK",
0xA1E48i32 => "SPRFILE",
0x856060i32 => "GIBLO",
0x9D499i32 => "BTSET",
0x222D41Ei32 => "TASKLB",
0xE56BEi32 => "BATTERY",
0xE55B1i32 => "CRST",
0xC164D5Bi32 => "PBSOBN",
0x9E5D007i32 => "CYMLNK",
0xD8874i32 => "DPAREA",
0xDB1A6i32 => "TAPROPERTY",
0xE4723i32 => "CONTYPE",
0xDBEF0i32 => "TSTRESS",
0x553C068i32 => "RESDEF",
0x97418i32 => "BWLD",
0x506E2DAi32 => "HCURVE",
0x6980B4Bi32 => "SPLDRG",
0x9BF1Bi32 => "HELEMENT",
0x84618i32 => "RUNDECK",
0xE4B8D47i32 => "DETGRP",
0xC4E0E13i32 => "CEILIN",
0xBF71168i32 => "DESSYMBOL",
0x11C0E0D3i32 => "TRMLST",
0x2B7CDB9i32 => "TRSUCCESS",
0x553D10Ci32 => "LAYDEF",
0xF2829i32 => "ROLWL",
0x97247i32 => "WELD",
0x11BE9919i32 => "DSXDST",
0xD330BBi32 => "TRDAY",
0xC4E0DEDi32 => "SCILINE",
0xF3A8Fi32 => "CASWORLD",
0x4EF2467i32 => "POSTSE",
0x11757BE9i32 => "MWLDJT",
0x707ACAi32 => "GTMWL",
0xB6F462i32 => "HIDOU",
0x3F1FC65i32 => "REVNOD",
0xCF94A2Di32 => "HYLOCO",
0xE53E6i32 => "CASTYPE",
0x112C6D79i32 => "REFDAT",
0x1CC4A82i32 => "SLOTAB",
0xC40CCi32 => "MNUM",
0xEC09F10i32 => "CONSEQ",
0xDF1A3i32 => "LNKSET",
0xC2FD6i32 => "ROOM",
0x85897i32 => "AHU",
0x175DA10i32 => "GSTBRA",
0xCD546i32 => "GRSO",
0xED6B4Ai32 => "SPMGAA",
0xD245Bi32 => "BLTP",
0xD0F45i32 => "DAMPER",
0x912EDi32 => "USECTION",
0x9D2EAi32 => "CDSE",
0xDF525i32 => "STLS",
0xE3800i32 => "PFITTING",
0xBEA29i32 => "ACRL",
0xF3D72i32 => "MATWORLD",
0xCF0C113i32 => "HPANBO",
0x9A831i32 => "ADDENTRY",
0x9A45Di32 => "TUBE",
0xED9D0i32 => "VALVE",
0xA4676i32 => "RSEG",
0xDB1C0i32 => "SBPROPERTY",
0xA7E5BE0i32 => "MPKGFL",
0xDB1DCi32 => "TCPROPERTY",
0x6E044Di32 => "HIHOL",
0x11519566i32 => "VLYSET",
0xE62A0i32 => "TEXT",
0xF770D02i32 => "TRYEAR",
0x119773E4i32 => "GPOINT",
0x9D4AAi32 => "STSECTION",
0xAEF165Ei32 => "TSTDTL",
0x41C0889i32 => "REVSTD",
0xCFB1F59i32 => "TROUCOMMAND",
0x8AA62DFi32 => "MOGOBJ",
0xF7D2C1i32 => "HYCOBA",
0xDCCD6i32 => "NPYRAMID",
0x429676i32 => "SCSEGMENT",
0x366844i32 => "PCDSE",
0xF2AB23Bi32 => "INSURQ",
0xAF25Bi32 => "MBLIST",
0x8F311i32 => "SNUB",
0x13F282FDi32 => "TRMSGW",
0xCCA3480i32 => "PBSTXN",
0xE26D2i32 => "SECTION",
0xB696AA1i32 => "HYAGHM",
0xC3021BBi32 => "SCSTENCIL",
0x632DD4Ai32 => "ISOREGI",
0xAFC5Ci32 => "IPOINT",
0x112CBB17i32 => "PLFEAT",
0xAFB7308i32 => "LAYRUL",
0xBBCF760i32 => "HYFORM",
0x11B5802Ci32 => "MWPART",
0xB072184i32 => "REVCWL",
0xF89CB08i32 => "CPINCR",
0xE4B9458i32 => "DRVGRP",
0xF817EFDi32 => "ASSMBR",
0xB09D57Fi32 => "REVLWL",
0x11B62049i32 => "PBSCRT",
0x88CC2i32 => "RPLANE",
0xC505F71i32 => "COATING",
0xAB956i32 => "WASH",
0x2A30046i32 => "POLFACE",
0x180E2ABi32 => "SPMLSA",
0x10153EDEi32 => "LDRSTR",
0x32076Bi32 => "HNODE",
0x9545Fi32 => "HSADDLE",
0x553CA63i32 => "HSVDEF",
0xC64046Ci32 => "HOLDLN",
0xDF1E0F0i32 => "ASDFGP",
0xBFA40i32 => "PTWLD",
0xBFE25i32 => "NCYLINDER",
0x10EF433Ei32 => "LOOPTS",
0xB1C6BA7i32 => "ACSTYLE",
0x18AACECi32 => "SSBRTA",
0xEC03E23i32 => "CNGREQ",
0x8AC85i32 => "VTWAY",
0xAC9CCFAi32 => "HSPOOL",
0xE531Ei32 => "STRT",
0x67A1927i32 => "INTLNG",
0x171C3AD8i32 => "SCNOZZLE",
0x553C2C7i32 => "DATDEF",
0x1068E6C0i32 => "TREADSET",
0x1C1B38i32 => "ISOLB",
0x978BEi32 => "DIAMOND",
0x453C5Ei32 => "GENNG",
0x1288E44Bi32 => "MPLCQU",
0xBF44Fi32 => "GRULE",
0xCD696i32 => "SCTORUS",
0xBE197i32 => "UBOLT",
0xAF252i32 => "DBLI",
0x251537i32 => "TRLOCATION",
0xBBC76i32 => "TABLE",
0xA069FB4i32 => "MPTMRK",
0x3DC43EBi32 => "COMWLD",
0x1110544i32 => "SCAREA",
0xD21F0i32 => "DPSPHERICAL",
0xE0779i32 => "MESS",
0x4EBD447i32 => "CTRISE",
0x9D6F7i32 => "NOTE",
0xE2847i32 => "NSCTORUS",
0x8D937i32 => "PLLB",
0x11A8BE1Ai32 => "HYCMPT",
0x9ECF0Bi32 => "ARTORUS",
0xCED087Bi32 => "HBRABO",
0xDF4BFA2i32 => "ASSOGP",
0xBBAACC2i32 => "HYPGRM",
0x10E51FFFi32 => "REVISS",
0x10C72FA1i32 => "CTCROS",
0x51A03A5i32 => "DSLAYER",
0x2B2DDEi32 => "ATTHD",
0x5A90473i32 => "WLPROF",
0x48B9135i32 => "HYDMGE",
0xE5DFDF8i32 => "HYCOTP",
0xD8730i32 => "DDAREA",
0x4C0DB97i32 => "HRPANE",
0x182DB9Ci32 => "HYASSA",
0xB03332i32 => "ACRST",
0xD9489i32 => "SVERTEX",
0x4B1D777i32 => "HTPELE",
0x112CBB75i32 => "BPFEAT",
0x11C0DCF9i32 => "FILLSTYLE",
0xB70AA07i32 => "FMEDIM",
0xCF62D24i32 => "FACECODE",
0xBFFF5i32 => "STYLE",
0xA5A14i32 => "RPLGROUP",
0xBF9CBi32 => "GPWLD",
0x112CBBB1i32 => "HRFEAT",
0x11E765DFi32 => "BOTEXT",
0x11BFF717i32 => "PPLIST",
0x346DDCi32 => "CHOLE",
0x7D7EED2i32 => "SCOPCI",
0xAB41BEi32 => "RSECT",
0x7AE30EBi32 => "NPOLYHEDRON",
0x115155B8i32 => "ACCSET",
0x10AA3DA5i32 => "MPTBLS",
0x11A95002i32 => "SDAOPT",
0xE5570i32 => "SOST",
0x6261F2i32 => "BLOCK",
0x1EDBCAi32 => "SCTUBING",
0x4B1BD1Ai32 => "IMGELE",
0xB6EEB0i32 => "HIBOU",
0x81C2Bi32 => "DB",
0x8D9A5i32 => "RPLB",
0x861E0i32 => "BOX",
0x14D9A8D2i32 => "INCFIX",
0x46FA4D3i32 => "AREADEF",
0xAF3CCi32 => "DPLINE",
0x6793D58i32 => "FIXING",
0x267ECC7i32 => "LSTYTB",
0xAF0C5i32 => "LNKITEM",
0x1496D84i32 => "RAWPLA",
0x9D3E1i32 => "GMSET",
0x10173Di32 => "MNOZZLE",
0xEA3A3i32 => "DATUM",
0x9A821i32 => "LCDESCRIPTOR",
0xE29B5i32 => "BFDT",
0x6D08F4i32 => "DBALL",
0x25A54A7i32 => "SPMGSB",
0x1353AF77i32 => "FLRCOV",
0xB074AB6i32 => "GRIDWLD",
0xC7B76i32 => "SCONE",
0x66BB82i32 => "HMARK",
0x4EBC877i32 => "CPNISE",
0xC3F22i32 => "SYTM",
0xAF279i32 => "PCLIP",
0x71E452i32 => "CSEAM",
0xBFA21i32 => "LINESTYLEWORLD",
0x8628EE6i32 => "GICSTI",
0xCD826i32 => "NRTORUS",
0x3D71DA4i32 => "XPIFLD",
0x9577Ai32 => "TUBDATA",
0xA798Fi32 => "DRWG",
0xB0A2EAi32 => "HICUT",
0xCD243i32 => "SPROFILE",
0x11B5D534i32 => "SSSBRT",
0x251377i32 => "DBLOC",
0xE558Bi32 => "SPST",
0xB1C6BAAi32 => "DCSTYLE",
0xA5D81Ei32 => "XCELS",
0xF32023i32 => "SPMZAA",
0xD9805i32 => "TAGRULE",
0x4F3C0B8i32 => "TABITEM",
0x7AE30DEi32 => "APOLYHEDRON",
0x20F56Di32 => "HYSAC",
0x9BF92i32 => "SILENCER",
0x8D9DDi32 => "TRLB",
0x11BB180Ai32 => "SRFTRT",
0xB0D03i32 => "FLUID",
0xC4EF866i32 => "WLJOIN",
0xAFBC4i32 => "SJOINT",
0xC4E00C8i32 => "BNDLIN",
0x3D79D9i32 => "CPROF",
0xDB1C1i32 => "TBPROPERTY",
0xBA2BBF4i32 => "DSXHOM",
0xC77701Ai32 => "ELCONN",
0x97BBEi32 => "PNODE",
0x1013A5FBi32 => "POINTR",
0x39E2567i32 => "SCREED",
0xA783DA4i32 => "GPANEL",
0xC6BB4i32 => "HPIN",
0xE0911i32 => "PTSSET",
0xCC729i32 => "LSNOUT",
0xADCF3i32 => "NODISPLACEMENT",
0x10EE9AF5i32 => "WLJNTS",
0x11991FFEi32 => "HYCONT",
0x4659508i32 => "SCSUBEQUIPMENT",
0x10E070Ai32 => "TABHEADER",
0x1851715i32 => "SPMZSA",
0xAC9D7DCi32 => "MNTOOL",
0x6FDFE52i32 => "DSXSCH",
0x1199280Ei32 => "TTFONT",
0x71E457i32 => "HSEAM",
0xBF6EB88i32 => "AXESYMBOL",
0xC1D91i32 => "LDIMENSION",
0x43866F3i32 => "CNGFXD",
0xC53DEi32 => "HFAN",
0x4B1E2ADi32 => "PRTELE",
0xE9734i32 => "GROUP",
0x175DA11i32 => "HSTBRA",
0xC15B9EDi32 => "THUMBN",
0x112F4763i32 => "INSMAT",
0xF2B47i32 => "FRMWORK",
0x118AAFAi32 => "SPMPFA",
0xBF9ACi32 => "COWL",
0xF7C39i32 => "SBOX",
0x3DC53AFi32 => "PBSWLD",
0x1036ACi32 => "NOZZLE",
0xC83F477i32 => "HSUBPN",
0xA136Ai32 => "RUNFILE",
0x4104D66i32 => "MPTGSD",
0x9DCC9i32 => "SPVERT",
0xAF39265i32 => "TWRSTL",
0x4C0CFF8i32 => "CPLANE",
0xC2EE3C2i32 => "WLOPEN",
0x782E8AFi32 => "MPLCTH",
0x11C16F40i32 => "DSINST",
0x119773E5i32 => "HPOINT",
0xE74B4i32 => "DOCU",
0xFD41679i32 => "POSRLR",
0xF3348i32 => "CMPWORLD",
0x11C36749i32 => "DSXTST",
0xC06C6i32 => "IDAM",
0x88F3Bi32 => "CMMA",
0x9D395i32 => "LJSE",
0x3DC32DFi32 => "MOGWLD",
0xBF96Ai32 => "RLWLD",
0xC830033i32 => "HYPZON",
0x112EEC07i32 => "CLNLATTICE",
0xE4B58D0i32 => "STAGRP",
0x15CBD87Di32 => "ASMBLY",
0x10BD696Ci32 => "MAPLNS",
0xF6185i32 => "NSEXTRUSION",
0xC0E7F01i32 => "GISPAN",
0x133ACC5Ci32 => "SCVALV",
0xFA7FEi32 => "SLCYLINDER",
0xE4BC8i32 => "DEPT",
0xDF69Ai32 => "NGMSET",
0xFABFB88i32 => "UDETGR",
0x3E7222Fi32 => "CPREND",
0xAE98FBi32 => "PAINT",
0xCA7D8i32 => "NSCONE",
0x11BEC87Ci32 => "LINESTYLE",
0xCEF1Ei32 => "HTAP",
0x1CC51FFi32 => "SBRTAB",
0xA734Ai32 => "SLUG",
0xE97FDi32 => "SYOUNG",
0xFB75215i32 => "GLYCIRCLE",
0x788177Bi32 => "MNSTTH",
0xC2EE3BBi32 => "PLOPEN",
0x7CF1291i32 => "HPANBI",
0x98E58EDi32 => "HYGRCK",
0xA799064i32 => "COLRELATION",
0xAE264i32 => "CMFITTING",
0xC1D95i32 => "PDIMENSION",
0x8A3E5i32 => "ATTACHMENT",
0x8E9027i32 => "PEROP",
0x4ED4ED3i32 => "HRPNSE",
0x2C370Ci32 => "IWELD",
0x3BF275Bi32 => "ULOGID",
0xB24CBi32 => "SUBJOINT",
0xBF987i32 => "TEAMWORLD",
0xE35899Di32 => "SCLOOP",
0x9D6C6i32 => "SMTEXT",
0xB07D654i32 => "ASDFWL",
0x408310Ci32 => "PLTGRD",
0xFEB6EB4i32 => "CFLOOR",
0x2A31726i32 => "MPTFAC",
0xD163Ai32 => "CMOP",
0x11C42C6Ci32 => "HYLWST",
0x119773E0i32 => "CPOINT",
0x3DC41F4i32 => "MWLWLD",
0x83CCAi32 => "LNK",
0x4F177A3i32 => "HPLATE",
0x8A143i32 => "BVSAREA",
0xAFC65i32 => "RPOINT",
0xAC0306i32 => "GPSET",
0xC665E0i32 => "GROLWL",
0xE38DDi32 => "UNIT",
0xE2DF8i32 => "MSET",
0x3DC38B7i32 => "XPIWLD",
0x3DC5838i32 => "PRTWLD",
0x9CFBFi32 => "BAREA",
0x55D610Ci32 => "CSTIFF",
0xF0919i32 => "DRAWING",
0xBE18Fi32 => "MBOLT",
0xE21CC25i32 => "INSCMP",
0x8518F42i32 => "GENPRI",
0x4B1C482i32 => "OBJELE",
0x912D0i32 => "SRECTANGLE",
0x95779i32 => "SUBDOCU",
0x97E6Fi32 => "CMPDATA",
0x11515450i32 => "SPBSET",
0xFAD1253i32 => "DBVWGROUP",
0x9A45Ci32 => "SUBEQUIPMENT",
0x114D1Ei32 => "PIPCARTESIAN",
0xA75121Bi32 => "SPMCEL",
0xCC72Bi32 => "NSNOUT",
0x712865i32 => "HSTYLE",
0x3DC42E0i32 => "FEMWLD",
0xDFAA2i32 => "TRNS",
0x11996A88i32 => "ACCPNT",
0x9E4E565i32 => "LNLINK",
0xCF84D4Ei32 => "SCELCONNECTION",
0xE5AFCi32 => "HNUT",
0x8754D9i32 => "MBPRO",
0x871BCi32 => "LCCARTESIAN",
0x10720717i32 => "SCPDESTINATION",
0x18B3148i32 => "REVSTA",
0x6D0B2Ei32 => "GWALL",
0xCC3A5i32 => "CMMO",
0x267ECC1i32 => "FSTYTB",
0x2326298i32 => "NOMINB",
0x557CB70i32 => "HRDREF",
0xCD547i32 => "HRSO",
0x12A08727i32 => "PLDATUM",
0xEC6F7i32 => "CLEVIS",
0x28E8CFi32 => "TREAD",
0x10A5A602i32 => "STLNKS",
0x89E45i32 => "STRAIGHT",
0xB0AB506i32 => "ASSOWL",
0xA61F9EBi32 => "LAYTBL",
0x9129Ai32 => "SPECIFICATION",
0xE5561i32 => "DOST",
0x4F16904i32 => "RLGATE",
0x37D0EFFi32 => "SPMCAD",
0x4F01230i32 => "DBVWSET",
0xBEC59i32 => "UWRLD",
0xC941E3Ci32 => "MTPBRN",
0xD3E151i32 => "ASLCYLINDER",
0x9C5EDi32 => "ZONE",
0x487F263i32 => "RLCAGE",
0x25AA18Ai32 => "SPMHSB",
0x67101A8i32 => "FEMIMG",
0xF2E7Di32 => "RUNWORLD",
0x65A2406i32 => "CPINJG",
0x2C75C2Ci32 => "GENSEC",
0x98567i32 => "EYRD",
0x11A8DDF4i32 => "HCOMPT",
0x8628EE7i32 => "HICSTI",
0x9ECD76i32 => "ACTORUS",
0xC6B133i32 => "LCOMW",
0x8DD72i32 => "SYMBOL",
0x11700A47i32 => "ULIMIT",
0xE4CEEi32 => "APPT",
0xB0D22E1i32 => "DBVWWLD",
0xCA439i32 => "ELBOW",
0xBF450i32 => "HRULE",
0x1147690i32 => "SPBPFA",
0x6331179i32 => "CAGSEG",
0x9C53Di32 => "LINE",
0xD0731A1i32 => "EXTGEO",
0x10B85F4i32 => "HYSZDA",
0xBBA6A1Bi32 => "INTFRM",
0x8BAB8i32 => "LTABLE",
0xAF435i32 => "ATLIST",
0xEA22Ei32 => "INSULATION",
0x1141BA87i32 => "SCDUCT",
0x11C8018Di32 => "WLFITTING",
0xAD15A85i32 => "DSTMPL",
0x18DB92i32 => "BRTAB",
0x11CE72B0i32 => "CPRCUT",
0xE4CFFi32 => "RPPT",
0x14D9C561i32 => "COMFIXING",
0x4B6A98Ai32 => "DCRULE",
0x483C414i32 => "OPENFE",
0x9D08Ei32 => "THREEWAY",
0x1431DBF9i32 => "UNKNOWN",
0x54D2E7i32 => "LNKLI",
0xF2DCCi32 => "CONWORLD",
0xFCCFEi32 => "NLPYRAMID",
0xBF9D8i32 => "TPWLD",
0xFA6FADCi32 => "CLNCGR",
0x9C033i32 => "ROLE",
0x9554Di32 => "CABDATA",
0x18B2B97i32 => "SETSTATUS",
0x1C728CAi32 => "SCMCABLE",
0xDEBB1i32 => "BLIST",
0xAF38Bi32 => "TMLI",
0x3F49D4i32 => "INSUF",
0xCCB67i32 => "REPORT",
0xA0694BCi32 => "MTPMRK",
0x2C3710i32 => "MWELD",
0xAC9CD02i32 => "PSPOOL",
0xA783D9Fi32 => "BPANEL",
0xBD223i32 => "GRILLE",
0x1151824Ci32 => "STRSET",
0x3DC42ACi32 => "HCMWLD",
0x4D8542Di32 => "PRTYPE",
0xBBA6B25i32 => "EXTFRM",
0xBF979i32 => "FMWLD",
0x9D104i32 => "CMRE",
0x7A238Ci32 => "RBRAN",
0x11C1CFFBi32 => "HRPOST",
0xFD40812i32 => "ANNRLR",
0xDBF68i32 => "EXTRUSION",
0xBF9D7i32 => "SPWLD",
0xC942934i32 => "MPTBRN",
0xBEB83i32 => "WORLD",
0xAFC57i32 => "DPOINT",
0x9541Di32 => "WPAD",
0xC4EF92Ai32 => "CTJOIN",
0x5A9049Fi32 => "MNPROF",
0xC7867i32 => "SANNULUS",
0xF0A81i32 => "MDBW",
0xE26D389i32 => "SPLTMP",
0xD709C56i32 => "DSTGRO",
0xDD0FE02i32 => "CGRDCP",
0xC06ECi32 => "TEAM",
0xC5F18i32 => "TDENSITY",
0x8F584Bi32 => "FMGRP",
0x9D49Bi32 => "DTSET",
0xD9DF5i32 => "ADIRECTION",
0x11BFF760i32 => "HSLIST",
0x4EE7DECi32 => "CCORSET",
0xCD684i32 => "ACTO",
0x11C0F9FDi32 => "INVLST",
0xA0513i32 => "STIFFENER",
0xC2E90i32 => "PCOMPONENT",
0xAB7D17Ei32 => "LCTIML",
0x11993BC7i32 => "TRMONTH",
0x112CBBE2i32 => "CTFEAT",
0x9C5D6i32 => "CONE",
0x11D211ACi32 => "HCTOUT",
0x8ADBBi32 => "HEXAGON",
0xC547Ei32 => "FLANGE",
0x3DC2256i32 => "STAWLD",
0x2C3715i32 => "RWELD",
0x87AB03Ai32 => "MPLRWI",
0x3D79DDi32 => "GPROF",
0x11C0A05Bi32 => "MARKSTYLE",
0x2C6A07i32 => "STWLD",
0xAD7E9i32 => "TUBING",
0xB0D89i32 => "EQUIPMENT",
0x11C2DB20i32 => "FMBSST",
0x10EB3A95i32 => "HYCCTS",
0x3E6F50Bi32 => "CTBEND",
0x11E74F28i32 => "SOLEXT",
0xA783DAAi32 => "MPANEL",
0x1666475i32 => "HYGEPA",
0x3DC232Fi32 => "TABWLD",
0xBFA2Ai32 => "USERWORLD",
0x24CC9Ai32 => "GENNC",
0x267ECC8i32 => "MSTYTB",
0x4B1AF11i32 => "GOBELE",
0xEC7A9i32 => "SREVOLUTION",
0x114C373i32 => "SPMCFA",
0x117071AFi32 => "FURNIT",
0x11D220DBi32 => "GLYOUTLINE",
0x114A894Di32 => "HYLWDT",
0x1071E346i32 => "COCDES",
0x7429BC3i32 => "OBJELH",
0x6D7DD8i32 => "LCOML",
0xC6B9Bi32 => "JOINT",
0x3E03E26i32 => "LCTIMD",
0xCA7B1i32 => "BRCO",
0xF818D3Di32 => "DSXMBR",
0xDE12EA3i32 => "ISODEPT",
0x11C0F1E9i32 => "TRSLST",
0x2A3033Di32 => "SPMFAC",
0x149355Di32 => "GICPLA",
0xBFA16i32 => "ASWLD",
0xC6BAFi32 => "CPIN",
0xE4833i32 => "EYENUT",
0xD88C3i32 => "BSAREA",
0x55C742i32 => "HIPOI",
0x4ECA60i32 => "ADISH",
0x11C2CE7Ei32 => "LAYRST",
0x19FB0Ai32 => "SPMEB",
0x82663i32 => "ARC",
0xE3F888Ai32 => "CTSUPP",
0x1CC451Ci32 => "NOMTAB",
0xDEAEAi32 => "SDISK",
0x17580FEi32 => "TKPARA",
0xA755BFFi32 => "HOLDEL",
0x553C1D1i32 => "ASSDEF",
0x1672B5i32 => "HATTA",
0xF8C29i32 => "VRTX",
0xA9671DDi32 => "EXPFILTER",
0x11A118CBi32 => "CPRNOT",
0xC89A1i32 => "ACTN",
0xCD691i32 => "NCTORUS",
0xC4E21DCi32 => "SCPLINE",
0x3D7CEDi32 => "HRSOF",
0xBE195i32 => "SBOLT",
0x4E156E4i32 => "RSTAREA",
0xAE312EEi32 => "ATTRRL",
0x10140C91i32 => "GPROTR",
0x11C18899i32 => "MPRNST",
0xCC949i32 => "PLOOP",
0x926D5i32 => "NSLCYLINDER",
0xC87A39i32 => "SPMSW",
0xFA7F9i32 => "NLCYLINDER",
0x7825A17i32 => "MNPATH",
0xB551429i32 => "GPITEM",
0x76178Di32 => "XGEOMETRY",
0x115F6FFi32 => "SPMGFA",
0xC8DBFi32 => "BOUNDARY",
0xABA1Bi32 => "DISH",
0xA5E27i32 => "HANGER",
0x858A9i32 => "SHU",
0x15163AD0i32 => "CSURPX",
0x676B3C5i32 => "HFLANG",
0xA23B7i32 => "FONTFILE",
0xAEF4EBi32 => "HINOT",
0x71E456i32 => "GSEAM",
0x9DB31i32 => "PAVERT",
0xC0EE9D4i32 => "CWBRAN",
0x7A1E8Di32 => "HIPAN",
0xD707D03i32 => "DSIGRO",
0x8764Ei32 => "USDA",
0x12A11772i32 => "SCACTUATOR",
0x788CB4Ei32 => "MRAWTH",
0xE20F6i32 => "DDATA",
0x6D1487i32 => "XCELL",
0x7D4FD8Ai32 => "MPTFCI",
0x8B9E7i32 => "SLABEL",
0x8CB181i32 => "HCLIP",
0x8F3AEi32 => "NTUBE",
0x140111i32 => "HIPLA",
0xB023F9i32 => "STLST",
0x18A9A5i32 => "SCCABLE",
0x9DBA0i32 => "SEVERT",
0x4F177A2i32 => "GPLATE",
0x828FCi32 => "ROD",
0x15E8BAi32 => "APYRAMID",
0xA1835i32 => "CMPFITTING",
0xE4B8E40i32 => "JNTGRP",
0x3DC584Ai32 => "GSTWLD",
0x8DF8Fi32 => "TRNB",
0x9B4CDi32 => "POHEDRON",
0x154B05i32 => "TEXPANSION",
0xD358BEi32 => "CTRAY",
0xDD8C6i32 => "SUBSTRUCTURE",
0x350719i32 => "ACONE",
0x8B9DBi32 => "GLABEL",
0xFA704i32 => "LCCYLINDRICAL",
0xAE14Bi32 => "SBFITTING",
0xDD408i32 => "TCASE",
0x662E66Fi32 => "MANPKG",
0xE96D4i32 => "SNOUT",
0x26187CDi32 => "MWLDTB",
0x1079E78i32 => "SYSMDA",
0x115184E0i32 => "DRSSET",
0xC2A15i32 => "COMM",
0xDFA40i32 => "CONSTRAINT",
0x11C801B5i32 => "INFITTING",
0xBF9ADi32 => "DOWLD",
0xC0EE7C8i32 => "SCBRANCH",
0xA6DFDDi32 => "PTPOS",
0x9BF26i32 => "SELEC",
0x4EE81CCi32 => "WLPRSE",
0xD2F13i32 => "SEXPANSION",
0x557F7C7i32 => "SFTREF",
0x4C0DA01i32 => "GCPANE",
0xC83F476i32 => "GSUBPN",
0xAE29Ai32 => "COFITTING",
0x4B0C612i32 => "CTABLE",
0x3DC5DDEi32 => "DRVWLD",
0xB47E7i32 => "PCOJOINT",
0xE25392Bi32 => "VMCOMP",
0x4881676i32 => "SCPAGE",
0xCC94Ci32 => "SLOOP",
0x1672BFi32 => "RATTA",
0x35A0F1i32 => "SCOPE",
0x8D8CEi32 => "SHLB",
0xE084Bi32 => "GMSSET",
0xD337Bi32 => "MTYPE",
0x102DE14Bi32 => "STRTWR",
0x7C83C35i32 => "HYGRAI",
0x1155EF38i32 => "MPKGFT",
0x8D92Ai32 => "CLLB",
0xB1C6DD8i32 => "VWSTYL",
0x8AB0Bi32 => "VFWAY",
0xAFB74Bi32 => "GPART",
0xE55C2i32 => "TRST",
0xB1C6DF1i32 => "TXSTYL",
0x77B5ABi32 => "ISOTM",
0x4EC9F5Di32 => "RAILSET",
0xA94D461i32 => "TRFAILURE",
0xF2F71i32 => "SCOWL",
0x339FC89i32 => "HYDWSC",
0xE4B6087i32 => "WLDGRP",
0x4B1C0BEi32 => "WTHELE",
0xA7AAFAFi32 => "BLEVEL",
0xBBA69ECi32 => "PLTFRM",
0x754EEEi32 => "SVOLMODEL",
0xDDE3Di32 => "NSDSH",
0x4F1779Ei32 => "CPLATE",
0x4B1E7ECi32 => "INVELEMENT",
0xE19F97Bi32 => "CGRDLP",
0xCEA0FA0i32 => "GXTRAO",
0xB09723Ai32 => "LINKWLD",
0x128EC6EAi32 => "MRAWQU",
0xA0597Fi32 => "AEXTRUSION",
0x101DB949i32 => "FIXTUR",
0xE253911i32 => "WLCOMPONENTS",
0x1CC573Ai32 => "HYSTAB",
0xDB037i32 => "DOOR",
0x23D3C17i32 => "HYPROB",
0xC7B71i32 => "NCONE",
0x2A7C1D1i32 => "SCHVAC",
0x2E761D7i32 => "HYCRIC",
0x7F1FC08i32 => "SCHVFITTING",
0x560E06i32 => "GENPI",
0xEE047i32 => "CINVENTORY",
0xD9EC4i32 => "SKIR",
0x936D2i32 => "CIRCLE",
0x11BFF768i32 => "PSLIST",
0xB09DFA0i32 => "STYLWL",
0xCF99D54i32 => "SCOPCO",
0xD8833i32 => "TMAREA",
0xC0E5172i32 => "HICPAN",
0x11C5C192i32 => "SETATTRIBUTE",
0x1074805i32 => "APPLDATA",
0xBEEBFi32 => "NSSLCYLINDER",
0xDB0CCi32 => "RTORUS",
0xDC3799Ei32 => "MBRMAP",
0xB0C0C16i32 => "UDETWL",
0xFA365i32 => "CWAY",
0x1053DA88i32 => "LNCLAS",
0x14019Di32 => "MNPLA",
0x86A162Di32 => "SCEQUIPMENT",
0xDFD6Ai32 => "CROSS",
0xC839D3Ei32 => "HBRAPN",
0xAF71Di32 => "PTMIX",
0xB47EAi32 => "SCOJOINT",
0x11C0CCE4i32 => "TRFLST",
0xBFA4Di32 => "BUWLD",
0xDBF71i32 => "NXTRUSION",
0x326328i32 => "CTTEE",
0xB70DD49i32 => "FMWDIM",
0x506E2D5i32 => "CCURVE",
0xE2267A5i32 => "SCTEMPLATE",
0x15A685i32 => "HIBRA",
0x4B1D3D1i32 => "SLOELE",
0xCB86Ei32 => "UNION",
0x2D2B40Ci32 => "HYGCGC",
0xCA4FFi32 => "NSBOX",
0xD8C19i32 => "SWBR",
0x11A8DDF3i32 => "GCOMPT",
0x506E2D9i32 => "GCURVE",
0x60DF332i32 => "LDRCAGE",
0xA06B08i32 => "HICUR",
0x6FA34Ei32 => "DBSTL",
0xAC632DAi32 => "EXPCOLUMN",
0x6C4DC3i32 => "HIPIL",
0x8261Di32 => "LOC",
0x189E86Fi32 => "SSNOTA",
0x42964Bi32 => "CBSEGMENT",
0xE9580i32 => "CBOUNDARY",
0xB70A17Ci32 => "FMBDIM",
0x239E4E3i32 => "SPMGOB",
0xA7347i32 => "PLUG",
0xE2DECi32 => "ASET",
0x8D92Bi32 => "DLLB",
0xB55143Ai32 => "XPITEM",
0x86BD1Ai32 => "SUPPO",
0x12A08752i32 => "ENDATU",
0x93274i32 => "SUPC",
0xAD9F2i32 => "ANCI",
0xD706DB5i32 => "AIDGRO",
0xC4E0040i32 => "AIDLIN",
};

#[cfg(test)]
mod boundary_tests {
    use super::{
        padded_implicit_end, parse_ele_children, parse_ele_membs, parse_raw_ele_data,
        parse_raw_element_identity,
    };

    fn identity_record(noun_hash: i32) -> [u8; 24] {
        let mut input = [0_u8; 24];
        input[0..4].copy_from_slice(&6_i32.to_be_bytes());
        input[4..8].copy_from_slice(&17_u32.to_be_bytes());
        input[8..12].copy_from_slice(&29_u32.to_be_bytes());
        input[12..16].copy_from_slice(&noun_hash.to_be_bytes());
        input[16..20].copy_from_slice(&7_u32.to_be_bytes());
        input[20..24].copy_from_slice(&11_u32.to_be_bytes());
        input
    }

    fn record_with_terminal_padding() -> [u8; 168] {
        let mut input = [0_u8; 168];
        input[0..4].copy_from_slice(&40_i32.to_be_bytes());
        input
    }

    fn record_with_truncated_member_block() -> [u8; 28] {
        let mut input = [0_u8; 28];
        input[0..4].copy_from_slice(&4_i32.to_be_bytes());
        input[16..18].copy_from_slice(&2_u16.to_be_bytes());
        input[18..20].copy_from_slice(&3_u16.to_be_bytes());
        input
    }

    #[test]
    fn implicit_padding_can_reach_record_end() {
        let input = [0_u8; 12];
        assert_eq!(padded_implicit_end(&input, 4).unwrap(), input.len());
    }

    #[test]
    fn implicit_padding_stops_before_member_header() {
        let mut input = [0_u8; 12];
        input[8..12].copy_from_slice(&2_i32.to_be_bytes());
        assert_eq!(padded_implicit_end(&input, 4).unwrap(), 8);
    }

    #[test]
    fn implicit_padding_rejects_declared_end_beyond_record() {
        assert!(padded_implicit_end(&[0_u8; 8], 12).is_err());
    }

    #[test]
    fn member_scan_stops_at_record_end() {
        assert!(parse_ele_membs(&record_with_terminal_padding()).is_empty());
    }

    #[test]
    fn child_scan_stops_at_record_end() {
        let (_, children) = parse_ele_children(&record_with_terminal_padding());
        assert!(children.0.is_empty());
    }

    #[test]
    fn member_helper_ignores_truncated_member_block() {
        assert!(parse_ele_membs(&record_with_truncated_member_block()).is_empty());
    }

    #[test]
    fn child_helper_ignores_truncated_member_block() {
        let (_, children) = parse_ele_children(&record_with_truncated_member_block());
        assert!(children.0.is_empty());
    }

    #[test]
    fn minimal_identity_recognizes_mnum_when_canonical_decode_fails() {
        let input = identity_record(0xC40CC);
        let identity = parse_raw_element_identity(&input).unwrap();

        assert_eq!(identity.noun_name, "MNUM");
        assert_eq!(identity.noun_hash, 0xC40CC);
        assert!(parse_raw_ele_data(&input).is_err());
    }

    #[test]
    fn minimal_identity_does_not_classify_an_ordinary_noun_as_mnum() {
        let identity = parse_raw_element_identity(&identity_record(0x861E0)).unwrap();

        assert_eq!(identity.noun_name, "BOX");
        assert_ne!(identity.noun_name, "MNUM");
    }
}

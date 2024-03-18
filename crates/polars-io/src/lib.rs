#![cfg_attr(docsrs, feature(doc_auto_cfg))]
#![cfg_attr(feature = "simd", feature(portable_simd))]
#![allow(ambiguous_glob_reexports)]

#[cfg(feature = "avro")]
pub mod avro;
pub mod cloud;
#[cfg(any(feature = "csv", feature = "json"))]
pub mod csv;
#[cfg(feature = "parquet")]
pub mod export;
#[cfg(any(feature = "ipc", feature = "ipc_streaming"))]
pub mod ipc;
#[cfg(feature = "json")]
pub mod json;
#[cfg(feature = "json")]
pub mod ndjson;
#[cfg(feature = "cloud")]
pub use cloud::glob as async_glob;

pub mod mmap;
mod options;
#[cfg(feature = "parquet")]
pub mod parquet;
pub mod predicates;
pub mod prelude;
#[cfg(all(test, feature = "csv"))]
mod tests;
pub mod utils;
use once_cell::sync::Lazy;
use regex::Regex;

#[cfg(feature = "partition")]
pub mod partition;
#[cfg(feature = "async")]
pub mod pl_async;

use std::io::{Read, Write};
use std::path::{Path, PathBuf};

#[allow(unused)] // remove when updating to rust nightly >= 1.61
use arrow::array::new_empty_array;
pub use options::*;
use polars_core::frame::ArrowChunk;
use polars_core::prelude::*;

#[cfg(any(feature = "ipc", feature = "avro", feature = "ipc_streaming",))]
use crate::predicates::PhysicalIoExpr;

pub trait SerReader<R>
where
    R: Read,
{
    /// Create a new instance of the `[SerReader]`
    fn new(reader: R) -> Self;

    /// Make sure that all columns are contiguous in memory by
    /// aggregating the chunks into a single array.
    #[must_use]
    fn set_rechunk(self, _rechunk: bool) -> Self
    where
        Self: Sized,
    {
        self
    }

    /// Take the SerReader and return a parsed DataFrame.
    fn finish(self) -> PolarsResult<DataFrame>;
}

pub trait SerWriter<W>
where
    W: Write,
{
    fn new(writer: W) -> Self
    where
        Self: Sized;
    fn finish(&mut self, df: &mut DataFrame) -> PolarsResult<()>;
}

pub trait WriterFactory {
    fn create_writer<W: Write + 'static>(&self, writer: W) -> Box<dyn SerWriter<W>>;
    fn extension(&self) -> PathBuf;
}

pub trait ArrowReader {
    fn next_record_batch(&mut self) -> PolarsResult<Option<ArrowChunk>>;
}

#[cfg(any(feature = "ipc", feature = "avro", feature = "ipc_streaming",))]
pub(crate) fn finish_reader<R: ArrowReader>(
    mut reader: R,
    rechunk: bool,
    n_rows: Option<usize>,
    predicate: Option<Arc<dyn PhysicalIoExpr>>,
    arrow_schema: &ArrowSchema,
    row_index: Option<RowIndex>,
) -> PolarsResult<DataFrame> {
    use polars_core::utils::accumulate_dataframes_vertical;

    let mut num_rows = 0;
    let mut parsed_dfs = Vec::with_capacity(1024);

    loop {
        let remaining = match n_rows {
            Some(limit) => match limit.checked_sub(num_rows) {
                // We still need to read `remaining` rows.
                Some(remaining) => Some(remaining),
                // We have read enough since `num_rows >= limit`.
                None => break,
            },
            // Read until the end.
            None => None,
        };

        let Some(batch) = reader.next_record_batch()? else {
            break;
        };

        let mut df = DataFrame::try_from((batch, arrow_schema.fields.as_slice()))?;

        if let Some(rc) = &row_index {
            df.with_row_index_mut(
                &rc.name,
                Some(IdxSize::try_from(num_rows).unwrap() + rc.offset),
            );
        }

        if let Some(remaining) = remaining {
            if remaining < df.height() {
                df = df.slice(0, remaining);
            }
        }

        num_rows += df.height();

        if let Some(predicate) = &predicate {
            let s = predicate.evaluate_io(&df)?;
            let mask = s.bool().expect("filter predicates was not of type boolean");
            df = df.filter(mask)?;
        }

        parsed_dfs.push(df);
    }

    let df = {
        if parsed_dfs.is_empty() {
            // Create an empty dataframe with the correct data types
            let empty_cols = arrow_schema
                .fields
                .iter()
                .map(|fld| {
                    Series::try_from((fld.name.as_str(), new_empty_array(fld.data_type.clone())))
                })
                .collect::<PolarsResult<_>>()?;
            DataFrame::new(empty_cols)?
        } else {
            // If there are any rows, accumulate them into a df
            accumulate_dataframes_vertical(parsed_dfs)?
        }
    };

    Ok(if rechunk { df.agg_chunks() } else { df })
}

static CLOUD_URL: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"^(s3a?|gs|gcs|file|abfss?|azure|az|adl|https?)://").unwrap());

/// Check if the path is a cloud url.
pub fn is_cloud_url<P: AsRef<Path>>(p: P) -> bool {
    match p.as_ref().as_os_str().to_str() {
        Some(s) => CLOUD_URL.is_match(s),
        _ => false,
    }
}

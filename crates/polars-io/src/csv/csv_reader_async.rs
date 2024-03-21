use std::sync::Arc;

use futures::StreamExt;
use object_store::path::Path;
use object_store::ObjectMeta;
use polars_core::datatypes::IDX_DTYPE;
use polars_core::frame::DataFrame;
use polars_core::schema::Schema;
use polars_error::{polars_bail, polars_err, polars_warn, to_compute_err, PolarsResult};

use crate::cloud::{build_object_store, CloudLocation, CloudOptions, PolarsObjectStore};
use crate::mmap::ReaderBytes;
use crate::predicates::PhysicalIoExpr;
use crate::prelude::{materialize_projection, CsvReader};
use crate::RowIndex;

use super::parser::SplitLines;
use super::{CommentPrefix, CsvEncoding, NullValues};

/// An Arrow CSV reader implemented on top of PolarsObjectStore.
pub struct CsvReaderAsync {
    store: PolarsObjectStore,
    path: Path,
}

#[derive(Default, Clone)]
pub struct CsvReadOptions {
    // Names of the columns to include in the output.
    projection: Option<Vec<String>>,

    // The maximum number of rows to consider from the input.
    row_limit: Option<usize>,

    // Include a column with the row number under the provided name  starting at the provided index.
    row_index: Option<RowIndex>,

    // Only include rows that pass this predicate.
    predicate: Option<Arc<dyn PhysicalIoExpr>>,
}

impl CsvReaderAsync {
    pub async fn from_uri(
        uri: &str,
        cloud_options: Option<&CloudOptions>,
    ) -> PolarsResult<CsvReaderAsync> {
        let (
            CloudLocation {
                prefix, expansion, ..
            },
            store,
        ) = build_object_store(uri, cloud_options).await?;

        let path = {
            // Any wildcards should already have been resolved here. Without this assertion they would
            // be ignored.
            debug_assert!(expansion.is_none(), "path should not contain wildcards");
            Path::from_url_path(prefix).map_err(to_compute_err)?
        };

        Ok(Self {
            store: PolarsObjectStore::new(store),
            path,
        })
    }

    pub async fn fetch_bytes_for_schema_inference(&self, 
        infer_schema_length: Option<usize>,
    ) -> PolarsResult<Vec<u8>> {
        Ok(if let Some(max_read_rows) = infer_schema_length {
            let mut stream = self.store.get_stream(&self.path).await?;

            let mut acc = vec![];

            while let Some(item) = stream.next().await {
                let bytes = item?;
                acc.extend_from_slice(bytes.as_ref());
                if SplitLines::new(&acc[..], b',',b'\n').count() >= max_read_rows {
                    break;
                }
            }

            drop(stream);

            acc
        } else {
            polars_warn!("reading entire CSV to infer schema because `infer_schema_length` has not been provided.");
            self.store.get(&self.path).await?.to_vec()
        })
    }

    pub async fn data(
        &self,
        options: CsvReadOptions,
        verbose: bool,
    ) -> PolarsResult<DataFrame> {
        todo!();
    }
}

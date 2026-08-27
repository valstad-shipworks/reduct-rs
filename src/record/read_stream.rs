// Copyright 2026 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::http_client::map_error;
use crate::record::Record;
use async_stream::stream;
use bytes::{Bytes, BytesMut};
use futures::Stream;
use futures_util::StreamExt;
use reduct_base::batch::v3::{StreamDecoder, StreamItem, StreamRecord};
use reduct_base::error::{ErrorCode, ReductError};
use reqwest::Response;

/// Turns a v3 stream response into records. A record's payload is gathered before the
/// record is handed over, so no more than one record is held at a time.
pub(crate) fn decode_record_stream(
    response: Response,
) -> impl Stream<Item = Result<Record, ReductError>> {
    stream! {
        let mut decoder = StreamDecoder::new();
        let mut body = response.bytes_stream();
        let mut pending: Option<(StreamRecord, BytesMut)> = None;

        while let Some(chunk) = body.next().await {
            let chunk = match chunk {
                Ok(chunk) => chunk,
                Err(err) => {
                    yield Err(map_error(err));
                    return;
                }
            };
            decoder.feed(&chunk);

            loop {
                let item = match decoder.next_item() {
                    Ok(Some(item)) => item,
                    Ok(None) => break,
                    Err(err) => {
                        yield Err(err);
                        return;
                    }
                };

                match item {
                    StreamItem::Record(record) => {
                        if record.has_payload && record.content_length > 0 {
                            let capacity = record.content_length.min(1 << 22) as usize;
                            pending = Some((record, BytesMut::with_capacity(capacity)));
                        } else {
                            yield Ok(build_record(record, Bytes::new()));
                        }
                    }
                    StreamItem::Payload(payload) => {
                        let Some((record, data)) = pending.as_mut() else {
                            yield Err(ReductError::new(
                                ErrorCode::Unknown,
                                "Malformed record stream: payload without a record",
                            ));
                            return;
                        };
                        data.extend_from_slice(&payload);
                        if data.len() as u64 >= record.content_length {
                            let (record, data) = pending.take().unwrap();
                            yield Ok(build_record(record, data.freeze()));
                        }
                    }
                    StreamItem::Error(err) => {
                        yield Err(err);
                        return;
                    }
                    StreamItem::End => {
                        if pending.is_some() {
                            yield Err(ReductError::new(
                                ErrorCode::ConnectionError,
                                "Record stream ended mid-record",
                            ));
                        }
                        return;
                    }
                }
            }
        }

        yield Err(ReductError::new(
            ErrorCode::ConnectionError,
            "Record stream ended before the query finished",
        ));
    }
}

fn build_record(record: StreamRecord, data: Bytes) -> Record {
    Record::builder()
        .entry(record.entry)
        .timestamp_us(record.timestamp)
        .content_type(record.content_type)
        .labels(record.labels)
        .data(data)
        .content_length(record.content_length as usize)
        .build()
}

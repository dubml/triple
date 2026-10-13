// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Framing tests using independent wire vectors and every split boundary.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use bytes::Bytes;
use codec::{Error, FrameDecoder, encode_frame};

const WIRE: &[u8] = &[0, 0, 0, 0, 3, 10, 1, b'A'];

#[test]
fn independent_wire_vector() {
    assert_eq!(&encode_frame(&[10, 1, b'A'], 3).unwrap()[..], WIRE);
    let mut decoder = FrameDecoder::new(3);
    assert_eq!(
        decoder.decode(&mut Bytes::from_static(WIRE)).unwrap(),
        Some(Bytes::from_static(&[10, 1, b'A']))
    );
    decoder.finish().unwrap();
}

#[test]
fn every_split_and_bytewise_input() {
    for split in 0..WIRE.len() {
        let mut decoder = FrameDecoder::new(3);
        assert!(
            decoder
                .decode(&mut Bytes::copy_from_slice(&WIRE[..split]))
                .unwrap()
                .is_none()
        );
        assert_eq!(
            decoder
                .decode(&mut Bytes::copy_from_slice(&WIRE[split..]))
                .unwrap(),
            Some(Bytes::from_static(&[10, 1, b'A']))
        );
        decoder.finish().unwrap();
    }
    let mut decoder = FrameDecoder::new(3);
    let messages: Vec<_> = WIRE
        .iter()
        .filter_map(|byte| {
            decoder
                .decode(&mut Bytes::copy_from_slice(&[*byte]))
                .unwrap()
        })
        .collect();
    assert_eq!(messages, [Bytes::from_static(&[10, 1, b'A'])]);
    decoder.finish().unwrap();
}

#[test]
fn multiple_frames_leave_tail_with_caller() {
    let mut input = Bytes::from_static(&[0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 7]);
    let mut decoder = FrameDecoder::new(1);
    assert_eq!(decoder.decode(&mut input).unwrap(), Some(Bytes::new()));
    assert_eq!(input.len(), 6);
    assert_eq!(
        decoder.decode(&mut input).unwrap(),
        Some(Bytes::from_static(&[7]))
    );
    assert!(input.is_empty());
    decoder.finish().unwrap();
}

#[test]
fn oversize_is_rejected_without_payload_and_decoder_is_poisoned() {
    let mut decoder = FrameDecoder::new(8);
    assert!(matches!(
        decoder.decode(&mut Bytes::from_static(&[0, 255, 255, 255, 255])),
        Err(Error::LimitExceeded {
            size: 4_294_967_295,
            ..
        })
    ));
    assert!(matches!(
        decoder.decode(&mut Bytes::new()),
        Err(Error::DecoderClosed)
    ));
    assert!(matches!(
        encode_frame(&[1, 2], 1),
        Err(Error::LimitExceeded { .. })
    ));
}

#[test]
fn flags_are_not_silently_accepted() {
    for flag in [1, 2, 255] {
        let mut decoder = FrameDecoder::new(1);
        let error = decoder
            .decode(&mut Bytes::from(vec![flag, 0, 0, 0, 0]))
            .unwrap_err();
        assert!(matches!(
            (flag, error),
            (1, Error::UnsupportedCompression) | (2 | 255, Error::InvalidCompressionFlag(_))
        ));
    }
}

#[test]
fn eof_at_every_partial_position_is_an_error() {
    for end in 1..WIRE.len() {
        let mut decoder = FrameDecoder::new(3);
        assert!(
            decoder
                .decode(&mut Bytes::copy_from_slice(&WIRE[..end]))
                .unwrap()
                .is_none()
        );
        assert!(matches!(decoder.finish(), Err(Error::TruncatedFrame)));
        assert!(matches!(decoder.finish(), Err(Error::DecoderClosed)));
    }
}

#[test]
fn empty_stream_and_zero_limit() {
    let mut decoder = FrameDecoder::new(0);
    decoder.finish().unwrap();
    assert!(matches!(
        decoder.decode(&mut Bytes::from_static(WIRE)),
        Err(Error::DecoderClosed)
    ));
    assert_eq!(&encode_frame(&[], 0).unwrap()[..], &[0, 0, 0, 0, 0]);
}

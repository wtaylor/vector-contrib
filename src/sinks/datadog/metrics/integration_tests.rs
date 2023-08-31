use std::collections::HashMap;

use bytes::Bytes;
use flate2::read::ZlibDecoder;
use futures::{channel::mpsc::Receiver, stream, StreamExt};
use hyper::StatusCode;
use indoc::indoc;
use prost::Message;
use rand::{thread_rng, Rng};

use tokio::sync::mpsc;
use tokio::time::{sleep, Duration};

use super::DatadogMetricsConfig;
use base64::prelude::{Engine as _, BASE64_STANDARD};
use reqwest::{Client, Method};
use serde::Deserialize;
use vector_core::event::{BatchNotifier, BatchStatus, Event, Metric, MetricKind, MetricValue};

use crate::common::datadog::{DatadogMetricType, DatadogPoint, DatadogSeriesMetric};
use crate::{
    config::{ConfigBuilder, SinkConfig},
    sinks::util::test::{build_test_server_status, load_sink},
    sources::datadog_agent::{
        ddmetric_proto::{
            metric_payload::{MetricSeries, MetricType},
            MetricPayload,
        },
        DatadogAgentConfig,
    },
    test_util::{
        components::{assert_sink_compliance, SINK_TAGS},
        map_event_batch_stream, next_addr, start_topology, trace_init,
    },
    topology::RunningTopology,
};

enum ApiStatus {
    OK,
    // Forbidden,
}

fn test_server(
    addr: std::net::SocketAddr,
    api_status: ApiStatus,
) -> (
    Receiver<(http::request::Parts, Bytes)>,
    stream_cancel::Trigger,
    impl std::future::Future<Output = Result<(), ()>>,
) {
    let status = match api_status {
        ApiStatus::OK => StatusCode::OK,
        // ApiStatus::Forbidden => StatusCode::FORBIDDEN,
    };

    // NOTE: we pass `Trigger` out to the caller even though this suite never
    // uses it as it's being dropped cancels the stream machinery here,
    // indicating failures that might not be valid.
    build_test_server_status(addr, status)
}

/// Starts a test sink with random metrics running into it
///
/// This function starts a Datadog Metrics sink with a simplistic configuration and
/// runs random lines through it, returning a vector of the random lines and a
/// Receiver populated with the result of the sink's operation.
///
/// Testers may set `http_status` and `batch_status`. The first controls what
/// status code faked HTTP responses will have, the second acts as a check on
/// the `Receiver`'s status before being returned to the caller.
async fn start_test(
    api_status: ApiStatus,
    batch_status: BatchStatus,
) -> (Vec<Event>, Receiver<(http::request::Parts, Bytes)>) {
    let config = indoc! {r#"
        default_api_key = "atoken"
        default_namespace = "foo"
    "#};
    let (mut config, cx) = load_sink::<DatadogMetricsConfig>(config).unwrap();

    let addr = next_addr();
    // Swap out the endpoint so we can force send it
    // to our local server
    let endpoint = format!("http://{}", addr);
    config.dd_common.endpoint = Some(endpoint.clone());

    let (sink, _) = config.build(cx).await.unwrap();

    let (rx, _trigger, server) = test_server(addr, api_status);
    tokio::spawn(server);

    let (batch, receiver) = BatchNotifier::new_with_receiver();
    let events: Vec<_> = (0..10)
        .map(|index| {
            Event::Metric(Metric::new(
                format!("counter_{}", thread_rng().gen::<u32>()),
                MetricKind::Absolute,
                MetricValue::Counter {
                    value: index as f64,
                },
            ))
        })
        .collect();
    let stream = map_event_batch_stream(stream::iter(events.clone()), Some(batch));

    sink.run(stream).await.unwrap();
    assert_eq!(receiver.await, batch_status);

    (events, rx)
}

fn decompress_payload(payload: Vec<u8>) -> std::io::Result<Vec<u8>> {
    let mut decompressor = ZlibDecoder::new(&payload[..]);
    let mut decompressed = Vec::new();
    let result = std::io::copy(&mut decompressor, &mut decompressed);
    result.map(|_| decompressed)
}

#[tokio::test]
/// Assert the basic functionality of the sink in good conditions
///
/// This test rigs the sink to return OK to responses, checks that all batches
/// were delivered and then asserts that every message is able to be
/// deserialized.
async fn smoke() {
    let (expected, rx) = start_test(ApiStatus::OK, BatchStatus::Delivered).await;

    let output = rx.take(expected.len()).collect::<Vec<_>>().await;

    for val in output.iter() {
        assert_eq!(
            val.0.headers.get("Content-Type").unwrap(),
            "application/json"
        );
        assert_eq!(val.0.headers.get("DD-API-KEY").unwrap(), "atoken");
        assert!(val.0.headers.contains_key("DD-Agent-Payload"));

        let compressed_payload = val.1.to_vec();
        let payload = decompress_payload(compressed_payload).unwrap();
        let payload = std::str::from_utf8(&payload).unwrap();
        let payload: serde_json::Value = serde_json::from_str(payload).unwrap();

        let series = payload
            .as_object()
            .unwrap()
            .get("series")
            .unwrap()
            .as_array()
            .unwrap();
        assert!(!series.is_empty());

        // check metrics are sorted by name, which helps HTTP compression
        let metric_names: Vec<String> = series
            .iter()
            .map(|value| {
                value
                    .as_object()
                    .unwrap()
                    .get("metric")
                    .unwrap()
                    .as_str()
                    .unwrap()
                    .to_string()
            })
            .collect();
        let mut sorted_names = metric_names.clone();
        sorted_names.sort();
        assert_eq!(metric_names, sorted_names);

        let entry = series.first().unwrap().as_object().unwrap();
        assert_eq!(
            entry.get("metric").unwrap().as_str().unwrap(),
            "foo.counter"
        );
        assert_eq!(entry.get("type").unwrap().as_str().unwrap(), "count");
        let points = entry
            .get("points")
            .unwrap()
            .as_array()
            .unwrap()
            .first()
            .unwrap()
            .as_array()
            .unwrap();
        assert_eq!(points.len(), 2);
        assert_eq!(points.get(1).unwrap().as_f64().unwrap(), 1.0);
    }
}

#[tokio::test]
async fn real_endpoint() {
    assert_sink_compliance(&SINK_TAGS, async {
        let config = indoc! {r#"
        default_api_key = "${TEST_DATADOG_API_KEY}"
        default_namespace = "fake.test.integration"
    "#};
        let api_key = std::env::var("TEST_DATADOG_API_KEY").unwrap();
        assert!(!api_key.is_empty(), "$TEST_DATADOG_API_KEY required");
        let config = config.replace("${TEST_DATADOG_API_KEY}", &api_key);
        let (config, cx) = load_sink::<DatadogMetricsConfig>(config.as_str()).unwrap();

        let (sink, _) = config.build(cx).await.unwrap();
        let (batch, receiver) = BatchNotifier::new_with_receiver();
        let events: Vec<_> = (0..10)
            .map(|index| {
                Event::Metric(Metric::new(
                    "counter",
                    MetricKind::Absolute,
                    MetricValue::Counter {
                        value: index as f64,
                    },
                ))
            })
            .collect();
        let stream = map_event_batch_stream(stream::iter(events.clone()), Some(batch));

        sink.run(stream).await.unwrap();
        assert_eq!(receiver.await, BatchStatus::Delivered);
    })
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//    e2e Agent -> Vector POC
//    Temporary location in the code. Ideally I think this would be located in somewhere like lib/
//    , since the same framework would be utilized to test the datadog_agent source and all of the
//    datadog sinks.
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn vector_receive_port() -> u16 {
    std::env::var("VECTOR_RECEIVE_PORT")
        .unwrap_or_else(|_| "8081".to_string())
        .parse::<u16>()
        .unwrap()
}

fn fake_intake_vector_endpoint() -> String {
    std::env::var("FAKE_INTAKE_VECTOR_ENDPOINT")
        .unwrap_or_else(|_| "http://127.0.0.1:8082".to_string())
}

fn fake_intake_agent_endpoint() -> String {
    std::env::var("FAKE_INTAKE_AGENT_ENDPOINT")
        .unwrap_or_else(|_| "http://127.0.0.1:8083".to_string())
}

async fn start_vector() -> (
    RunningTopology,
    (mpsc::UnboundedSender<()>, mpsc::UnboundedReceiver<()>),
) {
    let dd_agent_address = format!("0.0.0.0:{}", vector_receive_port());

    let source_config = toml::from_str::<DatadogAgentConfig>(&format!(
        indoc! { r#"
            address = "{}"
            multiple_outputs = true
        "#},
        dd_agent_address,
    ))
    .unwrap();

    let mut builder = ConfigBuilder::default();
    builder.add_source("in", source_config);

    let dd_metrics_endpoint = fake_intake_vector_endpoint();
    let cfg = format!(
        indoc! { r#"
            default_api_key = "unused"
            endpoint = "{}"
        "#},
        dd_metrics_endpoint
    );

    let sink_config = toml::from_str::<DatadogMetricsConfig>(&cfg).unwrap();

    builder.add_sink("out", &["in.metrics"], sink_config);

    let config = builder.build().expect("building config should not fail");

    let (topology, shutdown) = start_topology(config, false).await;
    println!("Started vector.");

    (topology, shutdown)
}

// Fakeintake response
#[derive(Deserialize, Debug)]
struct Payloads {
    payloads: Vec<Payload>,
}

#[allow(dead_code)]
#[derive(Deserialize, Debug)]
struct Payload {
    // base64 encoded
    data: String,
    encoding: String,
    timestamp: String,
}

async fn get_fakeintake_payloads(url: &str) -> Payloads {
    let res = Client::new()
        .request(Method::GET, url)
        .send()
        .await
        .unwrap_or_else(|_| panic!("Sending GET request to {} failed", url))
        .text()
        .await
        .expect("Parsing fakeintake payloads failed");

    println!("body= {:?}", res);

    Payloads { payloads: vec![] }
    // .json::<Payloads>()
    // .await
    // .expect("Parsing fakeintake payloads failed")
}

async fn get_payloads_agent() -> Payloads {
    let url = format!(
        "{}/fakeintake/payloads?endpoint=/api/v2/series?format=json",
        fake_intake_agent_endpoint()
    );
    get_fakeintake_payloads(&url).await
}

async fn get_payloads_vector() -> Payloads {
    let url = format!(
        "{}/fakeintake/payloads?endpoint=/api/v1/series?format=json",
        fake_intake_vector_endpoint()
    );
    get_fakeintake_payloads(&url).await
}

impl From<&MetricSeries> for DatadogSeriesMetric {
    fn from(other_series: &MetricSeries) -> DatadogSeriesMetric {
        let interval = other_series
            .interval
            .is_positive()
            .then_some(other_series.interval as u32)
            .or(None);

        let tags = if other_series.tags.is_empty() {
            None
        } else {
            Some(other_series.tags.clone())
        };

        let host_resource = other_series
            .resources
            .iter()
            .find(|resource| resource.r#type == "host");
        let host = host_resource.map(|resource| resource.name.clone());

        let source_type_name = if other_series.source_type_name.is_empty() {
            None
        } else {
            Some(other_series.source_type_name.clone())
        };

        let mut points = vec![];

        for point in &other_series.points {
            points.push(DatadogPoint(point.timestamp, point.value));
        }

        DatadogSeriesMetric {
            metric: other_series.metric.clone(),
            r#type: other_series.r#type().into(),
            interval,
            points,
            tags,
            host,
            source_type_name,
            device: None,
        }
    }
}

impl From<MetricType> for DatadogMetricType {
    fn from(other: MetricType) -> DatadogMetricType {
        match other {
            MetricType::Unspecified => panic!("unspecified metric type detected"),
            MetricType::Count => DatadogMetricType::Count,
            MetricType::Rate => DatadogMetricType::Rate,
            MetricType::Gauge => DatadogMetricType::Gauge,
        }
    }
}

fn unpack_payloads_agent_v2(in_payloads: &Vec<Payload>) -> Vec<MetricPayload> {
    let mut out_payloads = vec![];

    in_payloads.iter().for_each(|payload| {
        // decode base64
        let payload = BASE64_STANDARD
            .decode(&payload.data)
            .expect("Invalid base64 data");

        // decompress
        let bytes = Bytes::from(decompress_payload(payload).unwrap());

        let payload = MetricPayload::decode(bytes).unwrap();

        out_payloads.push(payload);
    });

    out_payloads
}

fn convert_v2_metric_payloads_v1(in_payload: &Vec<MetricPayload>) -> Vec<DatadogSeriesMetric> {
    let mut out_series = vec![];

    in_payload.iter().for_each(|payload| {
        payload.series.iter().for_each(|serie| {
            out_series.push(serie.into());
        });
    });

    out_series
}

fn unpack_vector_series(in_payloads: &Vec<Payload>) -> Vec<DatadogSeriesMetric> {
    let mut out_series = vec![];

    in_payloads.iter().for_each(|payload| {
        // decode base64
        let payload = BASE64_STANDARD
            .decode(&payload.data)
            .expect("Invalid base64 data");

        let payload = decompress_payload(payload).unwrap();
        let payload = std::str::from_utf8(&payload).unwrap();
        let payload: serde_json::Value = serde_json::from_str(payload).unwrap();

        //println!("decoded, decompressed payload: {:?}", &payload);
        //println!();

        let series = payload
            .as_object()
            .unwrap()
            .get("series")
            .unwrap()
            .as_array()
            .unwrap();

        series.iter().for_each(|serie| {
            let ser: DatadogSeriesMetric = serde_json::from_value(serie.clone()).unwrap();
            out_series.push(ser);
        });
    });

    out_series
}

// For debugging
fn print_series(series: &Vec<DatadogSeriesMetric>) {
    for serie in series {
        if serie.metric.contains("foo_metric") {
            println!("    {:?}", serie);
        }
    }
}

// NOTE: there are probably more eloquent ways to handle this but it works for a POC
// Sums up the metrics in each series by name.
fn aggregate_series_metrics(series: &Vec<DatadogSeriesMetric>) -> HashMap<String, (i64, f64)> {
    let mut aggregate = HashMap::new();
    for serie in series {
        let interval = serie.interval.unwrap_or(1) as f64;
        match serie.r#type {
            DatadogMetricType::Gauge => {
                // last one wins
                if let Some(point) = serie.points.last() {
                    if let Some((t, v)) = aggregate.get_mut(&serie.metric) {
                        if point.0 > *t {
                            *t = point.0;
                            *v = point.1;
                        }
                    } else {
                        aggregate.insert(serie.metric.clone(), (point.0, point.1));
                    }
                }
            }
            DatadogMetricType::Count => {
                if let Some((t, v)) = aggregate.get_mut(&serie.metric) {
                    for point in &serie.points {
                        *v += point.1;
                        *t = point.0;
                    }
                } else {
                    for point in &serie.points {
                        aggregate.insert(serie.metric.clone(), (point.0, point.1));
                    }
                }
            }
            DatadogMetricType::Rate => {
                if let Some((t, v)) = aggregate.get_mut(&serie.metric) {
                    for point in &serie.points {
                        *v += point.1 * interval;
                        *t = point.0;
                    }
                } else {
                    for (idx, point) in serie.points.iter().enumerate() {
                        if idx == 0 {
                            aggregate.insert(serie.metric.clone(), (point.0, point.1 * interval));
                        } else {
                            if let Some((t, v)) = aggregate.get_mut(&serie.metric) {
                                *v += point.1 * interval;
                                *t = point.0;
                            }
                        }
                    }
                }
            }
        }
    }
    aggregate
}

#[tokio::test]
async fn poc_e2e_metrics() {
    trace_init();

    // starts the vector source and sink
    // panics if vector errors during startup
    let (_topology, _shutdown) = start_vector().await;

    // TODO there hopefully is a way to configure the flushing of metrics such that we don't have
    // to wait statically for so long here.
    sleep(Duration::from_secs(25)).await;

    let agent_payloads = get_payloads_agent().await;
    let unpacked_agent_payloads = unpack_payloads_agent_v2(&agent_payloads.payloads);

    // this is kind of ugly but, because the Agent is sending out v2 (protobuf) payloads, and
    // vector sends out v1 (JSON) payloads, we have to convert the Agent protobuf payloads to v1,
    // in order to have a proper validation.
    let v1_agent_series = convert_v2_metric_payloads_v1(&unpacked_agent_payloads);

    println!("AGENT METRICS");
    println!();
    print_series(&v1_agent_series);
    println!();

    let agent_aggregate = aggregate_series_metrics(&v1_agent_series);
    let foo_rate_agent = agent_aggregate.get("foo_metric.rate");
    println!("AGENT RATE AGGREGATE: {:?}", foo_rate_agent);

    let foo_gauge_agent = agent_aggregate.get("foo_metric.gauge");
    println!("AGENT GAUGE AGGREGATE: {:?}", foo_gauge_agent);

    assert!(foo_rate_agent.is_some());
    assert!(foo_gauge_agent.is_some());

    let vector_payloads = get_payloads_vector().await;
    let v1_vector_series = unpack_vector_series(&vector_payloads.payloads);

    println!("VECTOR METRICS");
    println!();
    print_series(&v1_vector_series);
    println!();

    let vector_aggregate = aggregate_series_metrics(&v1_vector_series);
    let foo_rate_vector = vector_aggregate.get("foo_metric.rate");
    println!("VECTOR RATE AGGREGATE: {:?}", foo_rate_vector);

    assert!(foo_rate_vector.is_some());

    let foo_gauge_vector = vector_aggregate.get("foo_metric.gauge");
    println!("AGENT GAUGE AGGREGATE: {:?}", foo_gauge_vector);

    assert_eq!(foo_rate_agent.unwrap().1, foo_rate_vector.unwrap().1);

    assert_eq!(foo_gauge_agent.unwrap().1, foo_gauge_vector.unwrap().1);
}

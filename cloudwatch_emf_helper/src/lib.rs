use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use thiserror::Error;
use tracing::{error, info};

const MAX_DIMENSIONS: usize = 30;
const MAX_METRICS: usize = 100;
const STORAGE_RESOLUTION: u64 = 60;

/// Error type for metrics operations.
#[derive(Error, Debug)]
pub enum MetricsError {
    /// Error indicating too many dimensions.
    #[error("too many dimensions")]
    TooManyDimensions,
    /// Error indicating too many metrics.
    #[error("too many metrics")]
    TooManyMetrics,
    /// Error indicating a serialization failure.
    #[error("serialization error: {0}")]
    SerializationError(String),
}

/// Dimensions for CloudWatch metrics.
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "PascalCase")]
struct Dimensions(HashMap<String, String>);

/// Metric values for CloudWatch metrics.
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct MetricValues(HashMap<String, f64>);

/// Name of a dimension.
#[derive(Debug, Serialize, Deserialize)]
struct DimensionName(String);

/// Namespace for CloudWatch metrics.
#[derive(Debug, Serialize, Deserialize)]
struct Namespace(String);

/// Enum representing units for metrics in CloudWatch.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub enum MetricUnit {
    Seconds,
    Milliseconds,
    Bytes,
    Kilobytes,
    Count,
    Percent,
    CountPerSecond,
}

/// Struct representing a metric for CloudWatch.
#[derive(Debug, Serialize, Deserialize)]
pub struct Metric {
    /// Name of the metric.
    pub name: String,
    /// Unit of the metric.
    pub unit: MetricUnit,
    /// Value of the metric.
    pub value: f64,
}

impl Metric {
    fn to_metric_definition(&self) -> MetricDefinition {
        MetricDefinition {
            name: self.name.clone(),
            unit: self.unit.clone(),
            storage_resolution: STORAGE_RESOLUTION,
        }
    }
}

/// Struct representing a metric definition as specified in CloudWatch Embedded Metric Format.
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct MetricDefinition {
    name: String,
    unit: MetricUnit,
    storage_resolution: u64,
}

/// Struct representing a metric directive as specified in CloudWatch Embedded Metric Format.
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct MetricDirective {
    namespace: String,
    dimensions: Vec<Vec<DimensionName>>,
    metrics: Vec<MetricDefinition>,
}

/// Struct representing metadata as specified in CloudWatch Embedded Metric Format.
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct MetadataObject {
    timestamp: i64,
    cloud_watch_metrics: Vec<MetricDirective>,
}

/// Struct representing the CloudWatch metrics log.
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct CloudWatchMetricsLog {
    #[serde(rename = "_aws")]
    aws: MetadataObject,
    #[serde(flatten)]
    dimensions: Dimensions,
    #[serde(flatten)]
    metrics_values: MetricValues,
    #[serde(flatten)]
    properties: HashMap<String, PropertyValue>,
}

impl TryInto<String> for CloudWatchMetricsLog {
    type Error = String;

    fn try_into(self) -> Result<String, Self::Error> {
        serde_json::to_string(&self).map_err(|err| err.to_string())
    }
}

/// An enumeration representing various property values.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum PropertyValue {
    String(String),
    Bool(bool),
    Number(f64),
}

impl From<&str> for PropertyValue {
    fn from(value: &str) -> Self {
        PropertyValue::String(value.to_string())
    }
}

impl From<String> for PropertyValue {
    fn from(value: String) -> Self {
        PropertyValue::String(value)
    }
}

impl From<&String> for PropertyValue {
    fn from(value: &String) -> Self {
        PropertyValue::String(value.to_string())
    }
}

impl From<bool> for PropertyValue {
    fn from(value: bool) -> Self {
        PropertyValue::Bool(value)
    }
}

impl From<f64> for PropertyValue {
    fn from(value: f64) -> Self {
        PropertyValue::Number(value)
    }
}

/// Struct representing a collection of metrics to be logged to CloudWatch.
pub struct Metrics {
    namespace: Namespace,
    dimensions: Dimensions,
    entries: Vec<Metric>,
    properties: HashMap<String, PropertyValue>,
}

impl Metrics {
    /// Creates a new Metrics object with the given namespace.
    pub fn new(namespace: &str) -> Self {
        Self {
            dimensions: Dimensions(HashMap::new()),
            namespace: Namespace(namespace.to_string()),
            entries: Vec::new(),
            properties: HashMap::new(),
        }
    }

    /// Adds a metric to the Metrics object.
    pub fn add_metric(
        &mut self,
        name: &str,
        unit: MetricUnit,
        value: f64,
    ) -> Result<(), MetricsError> {
        if self.entries.len() >= MAX_METRICS {
            return Err(MetricsError::TooManyMetrics);
        }
        if self.entries.iter().any(|metric| metric.name == name) {
            return Err(MetricsError::TooManyMetrics);
        }
        self.entries.push(Metric {
            name: name.to_string(),
            unit,
            value,
        });
        Ok(())
    }

    /// Tries to add a dimension to the Metrics object.
    /// Returns an error if the maximum number of dimensions is reached.
    pub fn try_add_dimension(&mut self, key: &str, value: &str) -> Result<(), MetricsError> {
        if self.dimensions.0.len() >= MAX_DIMENSIONS {
            Err(MetricsError::TooManyDimensions)
        } else {
            self.dimensions.0.insert(key.to_string(), value.to_string());
            Ok(())
        }
    }

    /// Emits the metrics to stdout in a single payload.
    pub fn emit(&mut self) -> Result<(), MetricsError> {
        let serialized_metrics: Result<String, _> = self.format().try_into();
        match serialized_metrics {
            Ok(payload) => {
                info!("{payload}");
                self.entries.clear();
                Ok(())
            }
            Err(err) => {
                error!("Error Serializing metrics: {err}");
                Err(MetricsError::SerializationError(err))
            }
        }
    }

    /// Adds a property to the Metrics object.
    pub fn add_property<V: Into<PropertyValue>>(&mut self, key: &str, value: V) {
        self.properties.insert(key.to_string(), value.into());
    }

    fn format(&self) -> CloudWatchMetricsLog {
        let metrics_definitions = self
            .entries
            .iter()
            .map(Metric::to_metric_definition)
            .collect::<Vec<MetricDefinition>>();
        let metrics_entries = vec![MetricDirective {
            namespace: self.namespace.0.clone(),
            dimensions: vec![self
                .dimensions
                .0
                .keys()
                .map(|key| DimensionName(key.to_string()))
                .collect()],
            metrics: metrics_definitions,
        }];
        let cloudwatch_metrics = MetadataObject {
            timestamp: Utc::now().timestamp_millis(),
            cloud_watch_metrics: metrics_entries,
        };
        let metrics_values = self
            .entries
            .iter()
            .map(|metric| (metric.name.clone(), metric.value))
            .collect::<HashMap<_, _>>();
        CloudWatchMetricsLog {
            aws: cloudwatch_metrics,
            dimensions: self.dimensions.clone(),
            metrics_values: MetricValues(metrics_values),
            properties: self.properties.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_metrics() {
        let mut metrics = Metrics::new("test_namespace");
        metrics
            .try_add_dimension("service", "dummy_service")
            .unwrap();
        metrics
            .add_metric("test_metric_count", MetricUnit::Count, 1.0)
            .unwrap();
        metrics
            .add_metric("test_metric_seconds", MetricUnit::Seconds, 22.0)
            .unwrap();

        let log = metrics.format();

        assert_eq!(log.aws.cloud_watch_metrics[0].namespace, "test_namespace");
        assert_eq!(
            log.aws.cloud_watch_metrics[0].metrics[0].name,
            "test_metric_count"
        );
        assert_eq!(
            log.aws.cloud_watch_metrics[0].metrics[0].unit,
            MetricUnit::Count
        );
        assert_eq!(
            log.aws.cloud_watch_metrics[0].metrics[0].storage_resolution,
            STORAGE_RESOLUTION
        );
        assert_eq!(log.metrics_values.0.get("test_metric_count"), Some(&1.0));
        assert_eq!(
            log.aws.cloud_watch_metrics[0].metrics[1].name,
            "test_metric_seconds"
        );
        assert_eq!(
            log.aws.cloud_watch_metrics[0].metrics[1].unit,
            MetricUnit::Seconds
        );
        assert_eq!(
            log.aws.cloud_watch_metrics[0].metrics[1].storage_resolution,
            STORAGE_RESOLUTION
        );
        assert_eq!(log.dimensions.0.len(), 1);
    }

    #[test]
    fn test_handle_duplicate_metric() {
        let mut metrics = Metrics::new("test");
        metrics
            .try_add_dimension("service", "dummy_service")
            .unwrap();
        metrics.add_metric("test", MetricUnit::Count, 2.0).unwrap();
        metrics
            .add_metric("test", MetricUnit::Count, 1.0)
            .unwrap_err();

        assert_eq!(metrics.entries.len(), 1);
    }

    #[test]
    fn test_handle_max_metrics() {
        let mut metrics = Metrics::new("test");
        metrics
            .try_add_dimension("service", "dummy_service")
            .unwrap();
        for i in 0..100 {
            metrics
                .add_metric(&format!("metric{i}"), MetricUnit::Count, i as f64)
                .unwrap();
        }

        assert_eq!(metrics.entries.len(), 100);
        metrics
            .add_metric("over_100", MetricUnit::Count, 11.0)
            .unwrap_err();
        assert_eq!(metrics.entries.len(), 100);
    }

    #[test]
    fn test_fail_if_exceeding_max_dimensions() {
        let mut metrics = Metrics::new("test");
        for i in 0..30 {
            metrics
                .try_add_dimension(&format!("key{i}"), &format!("value{i}"))
                .unwrap();
        }

        assert!(metrics.try_add_dimension("key31", "value31").is_err());
    }

    #[test]
    fn test_add_property() {
        let mut metrics = Metrics::new("test_namespace");
        metrics.add_property("property1", "value1");
        metrics.add_property("property2", 42.0);
        metrics.add_property("property3", true);

        assert_eq!(
            metrics.properties.get("property1"),
            Some(&PropertyValue::String("value1".to_string()))
        );
        assert_eq!(
            metrics.properties.get("property2"),
            Some(&PropertyValue::Number(42.0))
        );
        assert_eq!(
            metrics.properties.get("property3"),
            Some(&PropertyValue::Bool(true))
        );
    }

    #[test]
    fn test_emit_metrics() {
        let mut metrics = Metrics::new("test_namespace");
        metrics
            .try_add_dimension("service", "dummy_service")
            .unwrap();
        metrics
            .add_metric("test_metric_count", MetricUnit::Count, 1.0)
            .unwrap();
        metrics
            .add_metric("test_metric_seconds", MetricUnit::Seconds, 22.0)
            .unwrap();
        metrics.add_property("property1", "value1");

        let result = metrics.emit();
        assert!(result.is_ok());
    }

    #[test]
    fn test_emit_empty_metrics() {
        let mut metrics = Metrics::new("test_namespace");
        let result = metrics.emit();
        assert!(result.is_ok());
    }

    #[test]
    fn test_format_metrics() {
        let mut metrics = Metrics::new("test_namespace");
        metrics
            .try_add_dimension("service", "dummy_service")
            .unwrap();
        metrics
            .add_metric("test_metric_count", MetricUnit::Count, 1.0)
            .unwrap();
        metrics.add_property("property1", "value1");

        let formatted = metrics.format();
        assert_eq!(
            formatted.aws.cloud_watch_metrics[0].namespace,
            "test_namespace"
        );
        assert_eq!(
            formatted.aws.cloud_watch_metrics[0].metrics[0].name,
            "test_metric_count"
        );
        assert_eq!(
            formatted.aws.cloud_watch_metrics[0].metrics[0].unit,
            MetricUnit::Count
        );
        assert_eq!(
            formatted.aws.cloud_watch_metrics[0].metrics[0].storage_resolution,
            STORAGE_RESOLUTION
        );
        assert_eq!(
            formatted.metrics_values.0.get("test_metric_count"),
            Some(&1.0)
        );
        assert_eq!(
            formatted.dimensions.0.get("service"),
            Some(&"dummy_service".to_string())
        );
        assert_eq!(
            formatted.properties.get("property1"),
            Some(&PropertyValue::String("value1".to_string()))
        );
    }

    #[test]
    fn test_metric_unit_conversion() {
        let mut metrics = Metrics::new("test_namespace");
        metrics
            .add_metric("test_metric_bytes", MetricUnit::Bytes, 1024.0)
            .unwrap();
        metrics
            .add_metric("test_metric_percent", MetricUnit::Percent, 75.0)
            .unwrap();

        let log = metrics.format();
        assert_eq!(log.aws.cloud_watch_metrics[0].namespace, "test_namespace");
        assert_eq!(
            log.aws.cloud_watch_metrics[0].metrics[0].name,
            "test_metric_bytes"
        );
        assert_eq!(
            log.aws.cloud_watch_metrics[0].metrics[0].unit,
            MetricUnit::Bytes
        );
        assert_eq!(
            log.aws.cloud_watch_metrics[0].metrics[1].name,
            "test_metric_percent"
        );
        assert_eq!(
            log.aws.cloud_watch_metrics[0].metrics[1].unit,
            MetricUnit::Percent
        );
    }
}

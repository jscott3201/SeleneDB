//! HTTP embedding provider: delegates to a remote embedding endpoint.
//!
//! Used when the `embed` feature is disabled or when the
//! `[vector] endpoint` config is set (endpoint takes priority over
//! local model). The endpoint contract:
//!
//! ```text
//! POST {endpoint}
//! Content-Type: application/json
//!
//! { "text": "...", "task": "retrieval", "dimensions": 768 }
//! → { "embedding": [0.1, ...], "model": "...", "dimensions": 768 }
//! ```

use std::io::Read;

use crate::types::error::GqlError;

use super::provider::{EmbeddingProvider, EmbeddingTask};

/// Request body sent to the embedding endpoint.
#[derive(serde::Serialize)]
struct EmbedRequest<'a> {
    text: &'a str,
    task: &'a str,
    dimensions: usize,
}

/// Response body from the embedding endpoint.
#[derive(serde::Deserialize)]
struct EmbedResponse {
    embedding: Vec<f32>,
    #[allow(dead_code)]
    model: String,
    dimensions: usize,
}

/// Maximum input size for HTTP embedding requests (1 MB).
const MAX_INPUT_BYTES: usize = 1_048_576;

/// Maximum response body size accepted from the embedding endpoint (16 MB).
///
/// Guards against a misconfigured, compromised, or proxy-redirected endpoint
/// streaming unbounded bytes into `serde_json::from_reader` and OOMing the
/// server before the dimension sanity checks can run. A 4096-dim f32 vector
/// is only ~16 KB in JSON, so 16 MB is comfortably oversized.
const MAX_RESPONSE_BYTES: u64 = 16 * 1024 * 1024;

/// Embedding provider that delegates to a remote HTTP endpoint.
pub struct HttpEmbeddingProvider {
    endpoint: String,
    dimensions: usize,
    model_name: &'static str,
    agent: ureq::Agent,
}

impl HttpEmbeddingProvider {
    /// Create a new HTTP embedding provider.
    ///
    /// Validates the endpoint URL (must be `http://` or `https://`)
    /// and builds an HTTP agent with sensible timeouts (connect: 5s, read: 30s).
    pub fn new(endpoint: String, dimensions: usize, model_name: String) -> Result<Self, GqlError> {
        if endpoint.is_empty() {
            return Err(GqlError::InvalidArgument {
                message: "embedding endpoint URL cannot be empty".into(),
            });
        }
        if !endpoint.starts_with("http://") && !endpoint.starts_with("https://") {
            return Err(GqlError::InvalidArgument {
                message: format!(
                    "embedding endpoint must use http:// or https:// scheme, got '{endpoint}'"
                ),
            });
        }

        let agent: ureq::Agent = ureq::Agent::config_builder()
            .timeout_connect(Some(std::time::Duration::from_secs(5)))
            .timeout_global(Some(std::time::Duration::from_secs(30)))
            .build()
            .new_agent();

        // Leak the model name — the provider lives in an OnceLock for the
        // entire process lifetime, so this is a one-time allocation.
        let model_name: &'static str = Box::leak(model_name.into_boxed_str());

        Ok(Self {
            endpoint,
            dimensions,
            model_name,
            agent,
        })
    }

    fn do_embed(&self, text: &str, task: EmbeddingTask) -> Result<Vec<f32>, GqlError> {
        if text.len() > MAX_INPUT_BYTES {
            return Err(GqlError::InvalidArgument {
                message: format!(
                    "input text too large: {} bytes exceeds limit of {MAX_INPUT_BYTES}",
                    text.len()
                ),
            });
        }

        let request = EmbedRequest {
            text,
            task: task.as_str(),
            dimensions: self.dimensions,
        };

        // Perform the blocking HTTP call. If we are running on a multi-thread
        // tokio runtime worker, use `block_in_place` to signal the scheduler to
        // move other tasks off this worker so they do not starve while `ureq`
        // parks the thread. Outside a runtime (e.g. direct unit tests) or on
        // current-thread runtimes, call directly.
        let call = || -> Result<EmbedResponse, GqlError> {
            let mut resp = self
                .agent
                .post(&self.endpoint)
                .send_json(&request)
                .map_err(|e| GqlError::internal(format!("embedding endpoint error: {e}")))?;
            // Cap response body: `Body::as_reader` has no default size limit,
            // so a runaway endpoint could OOM the server before the dimension
            // check runs. `take(MAX_RESPONSE_BYTES)` returns at most that many
            // bytes and then EOFs, so `serde_json::from_reader` surfaces a
            // parse error (unexpected EOF) rather than a memory exhaustion.
            let reader = resp.body_mut().as_reader().take(MAX_RESPONSE_BYTES);
            serde_json::from_reader(reader)
                .map_err(|e| GqlError::internal(format!("embedding response parse error: {e}")))
        };

        let response: EmbedResponse = match tokio::runtime::Handle::try_current() {
            Ok(h)
                if matches!(
                    h.runtime_flavor(),
                    tokio::runtime::RuntimeFlavor::MultiThread
                ) =>
            {
                tokio::task::block_in_place(call)?
            }
            _ => call()?,
        };

        if response.dimensions != self.dimensions {
            return Err(GqlError::internal(format!(
                "embedding dimension mismatch: expected {}, got {}",
                self.dimensions, response.dimensions
            )));
        }

        if response.embedding.len() != self.dimensions {
            return Err(GqlError::internal(format!(
                "embedding vector length mismatch: expected {}, got {}",
                self.dimensions,
                response.embedding.len()
            )));
        }

        Ok(response.embedding)
    }
}

impl EmbeddingProvider for HttpEmbeddingProvider {
    fn embed(&self, text: &str, _namespace: Option<&str>) -> Result<Vec<f32>, GqlError> {
        self.do_embed(text, EmbeddingTask::Retrieval)
    }

    fn embed_with_task(
        &self,
        text: &str,
        task: EmbeddingTask,
        _namespace: Option<&str>,
    ) -> Result<Vec<f32>, GqlError> {
        self.do_embed(text, task)
    }

    fn dimensions(&self, _namespace: Option<&str>) -> usize {
        self.dimensions
    }

    fn model_id(&self) -> &'static str {
        self.model_name
    }

    fn max_input_bytes(&self) -> usize {
        MAX_INPUT_BYTES
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn embed_request_serializes() {
        let req = EmbedRequest {
            text: "hello world",
            task: "retrieval",
            dimensions: 768,
        };
        let json = serde_json::to_string(&req).unwrap();
        assert!(json.contains("\"text\":\"hello world\""));
        assert!(json.contains("\"task\":\"retrieval\""));
        assert!(json.contains("\"dimensions\":768"));
    }

    #[test]
    fn embed_response_deserializes() {
        let json = r#"{"embedding":[0.1,0.2,0.3],"model":"test","dimensions":3}"#;
        let resp: EmbedResponse = serde_json::from_str(json).unwrap();
        assert_eq!(resp.embedding.len(), 3);
        assert_eq!(resp.dimensions, 3);
        assert_eq!(resp.model, "test");
    }

    #[test]
    fn empty_endpoint_rejected() {
        let result = HttpEmbeddingProvider::new(String::new(), 768, "test".into());
        assert!(result.is_err());
    }

    #[test]
    fn invalid_scheme_rejected() {
        let result = HttpEmbeddingProvider::new("ftp://host/embed".into(), 768, "test".into());
        assert!(result.is_err());
    }

    #[test]
    fn provider_dimensions() {
        let provider =
            HttpEmbeddingProvider::new("http://localhost:8080/embed".into(), 256, "test".into())
                .unwrap();
        assert_eq!(provider.dimensions(None), 256);
    }

    #[test]
    fn provider_model_id() {
        let provider = HttpEmbeddingProvider::new(
            "http://localhost:8080/embed".into(),
            768,
            "my-model".into(),
        )
        .unwrap();
        assert_eq!(provider.model_id(), "my-model");
    }

    #[test]
    fn task_as_str_all_variants() {
        assert_eq!(EmbeddingTask::Retrieval.as_str(), "retrieval");
        assert_eq!(
            EmbeddingTask::SemanticSimilarity.as_str(),
            "semantic_similarity"
        );
        assert_eq!(EmbeddingTask::Classification.as_str(), "classification");
        assert_eq!(EmbeddingTask::Clustering.as_str(), "clustering");
        assert_eq!(EmbeddingTask::Document.as_str(), "document");
        assert_eq!(EmbeddingTask::Raw.as_str(), "raw");
    }
}

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExampleComment {
    pub author: String,
    pub body: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExampleNote {
    pub id: String,
    pub title: String,
    pub status: String,
    pub comments: Vec<ExampleComment>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AddCommentInput {
    pub note_id: String,
    pub author: String,
    pub body: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReviewSummary {
    pub project: String,
    pub open_count: usize,
    pub slugs: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DemoReport {
    pub summary: ReviewSummary,
    pub runtime_result: serde_json::Value,
    pub emitted_files: Vec<String>,
    pub view_uri: String,
    pub export_path: String,
    pub notes: Vec<ExampleNote>,
}

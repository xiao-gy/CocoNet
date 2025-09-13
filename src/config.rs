use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Config {
    pub cidr: String,
    pub bootstrap: Vec<String>,
    pub relay: bool,
}

#[derive(thiserror::Error, Debug)]
pub enum InternalError {
    #[error("Generic error: {0}")]
    Generic(String),

    #[error(transparent)]
    IO(#[from] std::io::Error),

    #[error(transparent)]
    Config(#[from] confy::ConfyError),

    #[error(transparent)]
    TomlParseError(#[from] toml::de::Error),

    #[error(transparent)]
    PolarsError(#[from] polars::error::PolarsError),

    #[error(transparent)]
    JsonError(#[from] serde_json::error::Error),
}

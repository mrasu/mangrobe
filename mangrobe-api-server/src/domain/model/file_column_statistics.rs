#[derive(Debug, Clone)]
pub struct FileColumnStatistics {
    pub column_name: String,
    pub min: Option<f64>,
    pub max: Option<f64>,
}

impl FileColumnStatistics {
    pub fn new(column_name: String, min: Option<f64>, max: Option<f64>) -> Self {
        Self {
            column_name,
            min,
            max,
        }
    }
}

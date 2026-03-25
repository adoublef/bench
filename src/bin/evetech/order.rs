use serde::{Deserialize, Serialize};

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct Order {
    duration: i64,
    is_buy_order: bool,
    issued: String,
    location_id: i64,
    min_volume: i64,
    order_id: i64,
    price: f64,
    range: String,
    system_id: i64,
    type_id: i64,
    volume_remain: i64,
    volume_total: i64,
}

use arrow::datatypes::{Schema, Field, DataType};

pub fn category_schema() -> Schema {
    Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ])
}

pub fn person_schema() -> Schema {
    Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("email_address", DataType::Utf8, false),
        Field::new("credit_card", DataType::Utf8, false),
        Field::new("city", DataType::Utf8, false),
        Field::new("state", DataType::Utf8, false),
        Field::new("date_time", DataType::Int64, false),
        Field::new("extra", DataType::Utf8, false),
    ])
}

pub fn auction_schema() -> Schema {
    Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("item_name", DataType::Utf8, false),
        Field::new("description", DataType::Utf8, false),
        Field::new("initial_bid", DataType::Int64, false),
        Field::new("reserve", DataType::Int64, false),
        Field::new("date_time", DataType::Int64, false),
        Field::new("expires", DataType::Int64, false),
        Field::new("seller", DataType::Int64, false),
        Field::new("category", DataType::Int64, false),
        Field::new("extra", DataType::Utf8, false),
    ])
}

pub fn bid_schema() -> Schema {
    Schema::new(vec![
        Field::new("auction", DataType::Int64, false),
        Field::new("bidder", DataType::Int64, false),
        Field::new("price", DataType::Int64, false),
        Field::new("channel", DataType::Utf8, false),
        Field::new("url", DataType::Utf8, false),
        Field::new("date_time", DataType::Int64, false),
        Field::new("extra", DataType::Utf8, false),
    ])
}

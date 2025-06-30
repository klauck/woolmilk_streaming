use arrow::array::*;
use arrow::record_batch::RecordBatch;
use std::collections::HashSet;
use std::sync::Arc;

use nexmark::config::NexmarkConfig;
use nexmark::event::{Auction, Bid, Event, Person};
use nexmark::EventGenerator;

use super::{auction_schema, bid_schema, category_schema, person_schema};

#[derive(Debug)]
pub struct Category {
    pub id: usize,
    pub name: String,
}

#[derive(Clone)]
pub struct NexmarkDataGenerator {
    pub chunk_size: usize,
    pub no_records: usize,
}

impl NexmarkDataGenerator {
    pub fn new(chunk_size: usize, no_records: usize) -> Self {
        Self {
            chunk_size,
            no_records,
        }
    }

    pub fn iter_batches(&self) -> NexmarkDataIterator {
        NexmarkDataIterator {
            config: self.clone(),
            generator: EventGenerator::new(NexmarkConfig {
                bid_proportion: 48,
                person_proportion: 1,
                auction_proportion: 1,
                ..Default::default()
            }),
            people: Vec::with_capacity(self.chunk_size),
            auctions: Vec::with_capacity(self.chunk_size),
            bids: Vec::with_capacity(self.chunk_size),
            remaining: self.no_records,
        }
    }

    pub fn generate_categories(&self, auctions: &[Auction]) -> Vec<Category> {
        let mut seen = HashSet::new();
        auctions
            .iter()
            .filter_map(|a| {
                if seen.insert(a.category) {
                    Some(Category {
                        id: a.category,
                        name: format!("category_{}", a.category),
                    })
                } else {
                    None
                }
            })
            .collect()
    }

    pub fn build_arrow_tables(
        &self,
        people: &[Person],
        auctions: &[Auction],
        bids: &[Bid],
        categories: &[Category],
    ) -> (RecordBatch, RecordBatch, RecordBatch, RecordBatch) {
        let person_schema = Arc::new(person_schema());
        let auction_schema = Arc::new(auction_schema());
        let bid_schema = Arc::new(bid_schema());
        let category_schema = Arc::new(category_schema());

        let person_batch = RecordBatch::try_new(
            person_schema,
            vec![
                Arc::new(Int64Array::from(people.iter().map(|p| p.id as i64).collect::<Vec<_>>())),
                Arc::new(StringArray::from(people.iter().map(|p| p.name.clone()).collect::<Vec<_>>())),
                Arc::new(StringArray::from(people.iter().map(|p| p.email_address.clone()).collect::<Vec<_>>())),
                Arc::new(StringArray::from(people.iter().map(|p| p.credit_card.clone()).collect::<Vec<_>>())),
                Arc::new(StringArray::from(people.iter().map(|p| p.city.clone()).collect::<Vec<_>>())),
                Arc::new(StringArray::from(people.iter().map(|p| p.state.clone()).collect::<Vec<_>>())),
                Arc::new(Int64Array::from(people.iter().map(|p| p.date_time as i64).collect::<Vec<_>>())),
                Arc::new(StringArray::from(people.iter().map(|p| p.extra.clone()).collect::<Vec<_>>())),
            ],
        ).unwrap();

        let auction_batch = RecordBatch::try_new(
            auction_schema,
            vec![
                Arc::new(Int64Array::from(auctions.iter().map(|a| a.id as i64).collect::<Vec<_>>())),
                Arc::new(StringArray::from(auctions.iter().map(|a| a.item_name.clone()).collect::<Vec<_>>())),
                Arc::new(StringArray::from(auctions.iter().map(|a| a.description.clone()).collect::<Vec<_>>())),
                Arc::new(Int64Array::from(auctions.iter().map(|a| a.initial_bid as i64).collect::<Vec<_>>())),
                Arc::new(Int64Array::from(auctions.iter().map(|a| a.reserve as i64).collect::<Vec<_>>())),
                Arc::new(Int64Array::from(auctions.iter().map(|a| a.date_time as i64).collect::<Vec<_>>())),
                Arc::new(Int64Array::from(auctions.iter().map(|a| a.expires as i64).collect::<Vec<_>>())),
                Arc::new(Int64Array::from(auctions.iter().map(|a| a.seller as i64).collect::<Vec<_>>())),
                Arc::new(Int64Array::from(auctions.iter().map(|a| a.category as i64).collect::<Vec<_>>())),
                Arc::new(StringArray::from(auctions.iter().map(|a| a.extra.clone()).collect::<Vec<_>>())),
            ],
        ).unwrap();

        let bid_batch = RecordBatch::try_new(
            bid_schema,
            vec![
                Arc::new(Int64Array::from(bids.iter().map(|b| b.auction as i64).collect::<Vec<_>>())),
                Arc::new(Int64Array::from(bids.iter().map(|b| b.bidder as i64 ).collect::<Vec<_>>())),
                Arc::new(Int64Array::from(bids.iter().map(|b| b.price as i64).collect::<Vec<_>>())),
                Arc::new(StringArray::from(bids.iter().map(|b| b.channel.clone()).collect::<Vec<_>>())),
                Arc::new(StringArray::from(bids.iter().map(|b| b.url.clone()).collect::<Vec<_>>())),
                Arc::new(Int64Array::from(bids.iter().map(|b| b.date_time as i64 ).collect::<Vec<_>>())),
                Arc::new(StringArray::from(bids.iter().map(|b| b.extra.clone()).collect::<Vec<_>>())),
            ],
        ).unwrap();

        let category_batch = RecordBatch::try_new(
            category_schema,
            vec![
                Arc::new(Int64Array::from(categories.iter().map(|c| c.id as i64).collect::<Vec<_>>())),
                Arc::new(StringArray::from(categories.iter().map(|c| c.name.clone()).collect::<Vec<_>>())),
            ],
        ).unwrap();

        (person_batch, auction_batch, bid_batch, category_batch)
    }
}

pub struct NexmarkDataIterator {
    config: NexmarkDataGenerator,
    generator: EventGenerator,
    people: Vec<Person>,
    auctions: Vec<Auction>,
    bids: Vec<Bid>,
    remaining: usize,
}

//iterator just like yeild in python
impl Iterator for NexmarkDataIterator {
    type Item = (RecordBatch, RecordBatch, RecordBatch, RecordBatch);

    fn next(&mut self) -> Option<Self::Item> {
        while self.remaining > 0 {
            if let Some(event) = self.generator.next() {
                self.remaining -= 1;

                match event {
                    Event::Person(p) => self.people.push(p),
                    Event::Auction(a) => self.auctions.push(a),
                    Event::Bid(b) => self.bids.push(b),
                }

                if self.people.len() + self.auctions.len() + self.bids.len() >= self.config.chunk_size {
                    let categories = self.config.generate_categories(&self.auctions);
                    let batch = self.config.build_arrow_tables(&self.people, &self.auctions, &self.bids, &categories);

                    self.people.clear();
                    self.auctions.clear();
                    self.bids.clear();

                    return Some(batch);
                }
            }
        }

        if !self.people.is_empty() || !self.auctions.is_empty() || !self.bids.is_empty() {
            let categories = self.config.generate_categories(&self.auctions);
            let batch = self.config.build_arrow_tables(&self.people, &self.auctions, &self.bids, &categories);

            self.people.clear();
            self.auctions.clear();
            self.bids.clear();

            return Some(batch);
        }

        None
    }
}

mod nexmark;

use nexmark::{NexmarkDataGenerator};

fn main() {
    let generator = NexmarkDataGenerator::new(1000, 5000);

    for (people, auctions, bids, categories) in generator.iter_batches() {
        println!("Got batch: people={}, auctions={}, bids={}, categories={}",
            people.num_rows(),
            auctions.num_rows(),
            bids.num_rows(),
            categories.num_rows()
        );
    }
}

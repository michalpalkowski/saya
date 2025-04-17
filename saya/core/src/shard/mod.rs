use crate::{prover::SnosProof, service::Daemon};
use anyhow::Result;
use swiftness::types::StarkProof;
use tokio::sync::mpsc::Receiver;

mod aggregator;
use crate::shard::aggregator::AggregatorError;
pub use aggregator::{AggregatorMock, AggregatorMockBuilder};
mod shard_output;
pub trait AggregatorBuilder {
    type Aggregator: Aggregator;

    fn build(self) -> Result<Self::Aggregator, AggregatorError>;

    fn channel(self, channel: Receiver<SnosProof<StarkProof>>) -> Self;
}

pub trait Aggregator: Daemon {}

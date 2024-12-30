use pallas::{
    ledger::traverse::MultiEraHeader,
    network::{
        facades::PeerClient,
        miniprotocols::{blockfetch, chainsync, keepalive, Point, MAINNET_MAGIC},
    },
};

use thiserror::Error;
use tokio::time::Instant;

#[derive(Error, Debug)]
pub enum Error {
    #[error("hex conversion error")]
    FromHexError(#[from] hex::FromHexError),

    #[error("blockfetch error")]
    BlockFetchError(#[from] blockfetch::ClientError),

    #[error("chainsync error")]
    ChainSyncError(#[from] chainsync::ClientError),

    #[error("keepalive error")]
    KeepAliveError(#[from] keepalive::ClientError),

    #[error("pallas_traverse error")]
    PallasTraverseError(#[from] pallas::ledger::traverse::Error),
}

async fn do_blockfetch(
    blockfetch_client: &mut blockfetch::Client,
    range: (Point, Point),
) -> Result<(), Error> {
    let blocks = blockfetch_client.fetch_range(range.clone()).await?;

    for block in &blocks {
        tracing::trace!("received block of size: {}", block.len());
    }
    tracing::info!(
        "received {} blocks. last slot: {}",
        blocks.len(),
        range.1.slot_or_default()
    );
    Ok(())
}

async fn do_chainsync(
    mut chainsync_client: chainsync::N2NClient,
    mut blockfetch_client: blockfetch::Client,
) -> Result<(), Error> {
    let known_points = vec![Point::Specific(
        143972014u64,
        hex::decode("08a63ac44512f5feca525bde4a71026b11337879285fd0b992cd4e86d1de1daa")?,
    )];

    let (point, _) = chainsync_client.find_intersect(known_points).await?;

    tracing::info!("intersected point is {:?}", point);

    let mut block_count = 0u16;
    let mut start_point = Point::Specific(0, vec![]);
    let mut end_point: Point;
    let mut next_log = Instant::now();
    loop {
        let next = chainsync_client.request_or_await_next().await?;

        match next {
            chainsync::NextResponse::RollForward(h, t) => {
                tracing::trace!("rolling forward, header size: {}", h.cbor.len());
                let point = match h.byron_prefix {
                    None => {
                        let multi_era_header = MultiEraHeader::decode(h.variant, None, &h.cbor)?;
                        let slot = multi_era_header.slot();
                        let hash = multi_era_header.hash().to_vec();
                        let number = multi_era_header.number();
                        match &multi_era_header {
                            MultiEraHeader::EpochBoundary(_) => {
                                tracing::info!("epoch boundary");
                                None
                            }
                            MultiEraHeader::ShelleyCompatible(_)
                            | MultiEraHeader::BabbageCompatible(_) => {
                                if next_log.elapsed().as_secs() > 1 {
                                    tracing::info!("chainsync block header: {}, tip: {:?}", number, t);
                                    next_log = Instant::now();
                                }
                                Some(Point::Specific(slot, hash))
                            }
                            MultiEraHeader::Byron(_) => {
                                tracing::info!("ignoring byron header");
                                None
                            }
                        }
                    }
                    Some(_) => {
                        tracing::info!("skipping byron block");
                        None
                    }
                };
                if let Some(p) = point {
                    block_count += 1;
                    if block_count == 1 {
                        start_point = p;
                    } else if block_count == 10 {
                        end_point = p;
                        do_blockfetch(
                            &mut blockfetch_client,
                            (start_point.clone(), end_point.clone()),
                        )
                        .await?;
                        block_count = 0;
                    }
                };
            }
            chainsync::NextResponse::RollBackward(x, _) => log::info!("rollback to {:?}", x),
            chainsync::NextResponse::Await => tracing::info!("tip of chaing reached"),
        };
    }
}

#[tokio::main]
async fn main() {
    tracing::subscriber::set_global_default(
        tracing_subscriber::FmtSubscriber::builder()
            .with_max_level(tracing::Level::INFO)
            .finish(),
    )
    .unwrap();

    loop {
        // setup a TCP socket to act as data bearer between our agents and the remote
        // relay.
        let server = "127.0.0.1:6001";

        // let server = "localhost:6000";
        let peer = PeerClient::connect(server, MAINNET_MAGIC).await.unwrap();

        let PeerClient {
            plexer,
            chainsync,
            blockfetch,
            ..
        } = peer;

        do_chainsync(chainsync, blockfetch).await.unwrap();

        plexer.abort().await;

        tracing::info!("waiting 10 seconds before reconnecting...");
        tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
    }
}

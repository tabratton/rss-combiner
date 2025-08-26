#![feature(duration_constructors)]

use axum::Router;
use axum::body::Body;
use axum::extract::State;
use axum::http::{Request, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use bb8::Pool;
use bb8_postgres::PostgresConnectionManager;
use lazy_static::lazy_static;
use moka::future::{Cache, CacheBuilder};
use rss::{Channel, ChannelBuilder, Item};
use sentry::integrations::contexts::ContextIntegration;
use sentry::integrations::tower::{NewSentryLayer, SentryHttpLayer};
use sentry::integrations::tracing::EventFilter;
use std::fmt::Display;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::task::JoinSet;
use tokio_postgres::{Config, NoTls, Row};
use tower::ServiceBuilder;
use tower_http::cors::CorsLayer;
use tracing::{Instrument, Span, error, info, instrument};
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::fmt::time::UtcTime;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Registry, fmt};

lazy_static! {
    static ref ITEMS_PER_FEED: usize = std::env::var("ITEMS_PER_FEED")
        .unwrap_or_default()
        .parse()
        .unwrap_or(5);
}

lazy_static! {
    static ref FEED_LINK_URL: String =
        std::env::var("FEED_LINK_URL").unwrap_or("https://example.com/feed".to_string());
}

fn setup_logging() {
    Registry::default()
        .with(EnvFilter::from_default_env())
        .with(
            fmt::layer()
                .event_format(fmt::format().with_timer(UtcTime::rfc_3339()))
                .with_target(true)
                .with_file(true)
                .with_line_number(true)
                .with_span_events(FmtSpan::CLOSE)
                .with_level(true)
                .with_thread_ids(true)
                .with_thread_names(true),
        )
        .with(
            sentry::integrations::tracing::layer().event_filter(|md| match *md.level() {
                tracing::Level::ERROR => EventFilter::Event | EventFilter::Log,
                tracing::Level::TRACE => EventFilter::Ignore,
                _ => EventFilter::Log,
            }),
        )
        .init();
}

fn main() -> Result<(), anyhow::Error> {
    let integration = ContextIntegration::new().add_os(false);
    let _guard = sentry::init((
        std::env::var("SENTRY_DSN")?,
        sentry::ClientOptions {
            release: sentry::release_name!(),
            // Capture user IPs and potentially sensitive headers when using HTTP server integrations
            // see https://docs.sentry.io/platforms/rust/data-management/data-collected for more info
            send_default_pii: true,
            enable_logs: true,
            sample_rate: 0.1,
            traces_sample_rate: 1.0,
            ..Default::default()
        }
        .add_integration(integration),
    ));

    setup_logging();

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(async { run_server().await })
}

async fn run_server() -> Result<(), anyhow::Error> {
    let postgres_url = std::env::var("DATABASE_URL")?;
    let postgres_config = Config::from_str(postgres_url.as_ref())?;
    let manager = PostgresConnectionManager::new(postgres_config, NoTls);
    let db_pool = Pool::builder().max_size(10).build(manager).await?;

    let cache_ttl: u64 = match std::env::var("CACHE_TTL_MINS") {
        Ok(v) => v.parse()?,
        Err(_) => 35,
    };
    let rss_cache = CacheBuilder::new(100)
        .time_to_live(Duration::from_mins(cache_ttl))
        .build();
    let feed_cache = CacheBuilder::new(1)
        .time_to_live(Duration::from_days(7))
        .build();

    let app_state = AppState {
        db_pool,
        rss_cache,
        feed_cache,
    };

    let layer = ServiceBuilder::new()
        .layer(NewSentryLayer::<Request<Body>>::new_from_top())
        .layer(SentryHttpLayer::new().enable_transaction().enable_pii())
        .layer(CorsLayer::permissive());

    let app: Router<()> = Router::new()
        .route("/rss", get(generate_feed))
        .route("/flush", get(flush_caches))
        .with_state(app_state)
        .layer(layer);

    let addr = format!(
        "0.0.0.0:{}",
        std::env::var("PORT").unwrap_or("8080".to_string())
    );

    info!("binding to {}", &addr);
    let listener = TcpListener::bind(addr).await?;

    info!("starting server");
    axum::serve(listener, app).await?;

    Ok(())
}

#[derive(Clone)]
struct AppState {
    db_pool: Pool<PostgresConnectionManager<NoTls>>,
    feed_cache: Cache<usize, Arc<Vec<Feed>>>,
    rss_cache: Cache<Feed, Channel>,
}

#[instrument(skip(app_state))]
async fn generate_feed(State(app_state): State<AppState>) -> Result<impl IntoResponse, AppError> {
    let db_pool = app_state.db_pool.clone();
    let rss_cache = app_state.rss_cache.clone();
    let feed_cache = app_state.feed_cache.clone();

    let feeds = match get_db_feeds(db_pool, feed_cache).await {
        Ok(feeds) => feeds,
        Err(err) => {
            error!(error = %err.0, "Error while getting list of feeds from database");
            return Err(err);
        }
    };

    let feed_items = get_feed_items(feeds, rss_cache).await;

    Ok((
        [(header::CONTENT_TYPE, "text/xml")],
        get_return_string(feed_items),
    ))
}

#[instrument(skip(db_pool, feed_cache))]
async fn get_db_feeds(
    db_pool: Pool<PostgresConnectionManager<NoTls>>,
    feed_cache: Cache<usize, Arc<Vec<Feed>>>,
) -> Result<Arc<Vec<Feed>>, AppError> {
    match feed_cache.get(&0).await {
        Some(feeds) => Ok(feeds),
        None => {
            let conn = db_pool.get().await?;
            let prepared = conn
                .prepare("SELECT url FROM rss.feeds ORDER BY id ASC")
                .await?;

            let feeds: Arc<Vec<Feed>> = Arc::new(
                conn.query(&prepared, &[])
                    .await?
                    .iter()
                    .map(|row| row.into())
                    .collect(),
            );

            feed_cache.insert(0, feeds.clone()).await;

            Ok(feeds)
        }
    }
}

#[instrument(skip(feeds, rss_cache))]
async fn get_feed_items(feeds: Arc<Vec<Feed>>, rss_cache: Cache<Feed, Channel>) -> Vec<Item> {
    let mut feed_items = Vec::new();
    let mut join_set = JoinSet::new();
    for feed in feeds.iter() {
        let cache = rss_cache.clone();
        let url = feed.clone();
        let get_feed_closure = async move { get_feed_by_url(url, cache).await };
        join_set.spawn(get_feed_closure.in_current_span());
    }

    while let Some(Ok(Some(channel))) = join_set.join_next().await {
        feed_items.extend(channel.items.iter().take(*ITEMS_PER_FEED).cloned())
    }

    feed_items
}

#[instrument(skip(rss_cache))]
async fn get_feed_by_url(url: Feed, rss_cache: Cache<Feed, Channel>) -> Option<Channel> {
    match rss_cache.get(&url).await {
        Some(channel) => {
            let current_span = Span::current();
            current_span.record("cache.result", "hit");
            Some(channel)
        }
        None => {
            let current_span = Span::current();
            current_span.record("cache.result", "miss");
            if let Some(channel) = make_request(&url).await {
                rss_cache.insert(url, channel.clone()).await;
                Some(channel)
            } else {
                None
            }
        }
    }
}

#[instrument]
async fn make_request(url: &Feed) -> Option<Channel> {
    let content = match reqwest::get(&url.0).await {
        Ok(response) => match response.status() {
            StatusCode::OK => match response.bytes().await {
                Ok(bytes) => bytes,
                Err(err) => {
                    error!(error = %err, "error reading response bytes");
                    return None;
                }
            },
            _ => {
                if let Ok(text) = response.text().await {
                    error!(url = %url, response = text, "response was not 200 OK");
                } else {
                    error!(url = %url, "response was not 200 OK, error reading response text");
                }

                return None;
            }
        },
        Err(err) => {
            error!(error = %err, url = %url, "error fetching url");
            return None;
        }
    };

    Channel::read_from(&content[..]).map_or_else(
        |err| {
            error!(error = %err, url = %url, "Could not read channel from response");
            None
        },
        Some,
    )
}

#[instrument(skip(feed_items))]
fn get_return_string(feed_items: Vec<Item>) -> String {
    let return_feed = ChannelBuilder::default()
        .title("RSS Feed Recombinator")
        .link(FEED_LINK_URL.to_string())
        .description("A combination of other feeds")
        .items(feed_items)
        .build();

    return_feed.to_string()
}

#[instrument(skip(app_state))]
async fn flush_caches(State(app_state): State<AppState>) -> impl IntoResponse {
    let rss_cache = app_state.rss_cache.clone();
    let feed_cache = app_state.feed_cache.clone();

    feed_cache.invalidate_all();
    rss_cache.invalidate_all();

    StatusCode::OK
}

#[derive(Clone, Eq, PartialEq, Debug, Hash)]
struct Feed(String);

impl From<&Row> for Feed {
    fn from(row: &Row) -> Self {
        Self(row.get("url"))
    }
}

impl From<String> for Feed {
    fn from(s: String) -> Self {
        // TODO: add url parsing
        Self(s)
    }
}

impl Display for Feed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug)]
pub struct AppError(anyhow::Error);

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        (StatusCode::INTERNAL_SERVER_ERROR, self.0.to_string()).into_response()
    }
}

impl<E> From<E> for AppError
where
    E: Into<anyhow::Error>,
{
    fn from(err: E) -> Self {
        Self(err.into())
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[tokio::test]
    async fn test_get_feed_by_url() {
        let cache = CacheBuilder::default().build();
        let mut server = mockito::Server::new_async().await;

        let url = server.url();
        let mock = server
            .mock("GET", "/rss")
            .with_status(200)
            .with_header("content-type", "text/xml")
            .with_body(include_str!("test-feed.xml"))
            .expect(1)
            .create();

        let full_url = Feed(format!("{url}/rss"));
        let channel = get_feed_by_url(full_url.clone(), cache.clone()).await;
        assert!(channel.is_some());
        mock.assert_async().await;
        assert!(cache.contains_key(&full_url));

        // get same url, should fetch from cache this time
        let channel = get_feed_by_url(Feed(format!("{url}/rss")), cache).await;
        assert!(channel.is_some());
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn test_get_feed_by_url_not_ok() {
        let cache = CacheBuilder::default().build();
        let mut server = mockito::Server::new_async().await;

        let url = server.url();
        let mock = server
            .mock("GET", "/rss")
            .with_status(500)
            .with_header("content-type", "text/xml")
            .with_body("")
            .expect(1)
            .create();

        let full_url = Feed(format!("{url}/rss"));
        let channel = get_feed_by_url(full_url.clone(), cache.clone()).await;
        assert!(channel.is_none());
        mock.assert_async().await;
        assert!(!cache.contains_key(&full_url));
    }

    #[tokio::test]
    async fn test_get_feed_items() {
        let cache = CacheBuilder::default().build();
        let mut server = mockito::Server::new_async().await;

        let url = server.url();
        let mock1 = server
            .mock("GET", "/rss1")
            .with_status(200)
            .with_header("content-type", "text/xml")
            .with_body(include_str!("test-feed.xml"))
            .expect(1)
            .create();
        let mock2 = server
            .mock("GET", "/rss2")
            .with_status(200)
            .with_header("content-type", "text/xml")
            .with_body(include_str!("test-feed.xml"))
            .expect(1)
            .create();

        let feeds = vec![Feed(format!("{url}/rss1")), Feed(format!("{url}/rss2"))];
        let items = get_feed_items(Arc::new(feeds.to_vec()), cache.clone()).await;
        assert_eq!(2, items.len());
        mock1.assert_async().await;
        mock2.assert_async().await;
        assert!(cache.contains_key(&feeds[0]));
        assert!(cache.contains_key(&feeds[1]));
    }
}

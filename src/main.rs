#![feature(duration_constructors)]

use axum::Router;
use axum::body::Body;
use axum::extract::State;
use axum::http::{Request, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use bytes::Bytes;
use lazy_static::lazy_static;
use moka::future::{Cache, CacheBuilder};
use rss::{Channel, ChannelBuilder, Item};
use sentry::Hub;
use sentry::integrations::contexts::ContextIntegration;
use sentry::integrations::tower::{NewSentryLayer, SentryHttpLayer};
use sentry::integrations::tracing::EventFilter;
use sqlx::postgres::PgPoolOptions;
use sqlx::{Pool, Postgres};
use std::fmt::Display;
use std::io::BufRead;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::task::JoinSet;
use tower::ServiceBuilder;
use tower_http::cors::CorsLayer;
use tracing::{Instrument, error, info, info_span, instrument};
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
    let db_pool = PgPoolOptions::new()
        .max_connections(10)
        .connect(postgres_url.as_ref())
        .await?;

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
    db_pool: Pool<Postgres>,
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
    db_pool: Pool<Postgres>,
    feed_cache: Cache<usize, Arc<Vec<Feed>>>,
) -> Result<Arc<Vec<Feed>>, AppError> {
    match feed_cache.get(&0).await {
        Some(feeds) => Ok(feeds),
        None => {
            let feeds: Arc<Vec<Feed>> = Arc::new(
                sqlx::query_as!(Feed, "SELECT url FROM rss.feeds ORDER BY id")
                    .fetch_all(&db_pool)
                    .await?,
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
        join_set.spawn(
            async move { get_feed_by_url(url, cache).await }
                .instrument(info_span!("spawn_get_feed_task").or_current()),
        );
    }

    while let Some(task_result) = join_set.join_next().await {
        if let Ok(Some(channel)) = task_result {
            feed_items.extend(channel.items);
        }
    }

    feed_items
}

#[instrument(skip(rss_cache))]
async fn get_feed_by_url(url: Feed, rss_cache: Cache<Feed, Channel>) -> Option<Channel> {
    match rss_cache.get(&url).await {
        Some(channel) => {
            add_metric_data("cacheResult", "hit".into());
            Some(channel)
        }
        None => {
            add_metric_data("cacheResult", "miss".into());

            if let Some(mut channel) = get_external_feed(&url, rss_cache.clone()).await {
                channel.items.truncate(*ITEMS_PER_FEED);
                rss_cache.insert(url, channel.clone()).await;
                Some(channel)
            } else {
                None
            }
        }
    }
}

#[instrument(skip(rss_cache))]
async fn get_external_feed(url: &Feed, rss_cache: Cache<Feed, Channel>) -> Option<Channel> {
    let content = match make_http_request(url.clone().into()).await {
        Ok(content) => content,
        Err(HttpError::TooManyRequests) => {
            tokio::spawn(
                retry(url.clone(), 3, rss_cache).instrument(info_span!("retry_task").or_current()),
            );
            return None;
        }
        Err(_) => return None,
    };

    channel_from_content(&content[..], url)
}

async fn retry(url: Feed, retries: usize, rss_cache: Cache<Feed, Channel>) {
    let secs = rand::random_range(30..(15 * 10));
    tokio::time::sleep(Duration::from_secs(secs)).await;
    match make_http_request(url.clone().into()).await {
        Ok(content) => {
            if let Some(mut channel) = channel_from_content(&content[..], &url) {
                channel.items.truncate(*ITEMS_PER_FEED);
                rss_cache.insert(url, channel.clone()).await;
            }
        }
        Err(_) => {
            if retries > 0 {
                Box::pin(retry(url, retries - 1, rss_cache)).await;
            }
        }
    }
}

#[instrument]
async fn make_http_request(url: String) -> Result<Bytes, HttpError> {
    match reqwest::get(&url).await {
        Ok(response) => match response.status() {
            StatusCode::OK => match response.bytes().await {
                Ok(bytes) => Ok(bytes),
                Err(err) => {
                    error!(error = %err, "error reading response bytes");
                    Err(HttpError::ReadError)
                }
            },
            StatusCode::TOO_MANY_REQUESTS => {
                error!(url = %url, "too many requests");
                Err(HttpError::TooManyRequests)
            }
            _ => {
                if let Ok(text) = response.text().await {
                    error!(url = %url, response = text, "response was not 200 OK");
                } else {
                    error!(url = %url, "response was not 200 OK, error reading response text");
                }

                Err(HttpError::NotOk)
            }
        },
        Err(err) => {
            error!(error = %err, url = %url, "error fetching url");
            Err(HttpError::ConnectionError)
        }
    }
}

enum HttpError {
    ConnectionError,
    NotOk,
    TooManyRequests,
    ReadError,
}

fn channel_from_content(content: impl BufRead, url: &Feed) -> Option<Channel> {
    Channel::read_from(content).map_or_else(
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

fn add_metric_data(key: &str, value: sentry::protocol::Value) {
    if let Some(span) = Hub::current().configure_scope(|scope| scope.get_span()) {
        span.set_data(key, value);
    }
}

#[derive(Clone, Eq, PartialEq, Debug, Hash)]
struct Feed {
    url: String,
}

impl From<String> for Feed {
    fn from(s: String) -> Self {
        // TODO: add url parsing
        Self { url: s }
    }
}

impl From<&String> for Feed {
    fn from(s: &String) -> Self {
        // TODO: add url parsing
        Self { url: s.clone() }
    }
}

impl From<Feed> for String {
    fn from(feed: Feed) -> Self {
        feed.url
    }
}

impl Display for Feed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.url)
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

        let full_url: Feed = format!("{url}/rss").into();
        let channel = get_feed_by_url(full_url.clone(), cache.clone()).await;
        assert!(channel.is_some());
        mock.assert_async().await;
        assert!(cache.contains_key(&full_url));

        // get same url, should fetch from cache this time
        let channel = get_feed_by_url(format!("{url}/rss").into(), cache).await;
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

        let full_url: Feed = format!("{url}/rss").into();
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

        let feeds = vec![format!("{url}/rss1").into(), format!("{url}/rss2").into()];
        let items = get_feed_items(Arc::new(feeds.to_vec()), cache.clone()).await;
        assert_eq!(2, items.len());
        mock1.assert_async().await;
        mock2.assert_async().await;
        assert!(cache.contains_key(&feeds[0]));
        assert!(cache.contains_key(&feeds[1]));
    }
}

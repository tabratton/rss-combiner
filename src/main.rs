use axum::Router;
use axum::body::Body;
use axum::extract::{FromRef, State};
use axum::http::{Request, StatusCode, header};
use axum::response::IntoResponse;
use axum::routing::get;
use bb8::Pool;
use bb8_postgres::PostgresConnectionManager;
use moka::future::{Cache, CacheBuilder};
use rss::{Channel, ChannelBuilder, Item};
use sentry::integrations::tracing::EventFilter;
use std::str::FromStr;
use std::time::Duration;
use tokio::task::JoinSet;
use tokio_postgres::{Config, NoTls, Row};
use tower::ServiceBuilder;
use tower_http::cors::CorsLayer;
use tracing::{error, info, instrument};
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::fmt::time::UtcTime;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Registry, fmt};

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
    let integration = sentry::integrations::contexts::ContextIntegration::new().add_os(false);
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

    let rss_cache = CacheBuilder::new(100)
        .time_to_live(Duration::from_mins(25))
        .build();

    let app_state = AppState { db_pool, rss_cache };

    let layer = ServiceBuilder::new()
        .layer(sentry::integrations::tower::NewSentryLayer::<Request<Body>>::new_from_top())
        .layer(
            sentry::integrations::tower::SentryHttpLayer::new()
                .enable_transaction()
                .enable_pii(),
        )
        .layer(CorsLayer::permissive());

    let app: Router<()> = Router::new()
        .route("/rss", get(generate_feed))
        .with_state(app_state)
        .layer(layer);

    let addr = format!(
        "0.0.0.0:{}",
        std::env::var("PORT").unwrap_or("8080".to_string())
    );

    info!("binding to {}", &addr);
    let listener = tokio::net::TcpListener::bind(addr).await?;

    info!("starting server");
    axum::serve(listener, app).await?;

    Ok(())
}

#[derive(Clone)]
struct AppState {
    db_pool: Pool<PostgresConnectionManager<NoTls>>,
    rss_cache: Cache<String, Channel>,
}

impl FromRef<AppState> for Pool<PostgresConnectionManager<NoTls>> {
    fn from_ref(app_state: &AppState) -> Self {
        app_state.db_pool.clone()
    }
}

#[instrument(skip(app_state))]
async fn generate_feed(State(app_state): State<AppState>) -> Result<impl IntoResponse, AppError> {
    let db_pool = app_state.db_pool.clone();
    let rss_cache = app_state.rss_cache.clone();

    let feeds = match get_db_feeds(db_pool).await {
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

#[instrument(skip(db_pool))]
async fn get_db_feeds(
    db_pool: Pool<PostgresConnectionManager<NoTls>>,
) -> Result<Vec<Feed>, AppError> {
    let conn = db_pool.get().await?;
    let prepared = conn
        .prepare("SELECT url FROM rss.feeds ORDER BY id ASC")
        .await?;

    Ok(conn
        .query(&prepared, &[])
        .await?
        .iter()
        .map(|row| row.into())
        .collect())
}

#[instrument(skip(feeds, rss_cache))]
async fn get_feed_items(feeds: Vec<Feed>, rss_cache: Cache<String, Channel>) -> Vec<Item> {
    let mut feed_items = Vec::new();
    let mut join_set = JoinSet::new();
    for feed in feeds {
        let cache = rss_cache.clone();
        join_set.spawn(async move { get_feed_by_url(feed.url, cache).await });
    }

    while let Some(Ok(res)) = join_set.join_next().await {
        if let Some(channel) = res {
            feed_items.extend(channel.items)
        }
    }

    feed_items
}

#[instrument(skip(rss_cache))]
async fn get_feed_by_url(url: String, rss_cache: Cache<String, Channel>) -> Option<Channel> {
    let channel = match rss_cache.get(&url).await {
        Some(channel) => {
            info!("found channel in cache");
            channel
        }
        None => {
            info!("channel not found in cache");
            let content = match reqwest::get(&url).await {
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
            let channel: Channel = match Channel::read_from(&content[..]) {
                Ok(channel) => channel,
                Err(err) => {
                    error!(error = %err, url = %url, "Could not read channel from response");
                    return None;
                }
            };
            rss_cache.insert(url, channel.clone()).await;
            channel
        }
    };

    Some(channel)
}

#[instrument(skip(feed_items))]
fn get_return_string(feed_items: Vec<Item>) -> String {
    let return_feed = ChannelBuilder::default()
        .title("RSS Feed Recombinator")
        .link("http://example.com/feed")
        .description("A combination of other feeds")
        .items(feed_items)
        .build();

    return_feed.to_string()
}

struct Feed {
    // id: i64,
    url: String,
}

impl From<&Row> for Feed {
    fn from(row: &Row) -> Self {
        Self {
            // id: row.get("id"),
            url: row.get("url"),
        }
    }
}

#[derive(Debug)]
pub struct AppError(anyhow::Error);

impl IntoResponse for AppError {
    fn into_response(self) -> axum::response::Response {
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

use axum::Router;
use axum::extract::{FromRef, State};
use axum::http::{StatusCode, header};
use axum::response::IntoResponse;
use axum::routing::get;
use bb8::Pool;
use bb8_postgres::PostgresConnectionManager;
use rss::{Channel, ChannelBuilder, Item};
use std::str::FromStr;
use tokio_postgres::{Config, NoTls, Row};
use tower::ServiceBuilder;
use tower_http::cors::CorsLayer;
use tracing::{info, instrument};
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
        .init();
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    setup_logging();

    let postgres_url = std::env::var("DATABASE_URL")?;
    let postgres_config = Config::from_str(postgres_url.as_ref())?;
    let manager = PostgresConnectionManager::new(postgres_config, NoTls);
    let pool = Pool::builder().max_size(10).build(manager).await?;

    let app_state = AppState {
        db_pool: pool.clone(),
    };

    let layer = ServiceBuilder::new().layer(CorsLayer::permissive());

    let app: Router<()> = Router::new()
        .route("/rss", get(get_rss_feed))
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
}

impl FromRef<AppState> for Pool<PostgresConnectionManager<NoTls>> {
    fn from_ref(app_state: &AppState) -> Self {
        app_state.db_pool.clone()
    }
}

#[instrument(skip(db_pool))]
async fn get_rss_feed(
    State(db_pool): State<Pool<PostgresConnectionManager<NoTls>>>,
) -> Result<impl IntoResponse, AppError> {
    let feeds = get_db_feeds(db_pool).await?;
    let feed_items = get_feeds_by_url(feeds).await?;

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

#[instrument(skip(feeds))]
async fn get_feeds_by_url(feeds: Vec<Feed>) -> Result<Vec<Item>, AppError> {
    let mut feed_items = Vec::new();
    for feed in feeds {
        let content = reqwest::get(feed.url).await?.bytes().await?;
        let channel: Channel = Channel::read_from(&content[..])?;
        for item in channel.items {
            feed_items.push(item);
        }
    }

    Ok(feed_items)
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

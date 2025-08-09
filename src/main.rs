use axum::Router;
use axum::extract::{FromRef, State};
use axum::http::{StatusCode, header};
use axum::response::IntoResponse;
use axum::routing::get;
use bb8::Pool;
use bb8_postgres::PostgresConnectionManager;
use rss::{Channel, ChannelBuilder};
use std::str::FromStr;
use tokio_postgres::{Config, NoTls, Row};
use tower::ServiceBuilder;
use tower_http::cors::CorsLayer;

// TODO: Add tracing/logging

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
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

    let listener = tokio::net::TcpListener::bind(addr).await?;

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

async fn get_rss_feed(
    State(db_pool): State<Pool<PostgresConnectionManager<NoTls>>>,
) -> Result<impl IntoResponse, AppError> {
    let conn = db_pool.get().await?;

    let prepared = conn.prepare("SELECT url FROM rss.feeds ORDER BY id ASC").await?;
    let feeds: Vec<Feed> = conn
        .query(&prepared, &[])
        .await?
        .iter()
        .map(|row| row.into())
        .collect();

    let mut feed_items = Vec::new();
    for feed in feeds {
        let content = reqwest::get(feed.url).await?.bytes().await?;
        let channel: Channel = Channel::read_from(&content[..])?;
        for item in channel.items {
            feed_items.push(item);
        }
    }

    let return_feed = ChannelBuilder::default()
        .title("RSS Feed Recombinator")
        .link("http://example.com/feed")
        .description("A combination of other feeds")
        .items(feed_items)
        .build();

    Ok((
        [(header::CONTENT_TYPE, "text/xml")],
        return_feed.to_string(),
    ))
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

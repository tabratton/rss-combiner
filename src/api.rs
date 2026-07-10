use crate::db::{
    delete_db_feed, get_db_feeds, get_db_feeds_hard_read, save_db_feed, update_db_feed,
};
use crate::{
    AppError, AppState, Feed, FullFeed, get_feed_items, get_return_string, populate_feed_cache,
};
use anyhow::anyhow;
use axum::Json;
use axum::extract::{Path, State};
use axum::http::{StatusCode, header};
use axum::response::IntoResponse;
use std::sync::Arc;
use tracing::{error, instrument};

#[instrument(skip(app_state))]
pub async fn generate_feed(
    State(app_state): State<AppState>,
) -> Result<impl IntoResponse, AppError> {
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

#[instrument(skip(app_state))]
pub async fn list_feed_items(
    State(app_state): State<AppState>,
) -> Result<impl IntoResponse, AppError> {
    let db_pool = app_state.db_pool.clone();
    let feed_cache = app_state.feed_cache.clone();

    match get_db_feeds(db_pool, feed_cache).await {
        Ok(feeds) => Ok(Json(feeds)),
        Err(err) => {
            error!(error = %err.0, "Error while getting list of feeds from database");
            Err(err)
        }
    }
}

#[instrument(skip(app_state))]
pub async fn create_feed_item(
    State(app_state): State<AppState>,
    Json(feed): Json<Feed>,
) -> Result<impl IntoResponse, AppError> {
    let db_pool = app_state.db_pool.clone();
    let rss_cache = app_state.rss_cache.clone();
    let feed_cache = app_state.feed_cache.clone();
    let saved_feed = save_db_feed(db_pool.clone(), feed).await?;
    let feed_url = saved_feed.clone().into();

    tokio::spawn(async move {
        populate_feed_cache(feed_url, rss_cache).await;
    });

    tokio::spawn(async move {
        if let Ok(feeds) = get_db_feeds_hard_read(db_pool).await {
            feed_cache.insert(0, Arc::new(feeds)).await;
        }
    });

    Ok(Json(saved_feed))
}

#[instrument(skip(app_state))]
pub async fn update_feed_item(
    State(app_state): State<AppState>,
    Path(id): Path<i64>,
    Json(feed): Json<FullFeed>,
) -> Result<impl IntoResponse, AppError> {
    if id != feed.id {
        return Err(anyhow!("bad request").into());
    }

    let db_pool = app_state.db_pool.clone();
    let feed_cache = app_state.feed_cache.clone();
    let saved_feed = update_db_feed(db_pool.clone(), feed).await?;

    feed_cache
        .insert(0, Arc::new(get_db_feeds_hard_read(db_pool).await?))
        .await;

    Ok(Json(saved_feed))
}

#[instrument(skip(app_state))]
pub async fn delete_feed_item(
    State(app_state): State<AppState>,
    Path(id): Path<i64>,
) -> Result<impl IntoResponse, AppError> {
    let db_pool = app_state.db_pool.clone();
    let feed_cache = app_state.feed_cache.clone();

    if delete_db_feed(db_pool.clone(), id).await.is_ok() {
        feed_cache.invalidate_all();
        Ok(StatusCode::ACCEPTED)
    } else {
        Err(anyhow!("Error while deleting feed").into())
    }
}

#[instrument(skip(app_state))]
pub async fn flush_caches(State(app_state): State<AppState>) -> impl IntoResponse {
    let rss_cache = app_state.rss_cache.clone();
    let feed_cache = app_state.feed_cache.clone();

    feed_cache.invalidate_all();
    rss_cache.invalidate_all();

    StatusCode::OK
}

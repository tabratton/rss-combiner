use crate::{AppError, Feed, FullFeed};
use anyhow::anyhow;
use moka::future::Cache;
use sqlx::{Pool, Postgres};
use std::sync::Arc;
use tracing::instrument;

#[instrument(skip(db_pool, feed_cache))]
pub async fn get_db_feeds(
    db_pool: Pool<Postgres>,
    feed_cache: Cache<usize, Arc<Vec<Feed>>>,
) -> Result<Arc<Vec<Feed>>, AppError> {
    match feed_cache.get(&0).await {
        Some(feeds) => Ok(feeds),
        None => {
            let feeds: Arc<Vec<Feed>> = Arc::new(get_db_feeds_hard_read(db_pool).await?);

            feed_cache.insert(0, feeds.clone()).await;

            Ok(feeds)
        }
    }
}

#[instrument(skip(db_pool))]
pub async fn get_db_feeds_hard_read(db_pool: Pool<Postgres>) -> Result<Vec<Feed>, AppError> {
    Ok(
        sqlx::query_as!(Feed, "SELECT url FROM rss.feeds ORDER BY id")
            .fetch_all(&db_pool)
            .await?,
    )
}

#[instrument(skip(db_pool))]
pub async fn save_db_feed(db_pool: Pool<Postgres>, feed: Feed) -> Result<FullFeed, AppError> {
    Ok(sqlx::query_as!(
        FullFeed,
        "INSERT INTO rss.feeds (url) VALUES ($1) RETURNING id, url",
        &feed.url
    )
    .fetch_one(&db_pool)
    .await?)
}

#[instrument(skip(db_pool))]
pub async fn update_db_feed(db_pool: Pool<Postgres>, feed: FullFeed) -> Result<FullFeed, AppError> {
    Ok(sqlx::query_as!(
        FullFeed,
        "INSERT INTO rss.feeds (id, url) VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET url=EXCLUDED.url RETURNING id, url",
        &feed.id,
        &feed.url
    )
        .fetch_one(&db_pool)
        .await?)
}

#[instrument(skip(db_pool))]
pub async fn delete_db_feed(db_pool: Pool<Postgres>, id: i64) -> Result<(), AppError> {
    if sqlx::query!("DELETE FROM rss.feeds WHERE id=$1", &id)
        .execute(&db_pool)
        .await
        .is_ok()
    {
        Ok(())
    } else {
        Err(anyhow!("Failed to delete rss.feed").into())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use moka::future::CacheBuilder;
    use sqlx::postgres::PgPoolOptions;

    #[tokio::test]
    async fn test_get_db_feeds() -> Result<(), anyhow::Error> {
        let postgres_url = std::env::var("DATABASE_URL")?;
        let db_pool = PgPoolOptions::new()
            .max_connections(10)
            .connect(postgres_url.as_ref())
            .await?;
        let cache = CacheBuilder::default().build();

        // setup db
        sqlx::query("INSERT INTO rss.feeds (id, url) VALUES ($1, $2)")
            .bind(1)
            .bind("https://test.com".to_string())
            .execute(&db_pool)
            .await?;
        sqlx::query("INSERT INTO rss.feeds (id, url) VALUES ($1, $2)")
            .bind(2)
            .bind("https://test2.com".to_string())
            .execute(&db_pool)
            .await?;

        let feeds = get_db_feeds(db_pool, cache).await.unwrap();

        assert_eq!(2, feeds.len());
        assert!(feeds.iter().any(|feed| feed.url == "https://test.com"));
        assert!(feeds.iter().any(|feed| feed.url == "https://test2.com"));

        Ok(())
    }
}

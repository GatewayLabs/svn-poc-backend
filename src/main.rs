use compute::prelude::*;
use lambda_runtime::{service_fn, Error, LambdaEvent};
use redis::{aio::Connection, AsyncCommands, Client};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::env;
use tracing::{error, info};

#[derive(Debug, Deserialize)]
struct SleepMetrics {
    rem_sleep: u16,
    deep_sleep: u16,
    total_sleep: u16,
    restfulness: u16,
    efficiency: u16,
    timing: u16,
    latency: u16,
}

#[derive(Serialize)]
struct SleepMetricsResponse {
    rem_sleep: String,
    deep_sleep: String,
    total_sleep: String,
    restfulness: String,
    efficiency: String,
    timing: String,
    latency: String,
}

struct BaselineAverages {
    rem_sleep: u16,
    deep_sleep: u16,
    total_sleep: u16,
    restfulness: u16,
    efficiency: u16,
    timing: u16,
    latency: u16,
}

#[encrypted(execute)]
fn evaluate_metric(value: u16, baseline: u16) -> bool {
    value >= baseline
}

#[encrypted(execute)]
fn addition(num1: u16, num2: u16) -> u16 {
    let sum = num1 + num2;
    sum
}

#[encrypted(execute)]
fn division(number: u16, count: u16) -> u16 {
    let result = number / count;
    result
}

#[encrypted(execute)]
fn multiplication(number: u16, count: u16) -> u32 {
    let result = number * count;
    result
}

async fn update_metric(
    con: &mut Connection,
    key: &str,
    new_value: u16,
) -> Result<(), redis::RedisError> {
    let avg_key = format!("{}_avg", key);
    let count_key = format!("{}_count", key);

    let current_avg: Option<u16> = con.get(&avg_key).await?;
    let current_count: Option<u16> = con.get(&count_key).await?;

    let current_avg = current_avg.unwrap_or(0);
    let current_count = current_count.unwrap_or(0);

    let new_avg = multiplication(current_avg, current_count);
    let total_sum = addition(new_avg, new_value.into());

    let new_count = addition(current_count, 1);

    let avg = total_sum / new_count as u32;

    redis::pipe()
        .atomic()
        .cmd("SET")
        .arg(&avg_key)
        .arg(avg)
        .cmd("SET")
        .arg(&count_key)
        .arg(new_count)
        .query_async(con)
        .await?;

    Ok(())
}

fn get_sleep_category(is_good: bool) -> String {
    if is_good {
        "Good Sleep Quality".to_string()
    } else {
        "Poor Sleep Quality".to_string()
    }
}

async fn get_metric_average(
    con: &mut Connection,
    key: &str,
    default: u16,
) -> Result<u16, redis::RedisError> {
    let avg_key = format!("{}_avg", key);
    let count_key = format!("{}_count", key);

    let current_avg: Option<u16> = con.get(&avg_key).await?;
    let current_count: Option<u16> = con.get(&count_key).await?;

    Ok(match (current_avg, current_count) {
        (Some(avg), Some(count)) if count > 0 => avg,
        _ => default,
    })
}

impl BaselineAverages {
    async fn from_redis(redis_client: &Client) -> Result<Self, redis::RedisError> {
        let mut con = redis_client.get_async_connection().await?;
        Ok(BaselineAverages {
            rem_sleep: get_metric_average(&mut con, "rem_sleep", 73).await?,
            deep_sleep: get_metric_average(&mut con, "deep_sleep", 85).await?,
            total_sleep: get_metric_average(&mut con, "total_sleep", 65).await?,
            restfulness: get_metric_average(&mut con, "restfulness", 81).await?,
            efficiency: get_metric_average(&mut con, "efficiency", 89).await?,
            timing: get_metric_average(&mut con, "timing", 47).await?,
            latency: get_metric_average(&mut con, "latency", 79).await?,
        })
    }

    async fn update_averages(
        redis_client: &Client,
        metrics: &SleepMetrics,
    ) -> Result<(), redis::RedisError> {
        let mut con = redis_client.get_async_connection().await?;
        update_metric(&mut con, "rem_sleep", metrics.rem_sleep).await?;
        update_metric(&mut con, "deep_sleep", metrics.deep_sleep).await?;
        update_metric(&mut con, "total_sleep", metrics.total_sleep).await?;
        update_metric(&mut con, "restfulness", metrics.restfulness).await?;
        update_metric(&mut con, "efficiency", metrics.efficiency).await?;
        update_metric(&mut con, "timing", metrics.timing).await?;
        update_metric(&mut con, "latency", metrics.latency).await?;
        Ok(())
    }
}

async fn handler(event: LambdaEvent<Value>) -> Result<SleepMetricsResponse, Error> {
    let redis_url = env::var("REDIS_URL").map_err(|_| {
        error!("REDIS_URL environment variable not set");
        Error::from("Missing REDIS_URL configuration")
    })?;

    let redis_client = Client::open(redis_url).map_err(|e| {
        error!("Failed to create Redis client: {:?}", e);
        e
    })?;

    let payload = &event.payload;

    let body_str = payload.get("body").and_then(Value::as_str).ok_or_else(|| {
        error!("Missing or invalid 'body' field in payload");
        Error::from("Invalid request format")
    })?;

    let metrics: SleepMetrics = serde_json::from_str(body_str).map_err(|e| {
        error!("Failed to parse input metrics: {:?}", e);
        e
    })?;

    let baseline = BaselineAverages::from_redis(&redis_client).await?;
    let response = SleepMetricsResponse {
        rem_sleep: get_sleep_category(evaluate_metric(metrics.rem_sleep, baseline.rem_sleep)),
        deep_sleep: get_sleep_category(evaluate_metric(metrics.deep_sleep, baseline.deep_sleep)),
        total_sleep: get_sleep_category(evaluate_metric(metrics.total_sleep, baseline.total_sleep)),
        restfulness: get_sleep_category(evaluate_metric(metrics.restfulness, baseline.restfulness)),
        efficiency: get_sleep_category(evaluate_metric(metrics.efficiency, baseline.efficiency)),
        timing: get_sleep_category(evaluate_metric(metrics.timing, baseline.timing)),
        latency: get_sleep_category(evaluate_metric(metrics.latency, baseline.latency)),
    };

    BaselineAverages::update_averages(&redis_client, &metrics).await?;
    Ok(response)
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_target(false)
        .without_time()
        .init();

    info!("Lambda function starting up");
    lambda_runtime::run(service_fn(handler)).await
}

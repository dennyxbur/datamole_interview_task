from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import StreamingResponse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lag, unix_timestamp, expr, count, desc
from pyspark.sql.window import Window
from delta import configure_spark_with_delta_pip
import datetime as dt
from pydantic import BaseModel
from typing import Dict, List
import matplotlib.pyplot as plt
import io

app = FastAPI()

# Initialize Spark
builder = (
    SparkSession.builder.appName("GitHubMetricsAPI")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)

# must ensure spark - doesn't work without this configuration
spark = configure_spark_with_delta_pip(builder).getOrCreate()

# clean data path - taken from cleaner
CLEANED_PATH = "/delta/clean_github_events"

# Base model classes
class AverageIntervalResponse(BaseModel):
    repository: str
    average_interval_seconds: float
    average_interval_minutes: float
    event_count: int


class EventCounts(BaseModel):
    event_counts: Dict[str, int]


class RepoList(BaseModel):
    repos: List[str]


class TimestampList(BaseModel):
    timestamps: List[str]


@app.get("/average_pull_request_interval", response_model=AverageIntervalResponse)
def avg_pr_interval(repo: str):
    try:
        # Load cleaned data
        df = spark.read.format("delta").load(CLEANED_PATH)

        # Filter PullRequestEvents for given repo
        df_filtered = df.filter((col("type") == "PullRequestEvent") & (col("repo.name") == repo))

        if df_filtered.count() < 2:
            raise HTTPException(status_code=400, detail="Not enough pull request events to calculate average.")

        # Sort and calculate time differences
        window = Window.orderBy("created_at_ts")
        df_with_lag = df_filtered.withColumn("prev_time", lag("created_at_ts").over(window))
        df_with_diff = df_with_lag.withColumn(
            "time_diff_sec",
            unix_timestamp("created_at_ts") - unix_timestamp("prev_time")
        ).na.drop(subset=["time_diff_sec"])

        avg_diff = df_with_diff.agg({"time_diff_sec": "avg"}).collect()[0][0]

        return {
            "repository": repo,
            "average_interval_seconds": avg_diff,
            "average_interval_minutes": round(avg_diff / 60, 2),
            "event_count": df_filtered.count()
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/event_counts")
def event_counts(offset: int):
    # Github API uses UTC by default
    now = dt.datetime.utcnow()
    start_time = now - dt.timedelta(minutes=offset)

    df = spark.read.format("delta").load(CLEANED_PATH)
    df = df.filter(col("type").isin(["WatchEvent", "PullRequestEvent", "IssuesEvent"]))
    # Use tiemstamp column
    df = df.filter(col("created_at_ts") >= start_time)

    result_df = df.groupBy("type").agg(count("*").alias("total"))
    result = {row["type"]: row["total"] for row in result_df.collect()}

    return {"event_counts": result}


@app.get("/repos-with-pullrequests", response_model=RepoList)
def repos_with_pullrequests(limit: int = Query(10, description="Max number of repos to return")):
    df = spark.read.format("delta").load(CLEANED_PATH)
    df_pr = df.filter(col("type") == "PullRequestEvent")
    grouped = df_pr.groupBy(col("repo")["name"].alias("repo_name")).agg(count("*").alias("pr_count"))
    filtered = grouped.filter(col("pr_count") >= 2).orderBy(desc("pr_count")).limit(limit)

    repos = [row["repo_name"] for row in filtered.collect()]
    return {"repos": repos}


@app.get("/recent-timestamps", response_model=TimestampList)
def recent_timestamps(limit: int = Query(10, description="Max number of timestamps to return")):
    df = spark.read.format("delta").load(CLEANED_PATH)
    ts_col = col("created_at_ts")
    sorted_df = df.select(ts_col).orderBy(desc(ts_col)).limit(limit)
    timestamps = [row["created_at_ts"] for row in sorted_df.collect()]
    # Convert timestamps to ISO string (if they are TimestampType)
    timestamps = [ts.isoformat() if hasattr(ts, 'isoformat') else ts for ts in timestamps]

    return {"timestamps": timestamps}


@app.get("/metrics/pull_request_chart")
def pull_request_chart(limit: int = 10):
    # Filter PullRequestEvents
    df = spark.read.format("delta").load(CLEANED_PATH)
    df_filtered = df.filter(df["type"] == "PullRequestEvent")

    # Count per repo
    repo_counts = df_filtered.groupBy(df["repo"]["name"].alias("repo_name")) \
        .count().orderBy("count", ascending=False).limit(limit)

    # Collect to driver
    data = repo_counts.toPandas()

    # Plot by matplotlib
    plt.figure(figsize=(10, 6))
    plt.barh(data["repo_name"], data["count"], color="skyblue")
    plt.xlabel("Number of Pull Requests")
    plt.title("Top Repositories by Pull Request Events")
    plt.gca().invert_yaxis()

    buf = io.BytesIO()
    plt.tight_layout()
    plt.savefig(buf, format="png")
    buf.seek(0)
    plt.close()

    return StreamingResponse(buf, media_type="image/png")

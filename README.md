# GitHub Event Streaming and API

A pipeline that ingests GitHub events, cleans them, stores in Delta Lake
and serves metrics through a FastAPI REST API.

## Components

- **Kafka + Zookeeper**: Stream data base
- **Spark**: Running Spark in a container  
- **producer**: Streams data from Github API
- **consumer**: Ingests raw data in an original format
- **cleaner**: Deduplicates and cleans data using Spark.
  A new layer of data is created upon raw data. Because that task is not large
  there is only 1 layer with cleaning and filtering directly from the raw data.
- **api**: Serves REST endpoints
- **Dockerized** with support for `docker-compose`

## Run the stack
Create .env file with GITHUB_TOKEN to enable more request then given by default.
(.env is listed in .gitignore file).
For sure create folder delta-data and setup access to this folder.
I used this for sure (it's POC :-) ):
```bash
chmod -R 777 delta-data
```

Starts all components at once.
```bash
docker compose up --build
```

To start only API without streaming data use following command
```bash
docker compose up api
```

## API endpoints
Given metrics:
* Calculate the average time between pull requests for a given repository
    * additional endpoint with a graph can be used to find a repo with 2 or more PullReqeustEvents
* Return the total number of events grouped by the event type for a given    offset
    * additional endpoint with last 10 timestamps can be used to find a reasonable offset.
    Timestamps are in UTC and are filtered to obtaine only these events WatchEvent, PullRequestEvent and IssuesEvent.
      So 10 minutes offset didn't work for me when testing.

Curl commands to request GET methods of API endpoints
```bash
curl -X GET "http://localhost:8000/average-interval?repo=<REPO_NAME>" -H "accept: application/json"

curl -X GET "http://localhost:8000/event-counts?offset_minutes=10" -H "accept: application/json"

curl -X GET "http://localhost:8000/popular-repos?limit=10" -H "accept: application/json"

curl -X GET "http://localhost:8000/recent-events?limit=10" -H "accept: application/json"

curl -X GET "http://localhost:8000/chart/pr-events?limit=10" -H "accept: application/json"
```

UI web
```bash
http://0.0.0.0:8000/docs
```
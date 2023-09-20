<!-- This is generated. Edit it from src/main/documentation -->

# Elasticsearch Client Java Sample project

https://discuss.elastic.co has many questions about the Elasticsearch Java Client.

To address those questions, I often attempt to reproduce the issues.

This repository houses many examples derived from those discussions.
I believe it could be beneficial for many, so I've made the code available here.

You're welcome to contribute your own examples if you'd like.

This repository is tested against Elasticsearch 8.10.1.

## Start a local cluster

You can start Elasticsearch locally using `docker-compose`:

```sh
docker-compose up
```

## Using embedded TestContainers Elasticsearch module

If a local cluster is not running at `https://localhost:9200`, we will start automatically a Docker image using
the [Elasticsearch module for TestContainers](https://www.testcontainers.org/modules/elasticsearch/).

It requires to have Docker running.

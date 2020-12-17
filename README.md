# Elasticsearch Client Java Sample project

I'm getting a lot of questions on https://discuss.elastic.co where
people are asking about the High Level Rest Client.

The only way to answer is by trying to reproduce the problems.

This repository contains then some examples that are coming from those
discussions. It might be useful for anyone so I'm sharing the code here.

Feel free to add your own examples if you wish.

## Start a local cluster

You can start Elasticsearch locally using `docker-compose`:

```sh
docker-compose up
```

## Using embedded TestContainers Elasticsearch module

If a local cluster is not running at `http://localhost:9200`, we will start automatically a Docker image using
the [Elasticsearch module for TestContainers](https://www.testcontainers.org/modules/elasticsearch/).

It requires to have Docker running.

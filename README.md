<!-- This is generated. Edit it from src/main/documentation -->

# Elasticsearch Client Java Sample project

https://discuss.elastic.co has many questions about the Elasticsearch Java Client.

To address those questions, I often attempt to reproduce the issues.

This repository houses many examples derived from those discussions.
I believe it could be beneficial for many, so I've made the code available here.

You're welcome to contribute your own examples if you'd like.

This repository is tested against Elasticsearch 9.0.3.

We automatically start a Docker image using the [Elasticsearch module for TestContainers](https://www.testcontainers.org/modules/elasticsearch/).

If you want to have very fast startup times, you can use the [TestContainers `reuse` feature](https://java.testcontainers.org/features/reuse/).

To do this, you need to add the following to your `~/.testcontainers.properties` file:

```properties
testcontainers.reuse.enable=true
```

It requires to have Docker running.

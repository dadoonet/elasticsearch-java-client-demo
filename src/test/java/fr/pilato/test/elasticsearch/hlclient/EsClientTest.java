/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package fr.pilato.test.elasticsearch.hlclient;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.MainResponse;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.PutMappingRequest;
import org.elasticsearch.client.transform.GetTransformRequest;
import org.elasticsearch.client.transform.GetTransformResponse;
import org.elasticsearch.client.transform.PutTransformRequest;
import org.elasticsearch.client.transform.transforms.DestConfig;
import org.elasticsearch.client.transform.transforms.SourceConfig;
import org.elasticsearch.client.transform.transforms.TransformConfig;
import org.elasticsearch.client.transform.transforms.pivot.GroupConfig;
import org.elasticsearch.client.transform.transforms.pivot.PivotConfig;
import org.elasticsearch.client.transform.transforms.pivot.TermsGroupSource;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;

import static org.junit.Assume.assumeNotNull;
import static org.junit.jupiter.api.Assertions.fail;

class EsClientTest {

    private static final Logger logger = LogManager.getLogger();
    private static ElasticsearchContainer container;
    private static RestHighLevelClient client = null;
    private static final String PASSWORD = "changeme";

    @BeforeAll
    static void startOptionallyTestContainers() {
        client = getClient("http://localhost:9200");
        if (client == null) {
            logger.info("Starting testcontainers.");
            // Start the container. This step might take some time...
            container = new ElasticsearchContainer(
                    DockerImageName.parse("docker.elastic.co/elasticsearch/elasticsearch")
                            .withTag("7.15.2"))
                    .withPassword(PASSWORD);
            container.start();
            client = getClient(container.getHttpHostAddress());
            assumeNotNull(client);
        }
    }

    @AfterAll
    static void stopOptionallyTestContainers() {
        if (container != null && container.isRunning()) {
            container.close();
        }
        container = null;
    }

    @AfterAll
    static void elasticsearchClient() throws IOException {
        if (client != null) {
            client.close();
        }
    }

    static private RestHighLevelClient getClient(String elasticsearchServiceAddress) {
        try {
            final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(AuthScope.ANY,
                    new UsernamePasswordCredentials("elastic", PASSWORD));

            RestHighLevelClient client = new RestHighLevelClient(
                    RestClient.builder(HttpHost.create(elasticsearchServiceAddress))
                            .setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider))
                    );
            MainResponse info = client.info(RequestOptions.DEFAULT);
            logger.info("Connected to a cluster running version {} at {}.", info.getVersion().getNumber(), elasticsearchServiceAddress);
            return client;
        } catch (Exception e) {
            logger.info("No cluster is running yet at {}.", elasticsearchServiceAddress);
            return null;
        }
    }

    @Test
    void getWithFilter() throws IOException {
        try {
            client.indices().delete(new DeleteIndexRequest("test"), RequestOptions.DEFAULT);
        } catch (ElasticsearchStatusException ignored) { }
        client.index(new IndexRequest("test").id("1").source("{\"foo\":\"bar\", \"application_id\": 6}", XContentType.JSON), RequestOptions.DEFAULT);
        GetResponse getResponse = client.get(new GetRequest("test", "1").fetchSourceContext(
                new FetchSourceContext(true, new String[]{"application_id"}, null)
        ), RequestOptions.DEFAULT);
        logger.info("doc = {}", getResponse);
    }

    @Test
    void nodeStatsWithLowLevelClient() throws IOException {
        Response response = client.getLowLevelClient().performRequest(new Request("GET", "/_nodes/stats/thread_pool"));
        String s = EntityUtils.toString(response.getEntity());
        logger.info("thread_pool = {}", s);
    }

    @Test
    void exist() throws IOException {
        try {
            client.indices().delete(new DeleteIndexRequest("test"), RequestOptions.DEFAULT);
        } catch (ElasticsearchStatusException ignored) { }
        client.index(new IndexRequest("test").id("1").source("{\"foo\":\"bar\"}", XContentType.JSON), RequestOptions.DEFAULT);
        boolean exists1 = client.exists(new GetRequest("test", "1"), RequestOptions.DEFAULT);
        boolean exists2 = client.exists(new GetRequest("test", "2"), RequestOptions.DEFAULT);
        logger.info("exists1 = {}", exists1);
        logger.info("exists2 = {}", exists2);
    }

    @Test
    void createIndex() throws IOException {
        String settings = "{\n" +
                "  \"mappings\": {\n" +
                "      \"properties\": {\n" +
                "        \"content\": {\n" +
                "          \"type\": \"text\"\n" +
                "        }\n" +
                "      }\n" +
                "  }\n" +
                "}\n";

        try {
            client.indices().delete(new DeleteIndexRequest("test"), RequestOptions.DEFAULT);
        } catch (ElasticsearchStatusException ignored) { }
        CreateIndexRequest createIndexRequest = new CreateIndexRequest("test");
        createIndexRequest.source(settings, XContentType.JSON);
        client.indices().create(createIndexRequest, RequestOptions.DEFAULT);
    }

    @Test
    void callInfo() throws IOException {
        MainResponse info = client.info(RequestOptions.DEFAULT);
        String version = info.getVersion().getNumber();
        logger.info("version = {}", version);
    }

    @Test
    void createMapping() throws IOException {
        try {
            client.indices().delete(new DeleteIndexRequest("test"), RequestOptions.DEFAULT);
        } catch (ElasticsearchStatusException ignored) { }
        client.indices().create(new CreateIndexRequest("test"), RequestOptions.DEFAULT);
        PutMappingRequest request =
                new PutMappingRequest("test").source("{\n" +
                        "    \"properties\":{\n" +
                        "        \"foo\":{\"type\":\"text\"}\n" +
                        "    }\n" +
                        "}", XContentType.JSON);
        client.indices().putMapping(request, RequestOptions.DEFAULT);
    }

    @Test
    void createData() throws IOException {
        try {
            client.indices().delete(new DeleteIndexRequest("test"), RequestOptions.DEFAULT);
        } catch (ElasticsearchStatusException ignored) { }
        client.index(new IndexRequest("test").id("1").source("{\"foo\":\"bar\"}", XContentType.JSON), RequestOptions.DEFAULT);
        client.indices().refresh(new RefreshRequest("test"), RequestOptions.DEFAULT);
        SearchResponse response = client.search(new SearchRequest("test"), RequestOptions.DEFAULT);
        logger.info("response.getHits().totalHits = {}", response.getHits().getTotalHits().value);
    }

    @Test
    void searchData() throws IOException {
        try {
            client.indices().delete(new DeleteIndexRequest("test"), RequestOptions.DEFAULT);
        } catch (ElasticsearchStatusException ignored) { }
        client.index(new IndexRequest("test").id("1").source("{\"foo\":\"bar\"}", XContentType.JSON), RequestOptions.DEFAULT);
        client.indices().refresh(new RefreshRequest("test"), RequestOptions.DEFAULT);
        SearchResponse response = client.search(new SearchRequest("test").source(
                new SearchSourceBuilder().query(
                        QueryBuilders.matchQuery("foo", "bar")
                )
        ), RequestOptions.DEFAULT);
        logger.info("response.getHits().totalHits = {}", response.getHits().getTotalHits().value);
        response = client.search(new SearchRequest("test").source(
                new SearchSourceBuilder().query(
                        QueryBuilders.termQuery("foo", "bar")
                )
        ), RequestOptions.DEFAULT);
        logger.info("response.getHits().totalHits = {}", response.getHits().getTotalHits().value);
        response = client.search(new SearchRequest("test").source(
                new SearchSourceBuilder().query(
                        QueryBuilders.wrapperQuery("{\"match_all\":{}}")
                )
        ), RequestOptions.DEFAULT);
        logger.info("response.getHits().totalHits = {}", response.getHits().getTotalHits().value);
        response = client.search(new SearchRequest("test").source(
                new SearchSourceBuilder().query(QueryBuilders.matchAllQuery())
                .trackScores(true)
        ), RequestOptions.DEFAULT);
        logger.info("response.getHits().totalHits = {}", response.getHits().getTotalHits().value);
    }

    @Test
    void transformSqlQuery() throws IOException {
        try {
            client.indices().delete(new DeleteIndexRequest("test"), RequestOptions.DEFAULT);
        } catch (ElasticsearchStatusException ignored) { }
        client.index(new IndexRequest("test").id("1").source("{\"foo\":\"bar\"}", XContentType.JSON), RequestOptions.DEFAULT);
        client.indices().refresh(new RefreshRequest("test"), RequestOptions.DEFAULT);

        RestClient llClient = client.getLowLevelClient();
        Request request = new Request("POST",  "/_sql/translate");
        request.setJsonEntity("{\"query\":\"SELECT * FROM test WHERE foo='bar' limit 10\"}");
        Response llResponse = llClient.performRequest(request);

        ObjectMapper mapper = new ObjectMapper();
        JsonNode tree = mapper.readTree(llResponse.getEntity().getContent());

        int size = tree.get("size").asInt(10);
        String query = tree.get("query").toString();

        SearchResponse response = client.search(new SearchRequest("test").source(
                new SearchSourceBuilder().query(
                        QueryBuilders.wrapperQuery(query)
                ).size(size)
        ), RequestOptions.DEFAULT);
        logger.info("response.getHits().totalHits = {}", response.getHits().getTotalHits().value);
    }

    @Test
    void transformApi() throws IOException {
        try {
            client.indices().delete(new DeleteIndexRequest("transform-source"), RequestOptions.DEFAULT);
        } catch (ElasticsearchStatusException ignored) { }
        client.index(new IndexRequest("transform-source").id("1").source("{\"foo\":\"bar\"}", XContentType.JSON), RequestOptions.DEFAULT);
        client.indices().refresh(new RefreshRequest("transform-source"), RequestOptions.DEFAULT);

        String id = "test-get";

        GroupConfig groupConfig = GroupConfig.builder().groupBy("reviewer",
                TermsGroupSource.builder().setField("user_id").build()).build();
        AggregatorFactories.Builder aggBuilder = new AggregatorFactories.Builder();
        aggBuilder.addAggregator(AggregationBuilders.avg("avg_rating").field("stars"));
        PivotConfig pivotConfig = PivotConfig.builder().setGroups(groupConfig).setAggregations(aggBuilder).build();

        DestConfig destConfig = DestConfig.builder().setIndex("pivot-dest").build();

        TransformConfig transform = TransformConfig.builder()
                .setId(id)
                .setSource(SourceConfig.builder().setIndex("transform-source").setQuery(new MatchAllQueryBuilder()).build())
                .setDest(destConfig)
                .setPivotConfig(pivotConfig)
                .setDescription("this is a test transform")
                .build();

        client.transform().putTransform(new PutTransformRequest(transform), RequestOptions.DEFAULT);

        // Todo fix it with a coming version of elasticsearch
        // Bug reported at https://github.com/elastic/elasticsearch/issues/64602
        try {
            GetTransformResponse response = client.transform().getTransform(new GetTransformRequest(id), RequestOptions.DEFAULT);
            logger.info("response.getCount() = {}", response.getCount());
            fail("Failing this test indicates that https://github.com/elastic/elasticsearch/issues/64602 has been fixed. The code should be reviewed");
        } catch (IOException ignored) { }
    }

    @Test
    void highlight() throws IOException {
        try {
            client.indices().delete(new DeleteIndexRequest("highlight"), RequestOptions.DEFAULT);
        } catch (ElasticsearchStatusException ignored) {
        }
        client.index(new IndexRequest("highlight").source("{\"foo\":\"bar baz\"}", XContentType.JSON), RequestOptions.DEFAULT);
        client.indices().refresh(new RefreshRequest("highlight"), RequestOptions.DEFAULT);
        SearchResponse response = client.search(new SearchRequest("highlight").source(
                new SearchSourceBuilder()
                        .query(QueryBuilders.matchQuery("foo", "bar"))
                        .highlighter(new HighlightBuilder().field("foo").maxAnalyzedOffset(10)
                        )
        ), RequestOptions.DEFAULT);
        HighlightField highlightField = response.getHits().getAt(0).getHighlightFields().get("foo");
        logger.info("Highlights: {}", (Object) highlightField.fragments());
    }

    @Test
    void termsAgg() throws IOException {
        try {
            client.indices().delete(new DeleteIndexRequest("termsagg"), RequestOptions.DEFAULT);
        } catch (ElasticsearchStatusException ignored) { }
        client.index(new IndexRequest("termsagg").id("1").source("{\"foo\":\"bar\"}", XContentType.JSON), RequestOptions.DEFAULT);
        client.index(new IndexRequest("termsagg").id("2").source("{\"foo\":\"bar\"}", XContentType.JSON), RequestOptions.DEFAULT);
        client.indices().refresh(new RefreshRequest("termsagg"), RequestOptions.DEFAULT);
        SearchResponse response = client.search(new SearchRequest("termsagg").source(new SearchSourceBuilder()
                .size(0)
                .aggregation(AggregationBuilders.terms("top10foo").field("foo.keyword").size(10))
        ), RequestOptions.DEFAULT);
        Terms top10foo = response.getAggregations().get("top10foo");
        for (Terms.Bucket bucket : top10foo.getBuckets()) {
            logger.info("top10foo bucket = {}, count = {}", bucket.getKeyAsString(), bucket.getDocCount());
        }
    }

    @Test
    void bulkProcessor() throws IOException {
        int size = 1000;
        try {
            client.indices().delete(new DeleteIndexRequest("bulk"), RequestOptions.DEFAULT);
        } catch (ElasticsearchStatusException | IOException ignored) { }
        BulkProcessor bulkProcessor = BulkProcessor.builder(
                (request, bulkListener) -> client.bulkAsync(request, RequestOptions.DEFAULT, bulkListener),
                new BulkProcessor.Listener() {
                    @Override
                    public void beforeBulk(long executionId, BulkRequest request) {
                        logger.debug("going to execute bulk of {} requests", request.numberOfActions());
                    }

                    @Override
                    public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                        logger.debug("bulk executed {} failures", response.hasFailures() ? "with" : "without");
                    }

                    @Override
                    public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                        logger.warn("error while executing bulk", failure);
                    }
                })
                .setBulkActions(10)
                .build();

        for (int i = 0; i < size; i++) {
            bulkProcessor.add(new IndexRequest("bulk").source("{\"foo\":\"bar\"}", XContentType.JSON));
        }

        // Make sure to close (and flush) the bulk processor before exiting
        bulkProcessor.close();

        client.indices().refresh(new RefreshRequest("bulk"), RequestOptions.DEFAULT);
        SearchResponse response = client.search(new SearchRequest("bulk").source(new SearchSourceBuilder().size(0)), RequestOptions.DEFAULT);
        logger.info("Indexed {} documents. Found {} documents.", size, response.getHits().getTotalHits().value);
    }

    @Test
    void rangeQuery() throws IOException {
        try {
            client.indices().delete(new DeleteIndexRequest("rangequery"), RequestOptions.DEFAULT);
        } catch (ElasticsearchStatusException ignored) { }
        client.index(new IndexRequest("rangequery").id("1").source("{\"foo\":1}", XContentType.JSON), RequestOptions.DEFAULT);
        client.index(new IndexRequest("rangequery").id("2").source("{\"foo\":2}", XContentType.JSON), RequestOptions.DEFAULT);
        client.indices().refresh(new RefreshRequest("rangequery"), RequestOptions.DEFAULT);
        SearchResponse response = client.search(new SearchRequest("rangequery").source(new SearchSourceBuilder()
                .query(QueryBuilders.rangeQuery("foo").from(0).to(1))
        ), RequestOptions.DEFAULT);
        for (SearchHit hit : response.getHits()) {
            logger.info("hit _id = {}, _source = {}", hit.getId(), hit.getSourceAsString());
        }
    }

}

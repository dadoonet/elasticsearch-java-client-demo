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

import co.elastic.clients.elasticsearch.ElasticsearchAsyncClient;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._helpers.bulk.BulkIngester;
import co.elastic.clients.elasticsearch._helpers.bulk.BulkListener;
import co.elastic.clients.elasticsearch._types.DistanceUnit;
import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch._types.GeoLocation;
import co.elastic.clients.elasticsearch._types.SortOrder;
import co.elastic.clients.elasticsearch._types.aggregations.StringTermsBucket;
import co.elastic.clients.elasticsearch.cat.IndicesResponse;
import co.elastic.clients.elasticsearch.cat.ShardsResponse;
import co.elastic.clients.elasticsearch.cat.ThreadPoolResponse;
import co.elastic.clients.elasticsearch.core.*;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.elasticsearch.ingest.SimulateResponse;
import co.elastic.clients.elasticsearch.sql.TranslateResponse;
import co.elastic.clients.elasticsearch.transform.GetTransformResponse;
import co.elastic.clients.json.JsonData;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.TransportException;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import co.elastic.clients.util.BinaryData;
import co.elastic.clients.util.ContentType;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.RestClient;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static fr.pilato.test.elasticsearch.hlclient.SSLUtils.createContextFromCaCert;
import static fr.pilato.test.elasticsearch.hlclient.SSLUtils.createTrustAllCertsContext;
import static org.junit.Assume.assumeNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class EsClientIT {

    private static final Logger logger = LogManager.getLogger();
    private static ElasticsearchContainer container;
    private static RestClient restClient = null;
    private static ElasticsearchClient client = null;
    private static ElasticsearchAsyncClient asyncClient = null;
    private static final String PASSWORD = "changeme";

    @BeforeAll
    static void startOptionallyTestContainers() throws IOException {
        client = getClient("https://localhost:9200", null);
        if (client == null) {
            Properties props = new Properties();
            props.load(EsClientIT.class.getResourceAsStream("/version.properties"));
            String version = props.getProperty("elasticsearch.version");
            logger.info("Starting testcontainers with Elasticsearch {}.", version);
            // Start the container. This step might take some time...
            container = new ElasticsearchContainer(
                    DockerImageName.parse("docker.elastic.co/elasticsearch/elasticsearch")
                            .withTag(version))
                    .withPassword(PASSWORD);
            container.start();
            byte[] certAsBytes = container.copyFileFromContainer(
                    "/usr/share/elasticsearch/config/certs/http_ca.crt",
                    InputStream::readAllBytes);
            client = getClient("https://" + container.getHttpHostAddress(), certAsBytes);
            assumeNotNull(client);
            asyncClient = getAsyncClient("https://" + container.getHttpHostAddress(), certAsBytes);
            assumeNotNull(asyncClient);
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
        if (restClient != null) {
            restClient.close();
        }
    }

    static private ElasticsearchClient getClient(String elasticsearchServiceAddress, byte[] certificate) {
        logger.debug("Trying to connect to {} {}.", elasticsearchServiceAddress,
                certificate == null ? "with no ssl checks": "using the provided SSL certificate");
        try {
            final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(AuthScope.ANY,
                    new UsernamePasswordCredentials("elastic", PASSWORD));

            // Create the low-level client
            restClient = RestClient.builder(HttpHost.create(elasticsearchServiceAddress))
                    .setHttpClientConfigCallback(hcb -> hcb
                            .setDefaultCredentialsProvider(credentialsProvider)
                            .setSSLContext(certificate != null ?
                                    createContextFromCaCert(certificate) : createTrustAllCertsContext())
                    ).build();

            // Create the transport with a Jackson mapper
            ElasticsearchTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());

            // And create the API client
            ElasticsearchClient client = new ElasticsearchClient(transport);

            InfoResponse info = client.info();
            logger.info("Connected to a cluster running version {} at {}.", info.version().number(), elasticsearchServiceAddress);
            return client;
        } catch (Exception e) {
            logger.info("No cluster is running yet at {}.", elasticsearchServiceAddress);
            return null;
        }
    }

    static private ElasticsearchAsyncClient getAsyncClient(String elasticsearchServiceAddress, byte[] certificate) {
        logger.debug("Trying to connect to {} {}.", elasticsearchServiceAddress,
                certificate == null ? "with no ssl checks": "using the provided SSL certificate");
        try {
            final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(AuthScope.ANY,
                    new UsernamePasswordCredentials("elastic", PASSWORD));

            // Create the low-level client
            restClient = RestClient.builder(HttpHost.create(elasticsearchServiceAddress))
                    .setHttpClientConfigCallback(hcb -> hcb
                            .setDefaultCredentialsProvider(credentialsProvider)
                            .setSSLContext(certificate != null ?
                                    createContextFromCaCert(certificate) : createTrustAllCertsContext())
                    ).build();

            // Create the transport with a Jackson mapper
            ElasticsearchTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());

            // And create the API client
            ElasticsearchAsyncClient client = new ElasticsearchAsyncClient(transport);

            InfoResponse info = client.info().get();
            logger.info("Connected to a cluster running version {} at {}.", info.version().number(), elasticsearchServiceAddress);
            return client;
        } catch (Exception e) {
            logger.info("No cluster is running yet at {}.", elasticsearchServiceAddress);
            return null;
        }
    }

    @Test
    void getWithFilter() throws IOException {
        try {
            client.indices().delete(dir -> dir.index("get-with-filter"));
        } catch (ElasticsearchException ignored) { }
        Reader input = new StringReader("{\"foo\":\"bar\", \"application_id\": 6}");
        client.index(ir -> ir.index("get-with-filter").id("1").withJson(input));
        GetResponse<ObjectNode> getResponse = client.get(gr -> gr.index("get-with-filter").id("1").sourceIncludes("application_id"), ObjectNode.class);
        logger.info("doc = {}", getResponse.source());
    }

    @Test
    void getAsMap() throws IOException {
        try {
            client.indices().delete(dir -> dir.index("get-as-map"));
        } catch (ElasticsearchException ignored) { }
        Reader input = new StringReader("{\"foo\":\"bar\", \"application_id\": 6}");
        client.index(ir -> ir.index("get-as-map").id("1").withJson(input));
        GetResponse<ObjectNode> getResponse = client.get(gr -> gr.index("get-with-filter").id("1"), ObjectNode.class);
        ObjectMapper mapper = new ObjectMapper();
        Map<String, Object> result = mapper.convertValue(getResponse.source(), new TypeReference<>() {});
        logger.info("doc = {}", result);
    }

    @Test
    void nodeStatsWithLowLevelClient() throws IOException {
        ThreadPoolResponse threadPoolResponse = client.cat().threadPool();
        logger.info("thread_pool = {}", threadPoolResponse);
    }

    @Test
    void exist() throws IOException {
        try {
            client.indices().delete(dir -> dir.index("exist"));
        } catch (ElasticsearchException ignored) { }
        client.index(ir -> ir.index("exist").id("1").withJson(new StringReader("{\"foo\":\"bar\"}")));
        boolean exists1 = client.exists(gr -> gr.index("exist").id("1")).value();
        boolean exists2 = client.exists(gr -> gr.index("exist").id("2")).value();
        logger.info("exists1 = {}", exists1);
        logger.info("exists2 = {}", exists2);
    }

    @Test
    void createIndex() throws IOException {
        try {
            client.indices().delete(dir -> dir.index("create-index"));
        } catch (ElasticsearchException ignored) { }
        client.indices().create(cir -> cir.index("create-index").mappings(m -> m.properties("content", p -> p.text(tp -> tp))));
    }

    @Test
    void callInfo() throws IOException {
        InfoResponse info = client.info();
        String version = info.version().number();
        logger.info("version = {}", version);
    }

    @Test
    void createMapping() throws IOException {
        try {
            client.indices().delete(dir -> dir.index("create-mapping"));
        } catch (ElasticsearchException ignored) { }
        client.indices().create(cir -> cir.index("create-mapping"));
        client.indices().putMapping(pmr -> pmr.index("create-mapping").properties("foo", p -> p.text(tp -> tp)));
    }

    @Test
    void createData() throws IOException {
        try {
            client.indices().delete(dir -> dir.index("test"));
        } catch (ElasticsearchException ignored) { }
        client.index(ir -> ir.index("test").id("1").withJson(new StringReader("{\"foo\":\"bar\"}")));
        client.indices().refresh(rr -> rr.index("test"));
        SearchResponse<Void> response = client.search(sr -> sr.index("test"), Void.class);
        logger.info("response.hits.total.value = {}", response.hits().total().value());
    }

    @Test
    void searchData() throws IOException {
        try {
            client.indices().delete(dir -> dir.index("search-data"));
        } catch (ElasticsearchException ignored) { }
        client.index(ir -> ir.index("search-data").id("1").withJson(new StringReader("{\"foo\":\"bar\"}")));
        client.indices().refresh(rr -> rr.index("search-data"));
        SearchResponse<Void> response = client.search(sr -> sr
                        .index("search-data")
                        .query(q -> q.match(mq -> mq.field("foo").query("bar"))),
                Void.class);
        logger.info("response.hits.total.value = {}", response.hits().total().value());
        response = client.search(sr -> sr
                        .index("search-data")
                        .query(q -> q.term(tq -> tq.field("foo").value("bar"))),
                Void.class);
        logger.info("response.hits.total.value = {}", response.hits().total().value());
        String matchAllQuery = Base64.getEncoder().encodeToString("{\"match_all\":{}}".getBytes(StandardCharsets.UTF_8));
        response = client.search(sr -> sr
                        .index("search-data")
                        .query(q -> q.wrapper(wq -> wq.query(matchAllQuery))),
                Void.class);
        logger.info("response.hits.total.value = {}", response.hits().total().value());
        response = client.search(sr -> sr
                        .index("search-data")
                        .query(q -> q.matchAll(maq -> maq))
                        .trackScores(true),
                Void.class);
        logger.info("response.hits.total.value = {}", response.hits().total().value());
    }

    @Test
    void translateSqlQuery() throws IOException {
        try {
            client.indices().delete(dir -> dir.index("test"));
        } catch (ElasticsearchException ignored) { }
        client.index(ir -> ir.index("test").id("1").withJson(new StringReader("{\"foo\":\"bar\"}")));
        client.indices().refresh(rr -> rr.index("test"));

        TranslateResponse translateResponse = client.sql().translate(tr -> tr
                .query("SELECT * FROM test WHERE foo='bar' limit 10"));
        SearchResponse<Void> response = client.search(sr -> sr
                        .index("test")
                        .query(translateResponse.query())
                        .size(translateResponse.size().intValue()),
                Void.class);
        logger.info("response.hits.total.value = {}", response.hits().total().value());
    }

    @Test
    void transformApi() throws IOException {
        String id = "test-get";

        try {
            client.indices().delete(dir -> dir.index("transform-source"));
        } catch (ElasticsearchException ignored) { }
        try {
            client.transform().deleteTransform(dtr -> dtr.transformId(id));
        } catch (ElasticsearchException ignored) { }
        client.index(ir -> ir.index("transform-source").id("1").withJson(new StringReader("{\"foo\":\"bar\"}")));
        client.indices().refresh(rr -> rr.index("transform-source"));
        client.transform().putTransform(ptr -> ptr
                .transformId(id)
                .source(s -> s.index("transform-source").query(q -> q.matchAll(maq -> maq)))
                .dest(d -> d.index("pivot-dest"))
                .pivot(p -> p
                        .groupBy("reviewer", pgb -> pgb.terms(ta -> ta.field("user_id")))
                        .aggregations("avg_rating", a -> a.avg(aa -> aa.field("stars")))
                )
                .description("this is a test transform")
        );

        GetTransformResponse response = client.transform().getTransform(gt -> gt.transformId(id));
        logger.info("response.getCount() = {}", response.count());
    }

    @Test
    void highlight() throws IOException {
        try {
            client.indices().delete(dir -> dir.index("highlight"));
        } catch (ElasticsearchException ignored) { }
        client.index(ir -> ir.index("highlight").withJson(new StringReader("{\"foo\":\"bar baz\"}")));
        client.indices().refresh(rr -> rr.index("highlight"));
        SearchResponse<Void> response = client.search(sr -> sr
                        .index("highlight")
                        .query(q -> q.match(mq -> mq.field("foo").query("bar")))
                        .highlight(h -> h.fields("foo", hf -> hf.maxAnalyzedOffset(10)))
                , Void.class);
        logger.info("response.hits.total.value = {}", response.hits().total().value());
        List<String> highlights = response.hits().hits().get(0).highlight().get("foo");
        logger.info("Highlights: {}", highlights);
    }

    @Test
    void termsAgg() throws IOException {
        try {
            client.indices().delete(dir -> dir.index("termsagg"));
        } catch (ElasticsearchException ignored) { }
        client.index(ir -> ir.index("termsagg").id("1").withJson(new StringReader("{\"foo\":\"bar\"}")));
        client.index(ir -> ir.index("termsagg").id("2").withJson(new StringReader("{\"foo\":\"bar\"}")));
        client.indices().refresh(rr -> rr.index("termsagg"));
        SearchResponse<Void> response = client.search(sr -> sr
                        .index("termsagg")
                        .aggregations("top10foo", a -> a.terms(ta -> ta.field("foo.keyword").size(10)))
                , Void.class);
        for (StringTermsBucket bucket : response.aggregations().get("top10foo").sterms().buckets().array()) {
            logger.info("top10foo bucket = {}, count = {}", bucket.key(), bucket.docCount());
        }
    }

    @Test
    void bulkIngester() throws IOException {
        int size = 1000;
        try {
            client.indices().delete(dir -> dir.index("bulk"));
        } catch (ElasticsearchException ignored) { }
        BulkIngester<Void> ingester = BulkIngester.of(b -> b
                .client(client)
                .listener(new BulkListener<Void>() {
                    @Override
                    public void beforeBulk(long executionId, BulkRequest request, List<Void> voids) {
                        logger.debug("going to execute bulk of {} requests", request.operations().size());
                    }

                    @Override
                    public void afterBulk(long executionId, BulkRequest request, List<Void> voids, BulkResponse response) {
                        logger.debug("bulk executed {} errors", response.errors() ? "with" : "without");
                    }

                    @Override
                    public void afterBulk(long executionId, BulkRequest request, List<Void> voids, Throwable failure) {
                        logger.warn("error while executing bulk", failure);
                    }
                })
                .maxOperations(10)
                .maxSize(1_000_000)
                .flushInterval(5, TimeUnit.SECONDS)
        );

        BinaryData data = BinaryData.of("{\"foo\":\"bar\"}".getBytes(StandardCharsets.UTF_8), ContentType.APPLICATION_JSON);
        for (int i = 0; i < size; i++) {
            ingester.add(bo -> bo.index(io -> io.index("bulk").document(data)));
        }

        // Make sure to close (and flush) the bulk ingester before exiting
        ingester.close();

        client.indices().refresh(rr -> rr.index("bulk"));
        SearchResponse<Void> response = client.search(sr -> sr.index("bulk"), Void.class);
        logger.info("Indexed {} documents. Found {} documents.", size, response.hits().total().value());
    }

    @Test
    void rangeQuery() throws IOException {
        try {
            client.indices().delete(dir -> dir.index("rangequery"));
        } catch (ElasticsearchException ignored) { }
        client.index(ir -> ir.index("rangequery").id("1").withJson(new StringReader("{\"foo\":1}")));
        client.index(ir -> ir.index("rangequery").id("2").withJson(new StringReader("{\"foo\":2}")));
        client.indices().refresh(rr -> rr.index("rangequery"));
        SearchResponse<ObjectNode> response = client.search(sr -> sr.index("rangequery")
                        .query(q -> q.range(rq -> rq.field("foo").from("0").to("1")))
                , ObjectNode.class);
        for (Hit<ObjectNode> hit : response.hits().hits()) {
            logger.info("hit _id = {}, _source = {}", hit.id(), hit.source());
        }
    }

    @Test
    void bulk() throws IOException {
        int size = 1_000;
        try {
            client.indices().delete(dir -> dir.index("bulk"));
        } catch (ElasticsearchException ignored) { }

        BinaryData data = BinaryData.of("{\"foo\":\"bar\"}".getBytes(StandardCharsets.UTF_8), ContentType.APPLICATION_JSON);
        BulkResponse response = client.bulk(br -> {
            br.index("bulk");
            for (int i = 0; i < size; i++) {
                br.operations(o -> o.index(ir -> ir.document(data)));
            }
            return br;
        });
        logger.info("bulk executed in {} ms {} errors", response.errors() ? "with" : "without", response.ingestTook());
        if (response.errors()) {
            response.items().stream().filter(p -> p.error() != null)
                    .forEach(item -> logger.error("Error {} for id {}", item.error().reason(), item.id()));
        }

        client.indices().refresh(rr -> rr.index("bulk"));
        SearchResponse<Void> searchResponse = client.search(sr -> sr.index("bulk"), Void.class);
        logger.info("Indexed {} documents. Found {} documents.", size, searchResponse.hits().total().value());
    }

    @Test
    void searchWithBeans() throws IOException {
        try {
            client.indices().delete(dir -> dir.index("with-beans"));
        } catch (ElasticsearchException ignored) { }
        Person p1 = new Person();
        p1.setId("1");
        p1.setName("Foo");
        Person p2 = new Person();
        p2.setId("2");
        p2.setName("Bar");
        client.index(ir -> ir.index("with-beans").id(p1.getId()).document(p1));
        client.index(ir -> ir.index("with-beans").id(p2.getId()).document(p2));
        client.indices().refresh(rr -> rr.index("with-beans"));
        SearchResponse<Person> response = client.search(sr -> sr.index("with-beans"), Person.class);
        for (Hit<Person> hit : response.hits().hits()) {
            logger.info("Person _id = {}, id = {}, name = {}", hit.id(), hit.source().getId(), hit.source().getName());
        }
    }

    @Test
    void reindex() throws IOException {
        // Check the error is thrown when the source index does not exist
        try {
            client.reindex(rr -> rr.source(s -> s.index("does-not-exists")).dest(d -> d.index("foo")));
        } catch (ElasticsearchException e) {
            logger.info("Got error {}", e.response());
        }

        // A regular reindex operation
        try {
            client.indices().delete(dir -> dir.index("reindex-source"));
        } catch (ElasticsearchException ignored) { }
        try {
            client.indices().delete(dir -> dir.index("reindex-dest"));
        } catch (ElasticsearchException ignored) { }

        client.index(ir -> ir.index("reindex-source").id("1").withJson(new StringReader("{\"foo\":1}")));
        client.indices().refresh(rr -> rr.index("reindex-source"));
        ReindexResponse reindexResponse = client.reindex(rr -> rr.source(s -> s.index("reindex-source")).dest(d -> d.index("reindex-dest")));
        logger.info("Reindexed {} documents.", reindexResponse.total());
    }

    @Test
    void geoPointSort() throws IOException {
        try {
            client.indices().delete(dir -> dir.index("geo-point-sort"));
        } catch (ElasticsearchException ignored) { }
        client.indices().create(cir -> cir.index("geo-point-sort"));
        client.indices().putMapping(pmr -> pmr.index("geo-point-sort").properties("location", p -> p.geoPoint(gp -> gp)));
        Person p1 = new Person();
        p1.setId("1");
        p1.setName("Foo");
        p1.setLocation(new GeoPoint(49.0404, 2.0174));
        Person p2 = new Person();
        p2.setId("2");
        p2.setName("Bar");
        p2.setLocation(new GeoPoint(38.7330, -109.8774));
        client.index(ir -> ir.index("geo-point-sort").id(p1.getId()).document(p1));
        client.index(ir -> ir.index("geo-point-sort").id(p2.getId()).document(p2));
        client.indices().refresh(rr -> rr.index("geo-point-sort"));
        SearchResponse<Person> response = client.search(sr -> sr.index("geo-point-sort")
                .sort(so -> so
                        .geoDistance(gd -> gd
                            .field("location")
                            .location(
                                    new GeoLocation.Builder()
                                            .latlon(ll -> ll.lat(49.0404).lon(2.0174))
                                            .build()
                            )
                        .order(SortOrder.Asc)
                        .unit(DistanceUnit.Kilometers)
                )
        ), Person.class);
        for (Hit<Person> hit : response.hits().hits()) {
            logger.info("Person _id = {}, id = {}, name = {}, distance = {}",
                    hit.id(), hit.source().getId(), hit.source().getName(), hit.sort().get(0).doubleValue());
        }
    }

    @Test
    void searchWithTimeout() throws IOException, ExecutionException, InterruptedException {
        String INDEX = "search-with-timeout";
        try {
            client.indices().delete(dir -> dir.index(INDEX));
        } catch (ElasticsearchException ignored) { }
        client.index(ir -> ir.index(INDEX).id("1").withJson(new StringReader("{\"foo\":\"bar\"}")));
        client.indices().refresh(rr -> rr.index(INDEX));
        asyncClient.search(sr -> sr
                        .index(INDEX)
                        .query(q -> q.match(mq -> mq.field("foo").query("bar"))),
                Void.class)
                .orTimeout(1, TimeUnit.NANOSECONDS)
                .exceptionally(e -> {
                    if (e instanceof TimeoutException) {
                        logger.info("Got a timeout as expected");
                    } else {
                        logger.error("Got an unexpected exception", e);
                    }
                    return null;
                });
        SearchResponse<Void> response = asyncClient.search(sr -> sr
                                .index(INDEX)
                                .query(q -> q.match(mq -> mq.field("foo").query("bar"))),
                        Void.class)
                .orTimeout(10, TimeUnit.SECONDS)
                .get();
        logger.info("response.hits.total.value = {}", response.hits().total().value());
    }

    @Test
    void catApi() throws IOException {
        IndicesResponse indices = client.cat().indices();
        logger.info("indices = {}", indices.valueBody());
        ShardsResponse shards = client.cat().shards();
        logger.info("shards = {}", shards.valueBody());
    }

    @Test
    void ingestPipelines() throws IOException {
        // Define some pipelines
        try {
            client.ingest().deletePipeline(pr -> pr.id("my-pipeline"));
        } catch (ElasticsearchException ignored) { }
        client.ingest().putPipeline(pr -> pr
                .id("my-pipeline")
                .processors(p -> p
                    .script(s -> s
                            .inline(is -> is
                                    .source("ctx.foo = 'bar'")
                                    .lang("painless")
                            )
                    )
                )
        );
        client.ingest().putPipeline(pr -> pr
                .id("my-pipeline")
                .processors(p -> p
                    .set(s -> s
                            .field("foo")
                            .value(JsonData.of("bar"))
                            .ignoreFailure(true)
                    )
                )
        );
        SimulateResponse response = client.ingest().simulate(sir -> sir
                .id("my-pipeline")
                .docs(d -> d
                        .source(JsonData.fromJson("{\"foo\":\"baz\"}"))
                )
        );
        logger.info("response = {}", response);
    }

    @Test
    void sourceRequest() throws IOException {
        String INDEX = "source-request";
        try {
            client.indices().delete(dir -> dir.index(INDEX));
        } catch (ElasticsearchException ignored) { }
        client.index(ir -> ir.index(INDEX).id("1").withJson(new StringReader("{\"foo\":\"bar\"}")));
        client.indices().refresh(rr -> rr.index(INDEX));
        assertThrows(TransportException.class, () -> {
            // This is failing with ES 8.11
            client.getSource(gsr -> gsr.index(INDEX).id("1"), ObjectNode.class);
        });
    }
}

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
import co.elastic.clients.elasticsearch._types.*;
import co.elastic.clients.elasticsearch._types.aggregations.StringTermsBucket;
import co.elastic.clients.elasticsearch.cat.IndicesResponse;
import co.elastic.clients.elasticsearch.cat.ShardsResponse;
import co.elastic.clients.elasticsearch.cat.ThreadPoolResponse;
import co.elastic.clients.elasticsearch.cat.indices.IndicesRecord;
import co.elastic.clients.elasticsearch.cat.shards.ShardsRecord;
import co.elastic.clients.elasticsearch.cat.thread_pool.ThreadPoolRecord;
import co.elastic.clients.elasticsearch.core.*;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.elasticsearch.indices.CreateIndexResponse;
import co.elastic.clients.elasticsearch.indices.PutMappingResponse;
import co.elastic.clients.elasticsearch.ingest.PutPipelineResponse;
import co.elastic.clients.elasticsearch.ingest.SimulateResponse;
import co.elastic.clients.elasticsearch.sql.TranslateResponse;
import co.elastic.clients.elasticsearch.transform.GetTransformResponse;
import co.elastic.clients.elasticsearch.transform.PutTransformResponse;
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
import org.junit.jupiter.api.*;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.random.RandomGenerator;

import static fr.pilato.test.elasticsearch.hlclient.SSLUtils.createContextFromCaCert;
import static fr.pilato.test.elasticsearch.hlclient.SSLUtils.createTrustAllCertsContext;
import static org.junit.Assume.assumeNotNull;
import static org.junit.jupiter.api.Assertions.*;

class EsClientIT {

    private static final Logger logger = LogManager.getLogger();
    private static ElasticsearchContainer container;
    private static RestClient restClient = null;
    private static ElasticsearchClient client = null;
    private static ElasticsearchAsyncClient asyncClient = null;
    private static final String PASSWORD = "changeme";
    private static final String PREFIX = "esclientit_";

    @BeforeAll
    static void startOptionallyTestContainers() throws IOException {
        client = getClient("https://localhost:9200", null);
        asyncClient = getAsyncClient("https://localhost:9200", null);
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
            asyncClient = getAsyncClient("https://" + container.getHttpHostAddress(), certAsBytes);
        }

        assumeNotNull(client);
        assumeNotNull(asyncClient);
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
            logger.info("Client connected to a cluster running version {} at {}.", info.version().number(), elasticsearchServiceAddress);
            return client;
        } catch (Exception e) {
            logger.debug("No cluster is running yet at {}.", elasticsearchServiceAddress);
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
            logger.info("Async Client connected to a cluster running version {} at {}.", info.version().number(), elasticsearchServiceAddress);
            return client;
        } catch (Exception e) {
            logger.debug("No cluster is running yet at {}.", elasticsearchServiceAddress);
            return null;
        }
    }

    List<String> indices;
    String indexName;
    
    @BeforeEach
    void cleanIndexBeforeRun(TestInfo testInfo) {
        indices = new ArrayList<>();
        String methodName = testInfo.getTestMethod().orElseThrow().getName();
        indexName = PREFIX + methodName.toLowerCase(Locale.ROOT);

        logger.debug("Using [{}] as the index name", indexName);
        setAndRemoveIndex(indexName);
    }

    @AfterEach
    void cleanIndexAfterRun() {
        indices.forEach(this::removeIndex);
    }

    @Test
    void getWithFilter() throws IOException {
        Reader input = new StringReader("{\"foo\":\"bar\", \"application_id\": 6}");
        client.index(ir -> ir.index(indexName).id("1").withJson(input));
        GetResponse<ObjectNode> getResponse = client.get(gr -> gr.index(indexName).id("1").sourceIncludes("application_id"), ObjectNode.class);
        assertEquals("{\"application_id\":6}", getResponse.source().toString());
    }

    @Test
    void getAsMap() throws IOException {
        Reader input = new StringReader("{\"foo\":\"bar\", \"application_id\": 6}");
        client.index(ir -> ir.index(indexName).id("1").withJson(input));
        GetResponse<ObjectNode> getResponse = client.get(gr -> gr.index(indexName).id("1"), ObjectNode.class);
        ObjectMapper mapper = new ObjectMapper();
        Map<String, Object> result = mapper.convertValue(getResponse.source(), new TypeReference<>() {});
        assertAll(
                () -> assertTrue(result.containsKey("foo")),
                () -> assertEquals(result.get("foo"), "bar"),
                () -> assertTrue(result.containsKey("application_id")),
                () -> assertEquals(6, result.get("application_id"))
        );
    }

    @Test
    void exist() throws IOException {
        client.index(ir -> ir.index(indexName).id("1").withJson(new StringReader("{\"foo\":\"bar\"}")));
        boolean exists1 = client.exists(gr -> gr.index(indexName).id("1")).value();
        assertTrue(exists1);
        boolean exists2 = client.exists(gr -> gr.index(indexName).id("2")).value();
        assertFalse(exists2);
    }

    @Test
    void createIndex() throws IOException {
        CreateIndexResponse response = client.indices().create(cir -> cir.index(indexName).mappings(m -> m.properties("content", p -> p.text(tp -> tp))));
        assertTrue(response.acknowledged());
    }

    @Test
    void callInfo() throws IOException {
        InfoResponse info = client.info();
        String version = info.version().number();
        assertNotNull(version);
        assertNotNull(info.clusterName());
        assertNotNull(info.tagline());
    }

    @Test
    void createMapping() throws IOException {
        client.indices().create(cir -> cir.index(indexName));
        PutMappingResponse response = client.indices().putMapping(pmr -> pmr.index(indexName).properties("foo", p -> p.text(tp -> tp)));
        assertTrue(response.acknowledged());
    }

    @Test
    void createData() throws IOException {
        IndexResponse indexResponse = client.index(ir -> ir.index(indexName).id("1").withJson(new StringReader("{\"foo\":\"bar\"}")));
        assertEquals(Result.Created, indexResponse.result());
        client.indices().refresh(rr -> rr.index(indexName));
        SearchResponse<Void> response = client.search(sr -> sr.index(indexName), Void.class);
        assertNotNull(response.hits().total());
        assertEquals(1, response.hits().total().value());
    }

    @Test
    void searchData() throws IOException {
        client.index(ir -> ir.index(indexName).id("1").withJson(new StringReader("{\"foo\":\"bar\"}")));
        client.indices().refresh(rr -> rr.index(indexName));

        {
            SearchResponse<Void> response = client.search(sr -> sr
                            .index(indexName)
                            .query(q -> q.match(mq -> mq.field("foo").query("bar"))),
                    Void.class);
            assertNotNull(response.hits().total());
            assertEquals(1, response.hits().total().value());
            assertEquals("1", response.hits().hits().get(0).id());
        }
        {
            SearchResponse<Void> response = client.search(sr -> sr
                            .index(indexName)
                            .query(q -> q.term(tq -> tq.field("foo").value("bar"))),
                    Void.class);
            assertNotNull(response.hits().total());
            assertEquals(1, response.hits().total().value());
            assertEquals("1", response.hits().hits().get(0).id());
        }
        {
            String matchAllQuery = Base64.getEncoder().encodeToString("{\"match_all\":{}}".getBytes(StandardCharsets.UTF_8));
            SearchResponse<Void> response = client.search(sr -> sr
                            .index(indexName)
                            .query(q -> q.wrapper(wq -> wq.query(matchAllQuery))),
                    Void.class);
            assertNotNull(response.hits().total());
            assertEquals(1, response.hits().total().value());
            assertEquals("1", response.hits().hits().get(0).id());
        }
        {
            SearchResponse<Void>  response = client.search(sr -> sr
                            .index(indexName)
                            .query(q -> q.matchAll(maq -> maq))
                            .trackScores(true),
                    Void.class);
            assertNotNull(response.hits().total());
            assertEquals(1, response.hits().total().value());
            assertEquals("1", response.hits().hits().get(0).id());
        }
    }

    @Test
    void translateSqlQuery() throws IOException {
        client.index(ir -> ir.index(indexName).id("1").withJson(new StringReader("{\"foo\":\"bar\"}")));
        client.indices().refresh(rr -> rr.index(indexName));

        TranslateResponse translateResponse = client.sql().translate(tr -> tr
                .query("SELECT * FROM " + indexName + " WHERE foo='bar' limit 10"));
        assertNotNull(translateResponse.query());
        assertEquals(10, translateResponse.size());
        assertNotNull(translateResponse.size());

        SearchResponse<Void> response = client.search(sr -> sr
                        .index(indexName)
                        .query(translateResponse.query())
                        .size(translateResponse.size().intValue()),
                Void.class);
        assertNotNull(response.hits().total());
        assertEquals(1, response.hits().total().value());
        assertEquals("1", response.hits().hits().get(0).id());
    }

    @Test
    void transformApi() throws IOException {
        String id = "test-get";

        try {
            client.transform().deleteTransform(dtr -> dtr.transformId(id));
        } catch (ElasticsearchException ignored) { }
        client.index(ir -> ir.index(indexName).id("1").withJson(new StringReader("{\"foo\":\"bar\"}")));
        client.indices().refresh(rr -> rr.index(indexName));
        PutTransformResponse putTransformResponse = client.transform().putTransform(ptr -> ptr
                .transformId(id)
                .source(s -> s.index(indexName).query(q -> q.matchAll(maq -> maq)))
                .dest(d -> d.index("pivot-dest"))
                .pivot(p -> p
                        .groupBy("reviewer", pgb -> pgb.terms(ta -> ta.field("user_id")))
                        .aggregations("avg_rating", a -> a.avg(aa -> aa.field("stars")))
                )
                .description("this is a test transform")
        );
        assertTrue(putTransformResponse.acknowledged());

        GetTransformResponse getTransformResponse = client.transform().getTransform(gt -> gt.transformId(id));
        assertEquals(1, getTransformResponse.count());
    }

    @Test
    void highlight() throws IOException {
        client.index(ir -> ir.index(indexName).withJson(new StringReader("{\"foo\":\"bar baz\"}")));
        client.indices().refresh(rr -> rr.index(indexName));
        SearchResponse<Void> response = client.search(sr -> sr
                        .index(indexName)
                        .query(q -> q.match(mq -> mq.field("foo").query("bar")))
                        .highlight(h -> h.fields("foo", hf -> hf.maxAnalyzedOffset(10)))
                , Void.class);
        assertNotNull(response.hits().total());
        assertEquals(1, response.hits().total().value());
        assertNotNull(response.hits().hits().get(0).highlight());
        assertTrue(response.hits().hits().get(0).highlight().containsKey("foo"));
        assertEquals(1, response.hits().hits().get(0).highlight().get("foo").size());
        assertEquals("<em>bar</em> baz", response.hits().hits().get(0).highlight().get("foo").get(0));
    }

    @Test
    void termsAgg() throws IOException {
        client.index(ir -> ir.index(indexName).id("1").withJson(new StringReader("{\"foo\":\"bar\"}")));
        client.index(ir -> ir.index(indexName).id("2").withJson(new StringReader("{\"foo\":\"bar\"}")));
        client.indices().refresh(rr -> rr.index(indexName));
        SearchResponse<Void> response = client.search(sr -> sr
                        .index(indexName)
                        .aggregations("top10foo", a -> a.terms(ta -> ta.field("foo.keyword").size(10)))
                , Void.class);
        assertNotNull(response.aggregations().get("top10foo"));
        assertNotNull(response.aggregations().get("top10foo").sterms());
        assertNotNull(response.aggregations().get("top10foo").sterms().buckets());
        assertEquals(1, response.aggregations().get("top10foo").sterms().buckets().array().size());

        StringTermsBucket top10foo = response.aggregations().get("top10foo").sterms().buckets().array().get(0);
        assertEquals("bar", top10foo.key().stringValue());
        assertEquals(2, top10foo.docCount());
    }

    @Test
    void bulkIngester() throws IOException {
        int size = 1000;
        try (BulkIngester<Void> ingester = BulkIngester.of(b -> b
                .client(client)
                .listener(new BulkListener<>() {
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
        )) {
            BinaryData data = BinaryData.of("{\"foo\":\"bar\"}".getBytes(StandardCharsets.UTF_8), ContentType.APPLICATION_JSON);
            for (int i = 0; i < size; i++) {
                ingester.add(bo -> bo.index(io -> io.index(indexName).document(data)));
            }
        }

        // Make sure to close (and flush) the bulk ingester before exiting if you are not using try-with-resources
        // ingester.close();

        client.indices().refresh(rr -> rr.index(indexName));
        SearchResponse<Void> response = client.search(sr -> sr.index(indexName), Void.class);
        assertNotNull(response.hits().total());
        assertEquals(size, response.hits().total().value());
    }

    @Test
    void rangeQuery() throws IOException {
        client.index(ir -> ir.index(indexName).id("1").withJson(new StringReader("{\"foo\":1}")));
        client.index(ir -> ir.index(indexName).id("2").withJson(new StringReader("{\"foo\":2}")));
        client.indices().refresh(rr -> rr.index(indexName));
        SearchResponse<ObjectNode> response = client.search(sr -> sr.index(indexName)
                        .query(q -> q.range(rq -> rq.field("foo").from("0").to("1")))
                , ObjectNode.class);
        assertNotNull(response.hits().total());
        assertEquals(1, response.hits().total().value());
        assertEquals("1", response.hits().hits().get(0).id());
    }

    @Test
    void bulk() throws IOException {
        int size = 1_000;
        AtomicInteger goodData = new AtomicInteger();
        BinaryData data = BinaryData.of("{\"foo\":\"bar\"}".getBytes(StandardCharsets.UTF_8), ContentType.APPLICATION_JSON);
        BinaryData wrongData = BinaryData.of("{\"foo\":\"bar}".getBytes(StandardCharsets.UTF_8), ContentType.APPLICATION_JSON);
        BulkResponse response = client.bulk(br -> {
            br.index(indexName);
            for (int i = 0; i < size; i++) {
                if (RandomGenerator.getDefault().nextBoolean()) {
                    br.operations(o -> o.index(ir -> ir.document(wrongData)));
                } else {
                    goodData.getAndIncrement();
                    br.operations(o -> o.index(ir -> ir.document(data)));
                }
            }
            return br;
        });
        logger.debug("bulk executed in {} ms {} errors", response.took(), response.errors() ? "with" : "without");
        if (response.errors()) {
            response.items().stream().filter(p -> p.error() != null)
                    .forEach(item -> {
                        assertNotNull(item.id());
                        assertNotNull(item.error().reason());
                        logger.trace("Error {} for id {}", item.error().reason(), item.id());
                    });
        }

        client.indices().refresh(rr -> rr.index(indexName));
        SearchResponse<Void> searchResponse = client.search(sr -> sr.index(indexName), Void.class);
        assertNotNull(searchResponse.hits().total());
        assertEquals(goodData.get(), searchResponse.hits().total().value());
    }

    @Test
    void searchWithBeans() throws IOException {
        Person p1 = new Person();
        p1.setId("1");
        p1.setName("Foo");
        Person p2 = new Person();
        p2.setId("2");
        p2.setName("Bar");
        client.index(ir -> ir.index(indexName).id(p1.getId()).document(p1));
        client.index(ir -> ir.index(indexName).id(p2.getId()).document(p2));
        client.indices().refresh(rr -> rr.index(indexName));
        SearchResponse<Person> response = client.search(sr -> sr.index(indexName), Person.class);
        for (Hit<Person> hit : response.hits().hits()) {
            assertNotNull(hit.id());
            assertNotNull(hit.source());
            assertEquals(hit.id(), hit.source().getId());
            assertNotNull(hit.source().getName());
        }
    }

    @Test
    void reindex() throws IOException {
        // Check the error is thrown when the source index does not exist
        ElasticsearchException exception = assertThrows(ElasticsearchException.class, () -> {
            client.reindex(rr -> rr.source(s -> s.index(PREFIX + "does-not-exists")).dest(d -> d.index("foo")));
        });
        assertEquals(404, exception.status());

        // A regular reindex operation
        setAndRemoveIndex(indexName + "-dest");

        client.index(ir -> ir.index(indexName).id("1").withJson(new StringReader("{\"foo\":1}")));
        client.indices().refresh(rr -> rr.index(indexName));
        ReindexResponse reindexResponse = client.reindex(rr -> rr.source(s -> s.index(indexName)).dest(d -> d.index(indexName + "-dest")));
        assertEquals(1, reindexResponse.total());
    }

    @Test
    void geoPointSort() throws IOException {
        client.indices().create(cir -> cir.index(indexName));
        client.indices().putMapping(pmr -> pmr.index(indexName).properties("location", p -> p.geoPoint(gp -> gp)));
        Person p1 = new Person();
        p1.setId("1");
        p1.setName("Foo");
        p1.setLocation(new GeoPoint(49.0404, 2.0174));
        Person p2 = new Person();
        p2.setId("2");
        p2.setName("Bar");
        p2.setLocation(new GeoPoint(38.7330, -109.8774));
        client.index(ir -> ir.index(indexName).id(p1.getId()).document(p1));
        client.index(ir -> ir.index(indexName).id(p2.getId()).document(p2));
        client.indices().refresh(rr -> rr.index(indexName));
        SearchResponse<Person> response = client.search(sr -> sr.index(indexName)
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

        assertNotNull(response.hits().total());
        assertEquals(2, response.hits().total().value());
        Hit<Person> hit1 = response.hits().hits().get(0);
        assertEquals("1", hit1.id());
        assertEquals(1, hit1.sort().size());
        assertEquals(0.0, hit1.sort().get(0).doubleValue());
        Hit<Person> hit2 = response.hits().hits().get(1);
        assertEquals("2", hit2.id());
        assertEquals(1, hit2.sort().size());
        assertEquals(8187.4318605250455, hit2.sort().get(0).doubleValue());
    }

    @Test
    void searchWithTimeout() throws IOException, ExecutionException, InterruptedException {
        client.index(ir -> ir.index(indexName).id("1").withJson(new StringReader("{\"foo\":\"bar\"}")));
        client.indices().refresh(rr -> rr.index(indexName));

        AtomicReference<Boolean> timeoutException = new AtomicReference<>(false);

        CompletableFuture<SearchResponse<Void>> future = asyncClient.search(sr -> sr
                                .index(indexName)
                                .query(q -> q.match(mq -> mq.field("foo").query("bar"))),
                        Void.class)
                .orTimeout(1, TimeUnit.NANOSECONDS)
                .exceptionally(e -> {
                    if (e instanceof TimeoutException) {
                        timeoutException.set(true);
                    } else {
                        logger.error("Got an unexpected exception", e);
                    }
                    return null;
                });
        assertNull(future.get());
        assertTrue(timeoutException.get());

        timeoutException.set(false);
        SearchResponse<Void> response = asyncClient.search(sr -> sr
                                .index(indexName)
                                .query(q -> q.match(mq -> mq.field("foo").query("bar"))),
                        Void.class)
                .orTimeout(10, TimeUnit.SECONDS)
                .exceptionally(e -> {
                    if (e instanceof TimeoutException) {
                        timeoutException.set(true);
                    } else {
                        logger.error("Got an unexpected exception", e);
                    }
                    return null;
                })
                .get();
        assertFalse(timeoutException.get());
        assertNotNull(response.hits().total());
        assertEquals(1, response.hits().total().value());
    }

    @Test
    void catApi() throws IOException {
        ThreadPoolResponse threadPool = client.cat().threadPool();
        assertNotNull(threadPool);
        for (ThreadPoolRecord record : threadPool.valueBody()) {
            logger.debug("threadPool = {}", record);
            assertNotNull(record.nodeName());
            assertNotNull(record.name());
            assertNotNull(record.active());
            assertNotNull(record.queue());
            assertNotNull(record.rejected());
        }
        IndicesResponse indices = client.cat().indices();
        assertNotNull(indices);
        for (IndicesRecord record : indices.valueBody()) {
            logger.debug("index = {}", record);
            assertNotNull(record.index());
            assertNotNull(record.docsCount());
            assertNotNull(record.docsDeleted());
        }
        ShardsResponse shards = client.cat().shards();
        assertNotNull(shards);
        for (ShardsRecord record : shards.valueBody()) {
            logger.debug("shard = {}", record);
            assertNotNull(record.index());
            assertNotNull(record.state());
            assertNotNull(record.prirep());
        }
    }

    @Test
    void ingestPipelines() throws IOException {
        // Define some pipelines
        try {
            client.ingest().deletePipeline(pr -> pr.id("my-pipeline"));
        } catch (ElasticsearchException ignored) { }
        {
            PutPipelineResponse response = client.ingest().putPipeline(pr -> pr
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
            assertTrue(response.acknowledged());
        }
        {
            PutPipelineResponse response = client.ingest().putPipeline(pr -> pr
                    .id("my-pipeline")
                    .processors(p -> p
                            .set(s -> s
                                    .field("foo")
                                    .value(JsonData.of("bar"))
                                    .ignoreFailure(true)
                            )
                    )
            );
            assertTrue(response.acknowledged());
        }
        {
            SimulateResponse response = client.ingest().simulate(sir -> sir
                    .id("my-pipeline")
                    .docs(d -> d
                            .source(JsonData.fromJson("{\"foo\":\"baz\"}"))
                    )
            );
            assertEquals(1, response.docs().size());
            assertNotNull(response.docs().get(0).doc());
            assertNotNull(response.docs().get(0).doc().source());
            assertEquals("bar", response.docs().get(0).doc().source().get("foo").to(String.class));
        }
    }

    @Test
    void sourceRequest() throws IOException {
        client.index(ir -> ir.index(indexName).id("1").withJson(new StringReader("{\"foo\":\"bar\"}")));
        client.indices().refresh(rr -> rr.index(indexName));
        assertThrows(TransportException.class, () -> {
            // This is failing with ES 8.11. So this test should fail when the following issue will be fixed:
            // See https://github.com/elastic/elasticsearch-java/issues/741
            client.getSource(gsr -> gsr.index(indexName).id("1"), ObjectNode.class);
        });
    }

    @Test
    void deleteByQuery() throws IOException {
        client.index(ir -> ir.index(indexName).id("1").withJson(new StringReader("{\"foo\":\"bar\"}")));
        client.indices().refresh(rr -> rr.index(indexName));
        SearchResponse<Void> response = client.search(sr -> sr.index(indexName), Void.class);
        assertNotNull(response.hits().total());
        assertEquals(1L, response.hits().total().value());
        DeleteByQueryResponse deleteByQueryResponse = client.deleteByQuery(dbq -> dbq
                .index(indexName)
                .query(q -> q
                        .match(mq -> mq
                                .field("foo")
                                .query("bar"))));
        assertEquals(1L, deleteByQueryResponse.deleted());
        client.indices().refresh(rr -> rr.index(indexName));
        response = client.search(sr -> sr.index(indexName), Void.class);
        assertNotNull(response.hits().total());
        assertEquals(0L, response.hits().total().value());
    }

    @Test
    void updateDocument() throws IOException {
        client.index(ir -> ir.index(indexName).id("1").withJson(new StringReader("{\"show_count\":0}")));
        client.update(ur -> ur.index(indexName).id("1").script(
                s -> s.inline(is -> is
                        .lang(ScriptLanguage.Painless)
                        .source("ctx._source.show_count += 1"))
        ), ObjectNode.class);
        GetResponse<ObjectNode> response = client.get(gr -> gr.index(indexName).id("1"), ObjectNode.class);
        assertEquals("{\"show_count\":1}", response.source().toString());
    }

    /**
     * This method adds the index name we want to use to the list
     * and deletes the index if it exists.
     * @param name the index name
     */
    private void setAndRemoveIndex(String name) {
        indices.add(name);
        removeIndex(name);
    }

    /**
     * This method deletes the index if it exists.
     * @param name the index name
     */
    private void removeIndex(String name) {
        try {
            client.indices().delete(dir -> dir.index(name));
            logger.debug("Index [{}] has been removed", name);
        } catch (IOException | ElasticsearchException ignored) { }
    }
}

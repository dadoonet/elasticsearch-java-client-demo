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
import co.elastic.clients.elasticsearch._helpers.esql.jdbc.ResultSetEsqlAdapter;
import co.elastic.clients.elasticsearch._helpers.esql.objects.ObjectsEsqlAdapter;
import co.elastic.clients.elasticsearch._types.*;
import co.elastic.clients.elasticsearch._types.aggregations.StringTermsBucket;
import co.elastic.clients.elasticsearch.cat.IndicesResponse;
import co.elastic.clients.elasticsearch.cat.ShardsResponse;
import co.elastic.clients.elasticsearch.cat.ThreadPoolResponse;
import co.elastic.clients.elasticsearch.cat.indices.IndicesRecord;
import co.elastic.clients.elasticsearch.cat.shards.ShardsRecord;
import co.elastic.clients.elasticsearch.cat.thread_pool.ThreadPoolRecord;
import co.elastic.clients.elasticsearch.cluster.PutComponentTemplateResponse;
import co.elastic.clients.elasticsearch.core.*;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.elasticsearch.indices.*;
import co.elastic.clients.elasticsearch.ingest.DocumentSimulation;
import co.elastic.clients.elasticsearch.ingest.PutPipelineResponse;
import co.elastic.clients.elasticsearch.ingest.SimulateResponse;
import co.elastic.clients.elasticsearch.sql.TranslateResponse;
import co.elastic.clients.elasticsearch.transform.GetTransformResponse;
import co.elastic.clients.elasticsearch.transform.PutTransformResponse;
import co.elastic.clients.json.JsonData;
import co.elastic.clients.json.JsonpMappingException;
import co.elastic.clients.transport.TransportException;
import co.elastic.clients.transport.endpoints.BinaryResponse;
import co.elastic.clients.util.BinaryData;
import co.elastic.clients.util.ContentType;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.*;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.SQLException;
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
    private static ElasticsearchClient client = null;
    private static ElasticsearchAsyncClient asyncClient = null;
    private static final String PASSWORD = "changeme";
    private static final String PREFIX = "esclientit_";

    @BeforeAll
    static void startOptionallyTestContainers() throws IOException {
        final var props = new Properties();
        props.load(EsClientIT.class.getResourceAsStream("/version.properties"));
        final String version = props.getProperty("elasticsearch.version");
        logger.info("Starting testcontainers with Elasticsearch {}.", props.getProperty("elasticsearch.version"));
        // Start the container. This step might take some time...
        final ElasticsearchContainer container = new ElasticsearchContainer(
                DockerImageName.parse("docker.elastic.co/elasticsearch/elasticsearch")
                        .withTag(version))
                .withPassword(PASSWORD)
                .withReuse(true);
        container.start();
        final byte[] certAsBytes = container.copyFileFromContainer(
                "/usr/share/elasticsearch/config/certs/http_ca.crt",
                InputStream::readAllBytes);
        try {
            client = getClient("https://" + container.getHttpHostAddress(), certAsBytes);
            asyncClient = getAsyncClient("https://" + container.getHttpHostAddress(), certAsBytes);
        } catch (Exception e) {
            logger.debug("No cluster is running yet at https://{}.", container.getHttpHostAddress());
        }

        assumeNotNull(client);
        assumeNotNull(asyncClient);
    }

    @AfterAll
    static void elasticsearchClient() throws IOException {
        if (client != null) {
            client.close();
        }
        if (asyncClient != null) {
            asyncClient.close();
        }
    }

    static private ElasticsearchClient getClient(final String elasticsearchServiceAddress, final byte[] certificate) throws Exception {
        // Create the API client
        final ElasticsearchClient client = ElasticsearchClient.of(b -> b
                .host(elasticsearchServiceAddress)
                .sslContext(certificate != null ? createContextFromCaCert(certificate) : createTrustAllCertsContext())
                .usernameAndPassword("elastic", PASSWORD)
        );
        final InfoResponse info = client.info();
        logger.info("Client connected to a cluster running version {} at {}.", info.version().number(), elasticsearchServiceAddress);
        return client;
    }

    static private ElasticsearchAsyncClient getAsyncClient(final String elasticsearchServiceAddress, final byte[] certificate) throws Exception {
        // Create the API client
        final ElasticsearchAsyncClient client = ElasticsearchAsyncClient.of(b -> b
                .host(elasticsearchServiceAddress)
                .sslContext(certificate != null ? createContextFromCaCert(certificate) : createTrustAllCertsContext())
                .usernameAndPassword("elastic", PASSWORD)
        );
        final InfoResponse info = client.info().get();
        logger.info("Async Client connected to a cluster running version {} at {}.", info.version().number(), elasticsearchServiceAddress);
        return client;
    }

    List<String> indices;
    String indexName;
    
    @BeforeEach
    void cleanIndexBeforeRun(final TestInfo testInfo) {
        indices = new ArrayList<>();
        final var methodName = testInfo.getTestMethod().orElseThrow().getName();
        indexName = PREFIX + methodName.toLowerCase(Locale.ROOT);

        logger.debug("Using [{}] as the index name", indexName);
        setAndRemoveIndex(indexName);
    }

    @AfterEach
    void cleanIndexAfterRun() {
        indices.forEach(this::removeIndex);
    }

    @Test
    void getDocument() throws IOException {
        client.index(ir -> ir.index(indexName).id("1")
                .withJson(new StringReader("{\"foo\":\"bar\", \"application_id\": 6}")));
        {
            final GetResponse<ObjectNode> getResponse = client.get(gr -> gr.index(indexName).id("1"), ObjectNode.class);
            assumeNotNull(getResponse.source());
            assertEquals("{\"foo\":\"bar\",\"application_id\":6}", getResponse.source().toString());
        }
        {
            // With source filtering
            final GetResponse<ObjectNode> getResponse = client.get(gr -> gr.index(indexName).id("1").sourceIncludes("application_id"), ObjectNode.class);
            assumeNotNull(getResponse.source());
            assertEquals("{\"application_id\":6}", getResponse.source().toString());
        }
        {
            // Get as Map
            final GetResponse<ObjectNode> getResponse = client.get(gr -> gr.index(indexName).id("1"), ObjectNode.class);
            final ObjectMapper mapper = new ObjectMapper();
            final Map<String, Object> result = mapper.convertValue(getResponse.source(), new TypeReference<>() {});
            assertAll(
                    () -> assertTrue(result.containsKey("foo")),
                    () -> assertEquals("bar", result.get("foo")),
                    () -> assertTrue(result.containsKey("application_id")),
                    () -> assertEquals(6, result.get("application_id"))
            );
        }
    }

    @Test
    void exists() throws IOException {
        client.index(ir -> ir.index(indexName).id("1")
                .withJson(new StringReader("{\"foo\":\"bar\"}")));
        assertTrue(client.exists(gr -> gr.index(indexName).id("1")).value());
        assertFalse(client.exists(gr -> gr.index(indexName).id("2")).value());
    }

    @Test
    void createIndex() throws IOException {
        final CreateIndexResponse response = client.indices().create(cir -> cir.index(indexName)
                .mappings(m -> m.properties("content", p -> p.text(tp -> tp))));
        assertTrue(response.acknowledged());
    }

    @Test
    void callInfo() throws IOException {
        final InfoResponse info = client.info();
        final String version = info.version().number();
        assertNotNull(version);
        assertNotNull(info.clusterName());
        assertNotNull(info.tagline());
    }

    @Test
    void createMapping() throws IOException {
        client.indices().create(cir -> cir.index(indexName));
        final PutMappingResponse response = client.indices().putMapping(pmr -> pmr.index(indexName)
                .properties("foo", p -> p.text(tp -> tp)));
        assertTrue(response.acknowledged());
    }

    @Test
    void createData() throws IOException {
        final IndexResponse indexResponse = client.index(ir -> ir.index(indexName).id("1")
                .withJson(new StringReader("{\"foo\":\"bar\"}")));
        assertEquals(Result.Created, indexResponse.result());
        client.indices().refresh(rr -> rr.index(indexName));
        final SearchResponse<Void> response = client.search(sr -> sr.index(indexName), Void.class);
        assertNotNull(response.hits().total());
        assertEquals(1, response.hits().total().value());
    }

    @Test
    void searchData() throws IOException {
        client.index(ir -> ir.index(indexName).id("1")
                .withJson(new StringReader("{\"foo\":\"bar\"}")));
        client.indices().refresh(rr -> rr.index(indexName));
        {
            final SearchResponse<Void> response = client.search(sr -> sr
                            .index(indexName)
                            .query(q -> q.match(mq -> mq.field("foo").query("bar"))),
                    Void.class);
            assertNotNull(response.hits().total());
            assertEquals(1, response.hits().total().value());
            assertEquals("1", response.hits().hits().get(0).id());
        }
        {
            final SearchResponse<Void> response = client.search(sr -> sr
                            .index(indexName)
                            .query(q -> q.term(tq -> tq.field("foo").value("bar"))),
                    Void.class);
            assertNotNull(response.hits().total());
            assertEquals(1, response.hits().total().value());
            assertEquals("1", response.hits().hits().get(0).id());
        }
        {
            final String matchAllQuery = Base64.getEncoder().encodeToString("{\"match_all\":{}}".getBytes(StandardCharsets.UTF_8));
            final SearchResponse<Void> response = client.search(sr -> sr
                            .index(indexName)
                            .query(q -> q.wrapper(wq -> wq.query(matchAllQuery))),
                    Void.class);
            assertNotNull(response.hits().total());
            assertEquals(1, response.hits().total().value());
            assertEquals("1", response.hits().hits().get(0).id());
        }
        {
            final SearchResponse<Void>  response = client.search(sr -> sr
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
        client.index(ir -> ir.index(indexName).id("1")
                .withJson(new StringReader("{\"foo\":\"bar\"}")));
        client.indices().refresh(rr -> rr.index(indexName));

        final TranslateResponse translateResponse = client.sql().translate(tr -> tr
                .query("SELECT * FROM " + indexName + " WHERE foo='bar' limit 10"));
        assertNotNull(translateResponse.query());
        assertEquals(10, translateResponse.size());
        assertNotNull(translateResponse.size());

        final SearchResponse<Void> response = client.search(sr -> sr
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
        final var id = "test-get";
        try {
            client.transform().deleteTransform(dtr -> dtr.transformId(id));
        } catch (ElasticsearchException ignored) { }
        client.index(ir -> ir.index(indexName).id("1")
                .withJson(new StringReader("{\"foo\":\"bar\"}")));
        client.indices().refresh(rr -> rr.index(indexName));
        final PutTransformResponse putTransformResponse = client.transform().putTransform(ptr -> ptr
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

        final GetTransformResponse getTransformResponse = client.transform().getTransform(gt -> gt.transformId(id));
        assertEquals(1, getTransformResponse.count());
    }

    @Test
    void highlight() throws IOException {
        client.index(ir -> ir.index(indexName)
                .withJson(new StringReader("{\"foo\":\"bar baz\"}")));
        client.indices().refresh(rr -> rr.index(indexName));
        final SearchResponse<Void> response = client.search(sr -> sr
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
        client.index(ir -> ir.index(indexName).id("1")
                .withJson(new StringReader("{\"foo\":\"bar\"}")));
        client.index(ir -> ir.index(indexName).id("2")
                .withJson(new StringReader("{\"foo\":\"bar\"}")));
        client.indices().refresh(rr -> rr.index(indexName));
        final SearchResponse<Void> response = client.search(sr -> sr
                        .index(indexName)
                        .aggregations("top10foo", a -> a
                                .terms(ta -> ta.field("foo.keyword").size(10)))
                , Void.class);
        assertNotNull(response.aggregations().get("top10foo"));
        assertNotNull(response.aggregations().get("top10foo").sterms());
        assertNotNull(response.aggregations().get("top10foo").sterms().buckets());
        assertEquals(1, response.aggregations().get("top10foo").sterms().buckets().array().size());

        final StringTermsBucket top10foo = response.aggregations().get("top10foo").sterms().buckets().array().get(0);
        assertEquals("bar", top10foo.key().stringValue());
        assertEquals(2, top10foo.docCount());
    }

    @Test
    void bulkIngester() throws IOException {
        final var size = 1000;
        try (final BulkIngester<Void> ingester = BulkIngester.of(b -> b
                .client(client)
                .globalSettings(gs -> gs
                        .index(indexName)
                )
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
            final var data = BinaryData.of("{\"foo\":\"bar\"}".getBytes(StandardCharsets.UTF_8), ContentType.APPLICATION_JSON);
            for (int i = 0; i < size; i++) {
                ingester.add(bo -> bo.index(io -> io.document(data)));
            }
        }

        // Make sure to close (and flush) the bulk ingester before exiting if you are not using try-with-resources
        // ingester.close();

        client.indices().refresh(rr -> rr.index(indexName));
        final SearchResponse<Void> response = client.search(sr -> sr.index(indexName), Void.class);
        assertNotNull(response.hits().total());
        assertEquals(size, response.hits().total().value());
    }

    @Test
    void bulkIngesterFlush() throws IOException {
        final var size = 100_000;
        try (final BulkIngester<Void> ingester = BulkIngester.of(b -> b
                .client(client)
                .globalSettings(gs -> gs
                        .index(indexName)
                )
                .maxOperations(10_000)
                .flushInterval(5, TimeUnit.SECONDS)
        )) {
            final var data = BinaryData.of("{\"foo\":\"bar\"}".getBytes(StandardCharsets.UTF_8), ContentType.APPLICATION_JSON);
            for (int i = 0; i < size; i++) {
                ingester.add(bo -> bo.index(io -> io.document(data)));
            }

            // Calling flush should actually flush the ingester and send the latest docs
            ingester.flush();

            client.indices().refresh(rr -> rr.index(indexName));
            final SearchResponse<Void> response = client.search(sr -> sr.index(indexName).trackTotalHits(tth -> tth.enabled(true)), Void.class);
            assertNotNull(response.hits().total());

            // But this test is failing as the flush is not sending the last batch
            // assertEquals(size, response.hits().total().value());
            assertEquals(size - 10_000, response.hits().total().value());
        }
    }

    @Test
    void rangeQuery() throws IOException {
        client.index(ir -> ir.index(indexName).id("1").withJson(new StringReader("{\"foo\":1}")));
        client.index(ir -> ir.index(indexName).id("2").withJson(new StringReader("{\"foo\":2}")));
        client.indices().refresh(rr -> rr.index(indexName));
        final SearchResponse<ObjectNode> response = client.search(sr -> sr.index(indexName)
                        .query(q -> q.range(rq -> rq
                                .number(nrq -> nrq.field("foo").gte(0.0).lte(1.0))
                        ))
                , ObjectNode.class);
        assertNotNull(response.hits().total());
        assertEquals(1, response.hits().total().value());
        assertEquals("1", response.hits().hits().get(0).id());
    }

    @Test
    void bulk() throws IOException {
        final var size = 1_000;
        final var goodData = new AtomicInteger();
        final var data = BinaryData.of("{\"foo\":\"bar\"}".getBytes(StandardCharsets.UTF_8), ContentType.APPLICATION_JSON);
        final var wrongData = BinaryData.of("{\"foo\":\"bar}".getBytes(StandardCharsets.UTF_8), ContentType.APPLICATION_JSON);
        final BulkResponse response = client.bulk(br -> {
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
        final SearchResponse<Void> searchResponse = client.search(sr -> sr.index(indexName), Void.class);
        assertNotNull(searchResponse.hits().total());
        assertEquals(goodData.get(), searchResponse.hits().total().value());
    }

    @Test
    void searchWithBeans() throws IOException {
        final var p1 = new Person();
        p1.setId("1");
        p1.setName("Foo");
        final var p2 = new Person();
        p2.setId("2");
        p2.setName("Bar");
        client.index(ir -> ir.index(indexName).id(p1.getId()).document(p1));
        client.index(ir -> ir.index(indexName).id(p2.getId()).document(p2));
        client.indices().refresh(rr -> rr.index(indexName));
        final SearchResponse<Person> response = client.search(sr -> sr.index(indexName), Person.class);
        for (final Hit<Person> hit : response.hits().hits()) {
            assertNotNull(hit.id());
            assertNotNull(hit.source());
            assertEquals(hit.id(), hit.source().getId());
            assertNotNull(hit.source().getName());
        }
    }

    @Test
    void reindex() throws IOException {
        // Check the error is thrown when the source index does not exist
        final ElasticsearchException exception = assertThrows(ElasticsearchException.class,
                () -> client.reindex(rr -> rr
                        .source(s -> s.index(PREFIX + "does-not-exists")).dest(d -> d.index("foo"))));
        assertEquals(404, exception.status());

        // A regular reindex operation
        setAndRemoveIndex(indexName + "-dest");

        client.index(ir -> ir.index(indexName).id("1").withJson(new StringReader("{\"foo\":1}")));
        client.indices().refresh(rr -> rr.index(indexName));
        final ReindexResponse reindexResponse = client.reindex(rr -> rr
                .source(s -> s.index(indexName)).dest(d -> d.index(indexName + "-dest")));
        assertEquals(1, reindexResponse.total());
    }

    @Test
    void geoPointSort() throws IOException {
        client.indices().create(cir -> cir.index(indexName));
        client.indices().putMapping(pmr -> pmr.index(indexName).properties("location", p -> p.geoPoint(gp -> gp)));
        final var p1 = new Person();
        p1.setId("1");
        p1.setName("Foo");
        p1.setLocation(new GeoPoint(49.0404, 2.0174));
        final var p2 = new Person();
        p2.setId("2");
        p2.setName("Bar");
        p2.setLocation(new GeoPoint(38.7330, -109.8774));
        client.index(ir -> ir.index(indexName).id(p1.getId()).document(p1));
        client.index(ir -> ir.index(indexName).id(p2.getId()).document(p2));
        client.indices().refresh(rr -> rr.index(indexName));
        final SearchResponse<Person> response = client.search(sr -> sr.index(indexName)
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
        final Hit<Person> hit1 = response.hits().hits().get(0);
        assertEquals("1", hit1.id());
        assertEquals(1, hit1.sort().size());
        assertEquals(0.0, hit1.sort().get(0).doubleValue());
        final Hit<Person> hit2 = response.hits().hits().get(1);
        assertEquals("2", hit2.id());
        assertEquals(1, hit2.sort().size());
        assertEquals(8187.4318605250455, hit2.sort().get(0).doubleValue());
    }

    @Test
    void geoPointSearch() throws IOException {
        client.indices().create(cir -> cir.index(indexName));
        client.indices().putMapping(pmr -> pmr.index(indexName).properties("location", p -> p.geoPoint(gp -> gp)));
        final var p1 = new Person();
        p1.setId("1");
        p1.setName("Foo");
        p1.setLocation(new GeoPoint(49.0404, 2.0174));
        final var p2 = new Person();
        p2.setId("2");
        p2.setName("Bar");
        p2.setLocation(new GeoPoint(38.7330, -109.8774));
        client.index(ir -> ir.index(indexName).id(p1.getId()).document(p1));
        client.index(ir -> ir.index(indexName).id(p2.getId()).document(p2));
        client.indices().refresh(rr -> rr.index(indexName));
        final SearchResponse<Person> response = client.search(sr -> sr.index(indexName)
                        .query(q -> q.geoBoundingBox(gbb -> gbb
                                .field("location")
                                .boundingBox(bbq -> bbq
                                        .coords(c -> c
                                                .bottom(0).left(0).top(50).right(10))
                        )))
                , Person.class);

        assertNotNull(response.hits().total());
        assertEquals(1, response.hits().total().value());
        Hit<Person> hit1 = response.hits().hits().get(0);
        assertEquals("1", hit1.id());
    }

    @Test
    void searchWithTimeout() throws IOException, ExecutionException, InterruptedException {
        client.index(ir -> ir.index(indexName).id("1").withJson(new StringReader("{\"foo\":\"bar\"}")));
        client.indices().refresh(rr -> rr.index(indexName));

        final var timeoutException = new AtomicReference<>(false);

        final CompletableFuture<SearchResponse<Void>> future = asyncClient.search(sr -> sr
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
        final SearchResponse<Void> response = asyncClient.search(sr -> sr
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
        final ThreadPoolResponse threadPool = client.cat().threadPool();
        assertNotNull(threadPool);
        for (final ThreadPoolRecord record : threadPool.threadPools()) {
            logger.debug("threadPool = {}", record);
            assertNotNull(record.nodeName());
            assertNotNull(record.name());
            assertNotNull(record.active());
            assertNotNull(record.queue());
            assertNotNull(record.rejected());
        }
        final IndicesResponse indices = client.cat().indices();
        assertNotNull(indices);
        for (final IndicesRecord record : indices.indices()) {
            logger.debug("index = {}", record);
            assertNotNull(record.index());
            assertNotNull(record.docsCount());
            assertNotNull(record.docsDeleted());
        }
        final ShardsResponse shards = client.cat().shards();
        assertNotNull(shards);
        for (final ShardsRecord record : shards.shards()) {
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
        } catch (final ElasticsearchException ignored) { }
        {
            final PutPipelineResponse response = client.ingest().putPipeline(pr -> pr
                    .id("my-pipeline")
                    .processors(p -> p
                            .script(s -> s
                                    .lang(ScriptLanguage.Painless)
                                    .source(src -> src.scriptString("ctx.foo = 'bar'"))
                            )
                    )
            );
            assertTrue(response.acknowledged());
        }
        {
            final PutPipelineResponse response = client.ingest().putPipeline(pr -> pr
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
            final SimulateResponse response = client.ingest().simulate(sir -> sir
                    .id("my-pipeline")
                    .docs(d -> d
                            .source(JsonData.fromJson("{\"foo\":\"baz\"}"))
                    )
            );
            assertEquals(1, response.docs().size());
            final DocumentSimulation doc = response.docs().get(0).doc();
            assertNotNull(doc);
            assertNotNull(doc.source());
            assertEquals("bar", doc.source().get("foo").to(String.class));
        }
    }

    @Test
    void sourceRequest() throws IOException {
        client.index(ir -> ir.index(indexName).id("1").withJson(new StringReader("{\"foo\":\"bar\"}")));
        client.indices().refresh(rr -> rr.index(indexName));
        final GetSourceResponse<ObjectNode> source = client.getSource(gsr -> gsr.index(indexName).id("1"), ObjectNode.class);
        assertEquals("{\"foo\":\"bar\"}", source.source().toString());
    }

    @Test
    void deleteByQuery() throws IOException {
        client.index(ir -> ir.index(indexName).id("1").withJson(new StringReader("{\"foo\":\"bar\"}")));
        client.indices().refresh(rr -> rr.index(indexName));
        final SearchResponse<Void> response1 = client.search(sr -> sr.index(indexName), Void.class);
        assertNotNull(response1.hits().total());
        assertEquals(1L, response1.hits().total().value());
        final DeleteByQueryResponse deleteByQueryResponse = client.deleteByQuery(dbq -> dbq
                .index(indexName)
                .query(q -> q
                        .match(mq -> mq
                                .field("foo")
                                .query("bar"))));
        assertEquals(1L, deleteByQueryResponse.deleted());
        client.indices().refresh(rr -> rr.index(indexName));
        final SearchResponse<Void> response2 = client.search(sr -> sr.index(indexName), Void.class);
        assertNotNull(response2.hits().total());
        assertEquals(0L, response2.hits().total().value());
    }

    @Test
    void updateDocument() throws IOException {
        client.index(ir -> ir.index(indexName).id("1").withJson(new StringReader("{\"show_count\":0}")));
        client.update(ur -> ur.index(indexName).id("1").script(
                s -> s
                        .lang(ScriptLanguage.Painless)
                        .source(src -> src.scriptString("ctx._source.show_count += 1"))
        ), ObjectNode.class);
        final GetResponse<ObjectNode> response = client.get(gr -> gr.index(indexName).id("1"), ObjectNode.class);
        assumeNotNull(response.source());
        assertEquals("{\"show_count\":1}", response.source().toString());
    }

    @Test
    void createComponentTemplate() throws IOException {
        {
            final PutComponentTemplateResponse response = client.cluster().putComponentTemplate(pct -> pct
                    .name("my_component_template")
                    .template(t -> t
                            .settings(s -> s.numberOfShards("1").numberOfReplicas("0"))
                            .mappings(m -> m
                                    .properties("foo", p -> p.text(tp -> tp))
                            )
                    )
            );
            assertTrue(response.acknowledged());
        }

        {
            // With JSON
            final PutComponentTemplateResponse response = client.cluster().putComponentTemplate(pct -> pct
                    .name("my_component_template")
                    .template(t -> t
                            .mappings(
                                    m -> m.properties("@timestamp", p -> p.date(dp -> dp))
                            )
                    )
            );
            assertTrue(response.acknowledged());
        }
    }

    @Test
    void createIndexTemplate() throws IOException {
        client.cluster().putComponentTemplate(pct -> pct
                .name("my_component_template")
                .template(t -> t
                        .settings(s -> s.numberOfShards("1").numberOfReplicas("0"))
                        .mappings(m -> m
                                .properties("foo", p -> p.text(tp -> tp))
                        )
                )
        );
        final PutIndexTemplateResponse response = client.indices().putIndexTemplate(pit -> pit
                .name("my_index_template")
                .indexPatterns("my-index-*")
                .composedOf("my_component_template")
                .template(t -> t
                        .aliases("foo", a -> a
                                .indexRouting("bar")
                        )
                        .settings(s -> s.numberOfShards("1").numberOfReplicas("0"))
                        .mappings(m -> m
                                .properties("foo", p -> p.text(tp -> tp))
                        )
                )
        );
        assertTrue(response.acknowledged());
    }

    @Test
    void elser() throws IOException {
        // Create the index with sparse vector
        client.indices().create(cir -> cir.index(indexName).mappings(m -> m
                .properties("content", p -> p.text(tp -> tp))
                .properties("content_embedding", p -> p.sparseVector(sp -> sp))
        ));

        // Create the pipeline
        // This requires to have the elserv2 model deployed and started
        client.ingest().putPipeline(pr -> pr
                .id("elser-v2-test")
                .processors(p -> p
                        .inference(i -> i
                                .modelId(".elser_model_2")
                                .fieldMap("content", JsonData.of("content"))
                                .targetField("content_embedding")
                        )
                )
        );

        // We are expecting an exception as the model is not deployed
        final ElasticsearchException exception = assertThrows(ElasticsearchException.class, () -> {
            // Search
            client.search(sr -> sr
                    .index(indexName)
                    .query(q -> q.sparseVector(sv -> sv
                            .field("content_embedding")
                            .inferenceId("elser-v2-test")
                            .query("How to avoid muscle soreness after running?")
                    )), ObjectNode.class);
        });
        assertEquals("current license is non-compliant for [inference]", exception.error().reason());
        assertEquals(403, exception.status());
    }

    @Test
    void testIlm() throws IOException {
        try {
            client.ilm().deleteLifecycle(dlr -> dlr.name(indexName + "-ilm"));
        } catch (IOException | ElasticsearchException ignored) { }
        client.ilm().putLifecycle(plr -> plr
                .name(indexName + "-ilm")
                .policy(p -> p
                        .phases(ph -> ph
                                .hot(h -> h
                                        .actions(a -> a
                                                .rollover(r -> r
                                                        .maxAge(t -> t.time("5d"))
                                                        .maxSize("10gb")
                                                )
                                        )
                                )
                        )
                )
        );
    }

    @Test
    void searchExistField() throws IOException {
        client.index(ir -> ir.index(indexName).id("1").withJson(new StringReader("{\"foo\":\"baz\"}")));
        client.index(ir -> ir.index(indexName).id("2").withJson(new StringReader("{\"foo\":\"baz\", \"bar\":\"baz\"}")));
        client.indices().refresh(rr -> rr.index(indexName));
        final SearchResponse<Void> response = client.search(sr -> sr
                        .index(indexName)
                        .query(q -> q.exists(eq -> eq.field("bar")))
                , Void.class);
        assertNotNull(response.hits().total());
        assertEquals(1, response.hits().total().value());
        assertEquals("2", response.hits().hits().get(0).id());
    }

    @Test
    void multipleAggs() throws IOException {
        client.index(ir -> ir.index(indexName).withJson(new StringReader("{\"country\":\"france\",\"state\":\"paris\",\"city\":\"paris\"}")));
        client.index(ir -> ir.index(indexName).withJson(new StringReader("{\"country\":\"germany\",\"state\":\"berlin\",\"city\":\"berlin\"}")));
        client.index(ir -> ir.index(indexName).withJson(new StringReader("{\"country\":\"italy\",\"state\":\"rome\",\"city\":\"rome\"}")));
        client.indices().refresh(rr -> rr.index(indexName));
        final SearchResponse<Void> response = client.search(sr -> sr
                        .index(indexName)
                        .aggregations("country", a -> a.terms(ta -> ta.field("country.keyword"))
                                .aggregations("state", sa -> sa.terms(ta -> ta.field("state.keyword"))
                                        .aggregations("city", ca -> ca.terms(ta -> ta.field("city.keyword")))
                                )
                        )
                , Void.class);
        assertNotNull(response.aggregations().get("country"));
        assertNotNull(response.aggregations().get("country").sterms());
        assertNotNull(response.aggregations().get("country").sterms().buckets());
        assertEquals(3, response.aggregations().get("country").sterms().buckets().array().size());
        final StringTermsBucket country = response.aggregations().get("country").sterms().buckets().array().get(0);
        assertEquals("france", country.key().stringValue());
        assertNotNull(country.aggregations().get("state"));
        assertNotNull(country.aggregations().get("state").sterms());
        assertNotNull(country.aggregations().get("state").sterms().buckets());
        assertEquals(1, country.aggregations().get("state").sterms().buckets().array().size());
        final StringTermsBucket state = country.aggregations().get("state").sterms().buckets().array().get(0);
        assertEquals("paris", state.key().stringValue());
        assertNotNull(state.aggregations().get("city"));
        assertNotNull(state.aggregations().get("city").sterms());
        assertNotNull(state.aggregations().get("city").sterms().buckets());
        assertEquals(1, state.aggregations().get("city").sterms().buckets().array().size());
        final StringTermsBucket city = state.aggregations().get("city").sterms().buckets().array().get(0);
        assertEquals("paris", city.key().stringValue());
    }

    @Test
    void esql() throws IOException, SQLException {
        final var p1 = new Person();
        p1.setId("1");
        p1.setName("David");
        final var p2 = new Person();
        p2.setId("2");
        p2.setName("Max");
        client.index(ir -> ir.index(indexName).id(p1.getId()).document(p1));
        client.index(ir -> ir.index(indexName).id(p2.getId()).document(p2));
        client.indices().refresh(rr -> rr.index(indexName));

        String query = """
            FROM indexName
            | WHERE name == "David"
            | KEEP name
            | LIMIT 1
            """.replaceFirst("indexName", indexName);

        {
            // Using the Raw ES|QL API
            try (final BinaryResponse response = client.esql().query(q -> q.query(query)); InputStream is = response.content()) {
                // The response object is {
                //  "took" : 4,
                //  "is_partial" : false,
                //  "columns" : [ {
                //    "name" : "name",
                //    "type" : "text"
                //  } ],
                //  "values" : [ [ "David" ] ]
                //}
                final ObjectMapper mapper = new ObjectMapper();
                final JsonNode node = mapper.readTree(is);
                assertNotNull(node);
                assertEquals(4, node.size());
                assertEquals(1, node.get("columns").size());
                assertEquals("name", node.get("columns").get(0).get("name").asText());
                assertEquals(1, node.get("values").size());
                assertEquals("David", node.get("values").get(0).get(0).asText());
                assertTrue(node.get("took").asInt() > 0);
                assertFalse(node.get("is_partial").asBoolean());
            }
        }

        {
            // Using the JDBC ResultSet ES|QL API
            try (final ResultSet resultSet = client.esql().query(ResultSetEsqlAdapter.INSTANCE, query)) {
                assertTrue(resultSet.next());
                assertEquals("David", resultSet.getString(1));
            } catch (final JsonpMappingException e) {
                // This is expected as we have this issue https://github.com/elastic/elasticsearch-java/pull/903
            }
        }

        {
            // Using the Object ES|QL API
            final Iterable<Person> persons = client.esql().query(ObjectsEsqlAdapter.of(Person.class), query);
            for (final Person person : persons) {
                assertNull(person.getId());
                assertNotNull(person.getName());
            }
        }

        {
            // Using named parameters
            String parametrizedQuery = """
            FROM indexName
            | WHERE name == ?name
            | KEEP name
            | LIMIT 1
            """.replaceFirst("indexName", indexName);

            // Using the Object ES|QL API
            final Iterable<Person> persons = client.esql()
                    .query(ObjectsEsqlAdapter.of(Person.class), parametrizedQuery,
                            Map.of("name", "David")
                    );
            for (final Person person : persons) {
                assertNull(person.getId());
                assertNotNull(person.getName());
            }
        }
    }

    /**
     * This one is failing for now. So we are expecting a failure.
     * When updating to 8.15.1, it should fix it.
     */
    @Test
    void callHotThreads() {
        assertThrows(TransportException.class, () -> client.nodes().hotThreads());
    }

    @Test
    void withAliases() throws IOException {
        setAndRemoveIndex(indexName + "-v2");
        assertTrue(client.indices().create(cir -> cir.index(indexName)
                .aliases(indexName + "_alias", a -> a)).acknowledged());
        assertTrue(client.indices().create(cir -> cir.index(indexName + "-v2")).acknowledged());

        // Check the alias existence by its name
        assertTrue(client.indices().existsAlias(ga -> ga.name(indexName + "_alias")).value());

        // Check we have one alias on indexName
        assertEquals(1, client.indices().getAlias(ga -> ga.index(indexName)).aliases().get(indexName).aliases().size());
        // Check we have no alias on indexName-v2
        assertEquals(0, client.indices().getAlias(ga -> ga.index(indexName + "-v2")).aliases().get(indexName + "-v2").aliases().size());

        // Switch the alias indexName_alias from indexName to indexName-v2
        client.indices().updateAliases(ua -> ua
                .actions(a -> a.add(aa -> aa.alias(indexName + "_alias").index(indexName + "-v2")))
                .actions(a -> a.remove(ra -> ra.alias(indexName + "_alias").index(indexName)))
        );

        // Check we have no alias on indexName
        assertEquals(0, client.indices().getAlias(ga -> ga.index(indexName)).aliases().get(indexName).aliases().size());
        // Check we have one alias on indexName-v2
        assertEquals(1, client.indices().getAlias(ga -> ga.index(indexName + "-v2")).aliases().get(indexName + "-v2").aliases().size());

        // Check the alias existence by its name
        assertTrue(client.indices().existsAlias(ga -> ga.name(indexName + "_alias")).value());

        // Delete the alias
        client.indices().deleteAlias(da -> da.name(indexName + "_alias").index("*"));

        // Check the alias non-existence by its name
        assertFalse(client.indices().existsAlias(ga -> ga.name(indexName + "_alias")).value());
    }

    @Test
    void kNNWithFunctionScore() throws IOException {
        client.indices().create(cir -> cir.index(indexName).mappings(m -> m
                .properties("vector", p -> p.denseVector(dv -> dv))
                .properties("country", p -> p.keyword(k -> k))
        ));
        client.index(ir -> ir.index(indexName).withJson(new StringReader("{\"country\":\"france\", \"vector\":[1.0, 0.4, 0.8]}")));
        client.indices().refresh(rr -> rr.index(indexName));
        final SearchResponse<Void> response = client.search(sr -> sr
                .index(indexName)
                .query(q -> q.functionScore(
                        fsq -> fsq
                                .query(qknn -> qknn.knn(
                                        k -> k.field("vector").queryVector(0.9f, 0.4f, 0.8f)
                                ))
                                .functions(fs -> fs.randomScore(rs -> rs.field("country").seed("hello")))
                ))
        , Void.class);

        assumeNotNull(response.hits().total());
        assertEquals(1, response.hits().total().value());
        assertEquals(0.4063275, response.hits().hits().get(0).score());
    }

    @Test
    void boolQuery() throws IOException {
        client.index(ir -> ir.index(indexName).id("1").withJson(new StringReader("""
                {
                    "number":1,
                    "effective_date":"2024-10-01T00:00:00.000Z"
                }""")));
        client.index(ir -> ir.index(indexName).id("2").withJson(new StringReader("""
                {
                    "number":2,
                    "effective_date":"2024-10-02T00:00:00.000Z"
                }""")));
        client.index(ir -> ir.index(indexName).id("3").withJson(new StringReader("""
                {
                    "number":3,
                    "effective_date":"2024-10-03T00:00:00.000Z"
                }""")));
        client.index(ir -> ir.index(indexName).id("4").withJson(new StringReader("""
                {
                    "number":4,
                    "effective_date":"2024-10-04T00:00:00.000Z"
                }""")));
        client.indices().refresh(rr -> rr.index(indexName));
        final SearchResponse<Void> response = client.search(sr -> sr
                        .index(indexName)
                        .query(q -> q.bool(bq -> bq
                                .filter(fq -> fq.terms(tq -> tq.field("number")
                                        .terms(t -> t.value(List.of(
                                                FieldValue.of("2"),
                                                FieldValue.of("3"))))))
                                .filter(fq -> fq
                                        .range(rq -> rq.date(drq -> drq
                                                .field("effective_date")
                                                .gte("2024-10-03T00:00:00.000Z"))))
                        ))
                , Void.class);
        assertNotNull(response.hits().total());
        assertEquals(1, response.hits().total().value());
        assertEquals("3", response.hits().hits().get(0).id());
    }

    /**
     * This method adds the index name we want to use to the list
     * and deletes the index if it exists.
     * @param name the index name
     */
    private void setAndRemoveIndex(final String name) {
        indices.add(name);
        removeIndex(name);
    }

    /**
     * This method deletes the index if it exists.
     * @param name the index name
     */
    private void removeIndex(final String name) {
        try {
            client.indices().delete(dir -> dir.index(name));
            logger.debug("Index [{}] has been removed", name);
        } catch (final IOException | ElasticsearchException ignored) { }
    }
}

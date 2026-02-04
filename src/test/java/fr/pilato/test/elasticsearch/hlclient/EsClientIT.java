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
import co.elastic.clients.elasticsearch.cat.IndicesResponse;
import co.elastic.clients.elasticsearch.cat.ShardsResponse;
import co.elastic.clients.elasticsearch.cat.ThreadPoolResponse;
import co.elastic.clients.elasticsearch.cluster.PutComponentTemplateResponse;
import co.elastic.clients.elasticsearch.core.*;
import co.elastic.clients.elasticsearch.core.search.HighlightField;
import co.elastic.clients.elasticsearch.ilm.PutLifecycleResponse;
import co.elastic.clients.elasticsearch.indices.*;
import co.elastic.clients.elasticsearch.ingest.PutPipelineResponse;
import co.elastic.clients.elasticsearch.ingest.SimulateResponse;
import co.elastic.clients.elasticsearch.sql.TranslateResponse;
import co.elastic.clients.elasticsearch.transform.GetTransformResponse;
import co.elastic.clients.elasticsearch.transform.PutTransformResponse;
import co.elastic.clients.json.JsonData;
import co.elastic.clients.transport.endpoints.BinaryResponse;
import co.elastic.clients.transport.endpoints.TextResponse;
import co.elastic.clients.util.BinaryData;
import co.elastic.clients.util.ContentType;
import co.elastic.clients.util.NamedValue;
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
import static org.assertj.core.api.Assertions.*;
import static org.junit.Assume.assumeNotNull;

class EsClientIT {

    private static final Logger logger = LogManager.getLogger();
    private static ElasticsearchClient client = null;
    private static ElasticsearchAsyncClient asyncClient = null;
    private static final String PASSWORD = "changeme";
    private static final String PREFIX = "esclientit_";
    private static String elasticsearchVersion;

    @BeforeAll
    static void startElasticsearchContainer() throws IOException {
        final var props = new Properties();
        props.load(EsClientIT.class.getResourceAsStream("/version.properties"));
        elasticsearchVersion = props.getProperty("elasticsearch.version");
        logger.info("Starting testcontainers with Elasticsearch {}.", props.getProperty("elasticsearch.version"));
        // Start the container. This step might take some time...
        final ElasticsearchContainer container = new ElasticsearchContainer(
                DockerImageName.parse("docker.elastic.co/elasticsearch/elasticsearch")
                        .withTag(elasticsearchVersion))
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

    private static ElasticsearchClient getClient(final String elasticsearchServiceAddress, final byte[] certificate) throws Exception {
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

    private static ElasticsearchAsyncClient getAsyncClient(final String elasticsearchServiceAddress, final byte[] certificate) throws Exception {
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

    List<String> elasticsearchCreatedIndices;
    String indexName;
    
    @BeforeEach
    void cleanIndexBeforeRun(final TestInfo testInfo) {
        elasticsearchCreatedIndices = new ArrayList<>();
        final var methodName = testInfo.getTestMethod().orElseThrow().getName();
        indexName = PREFIX + methodName.toLowerCase(Locale.ROOT);

        logger.debug("Using [{}] as the index name", indexName);
        setAndRemoveIndex(indexName);
    }

    @AfterEach
    void cleanIndexAfterRun() {
        elasticsearchCreatedIndices.forEach(this::removeIndex);
    }

    @Test
    void getDocument() throws IOException {
        client.index(ir -> ir.index(indexName).id("1")
                .withJson(new StringReader("{\"foo\":\"bar\", \"application_id\": 6}")));
        {
            final GetResponse<ObjectNode> getResponse = client.get(gr -> gr.index(indexName).id("1"), ObjectNode.class);
            assertThat(getResponse.source()).hasToString("{\"foo\":\"bar\",\"application_id\":6}");
        }
        {
            // With source filtering
            final GetResponse<ObjectNode> getResponse = client.get(gr -> gr.index(indexName).id("1").sourceIncludes("application_id"), ObjectNode.class);
            assertThat(getResponse.source()).hasToString("{\"application_id\":6}");
        }
        {
            // Get as Map
            final GetResponse<ObjectNode> getResponse = client.get(gr -> gr.index(indexName).id("1"), ObjectNode.class);
            final ObjectMapper mapper = new ObjectMapper();
            final Map<String, Object> result = mapper.convertValue(getResponse.source(), new TypeReference<>() {});
            assertThat(result)
                    .contains(entry("foo", "bar"))
                    .contains(entry("application_id", 6));
        }
    }

    @Test
    void exists() throws IOException {
        client.index(ir -> ir.index(indexName).id("1")
                .withJson(new StringReader("{\"foo\":\"bar\"}")));
        assertThat(client.exists(gr -> gr.index(indexName).id("1")).value()).isTrue();
        assertThat(client.exists(gr -> gr.index(indexName).id("2")).value()).isFalse();
    }

    @Test
    void createIndex() throws IOException {
        final CreateIndexResponse response = client.indices().create(cir -> cir.index(indexName)
                .mappings(m -> m.properties("content", p -> p.text(tp -> tp))));
        assertThat(response.acknowledged()).isTrue();
    }

    @Test
    void callInfo() throws IOException {
        final InfoResponse info = client.info();
        final String version = info.version().number();
        assertThat(version).isNotBlank();
        assertThat(info.clusterName()).isNotBlank();
        assertThat(info.tagline()).isEqualTo("You Know, for Search");
    }

    @Test
    void createMapping() throws IOException {
        client.indices().create(cir -> cir.index(indexName));
        final PutMappingResponse response = client.indices().putMapping(pmr -> pmr.index(indexName)
                .properties("foo", p -> p.text(tp -> tp)));
        assertThat(response.acknowledged()).isTrue();
    }

    @Test
    void createData() throws IOException {
        final IndexResponse indexResponse = client.index(ir -> ir.index(indexName).id("1")
                .withJson(new StringReader("{\"foo\":\"bar\"}")));
        assertThat(indexResponse.result()).isEqualTo(Result.Created);
        client.indices().refresh(rr -> rr.index(indexName));
        final SearchResponse<Void> response = client.search(sr -> sr.index(indexName), Void.class);
        assertThat(response.hits().total()).isNotNull();
        assertThat(response.hits().total().value()).isEqualTo(1);
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
            assertThat(response.hits().total()).isNotNull();
            assertThat(response.hits().total().value()).isEqualTo(1);
            assertThat(response.hits().hits().get(0).id()).isEqualTo("1");
        }
        {
            final SearchResponse<Void> response = client.search(sr -> sr
                            .index(indexName)
                            .query(q -> q.term(tq -> tq.field("foo").value("bar"))),
                    Void.class);
            assertThat(response.hits().total()).isNotNull();
            assertThat(response.hits().total().value()).isEqualTo(1);
            assertThat(response.hits().hits().get(0).id()).isEqualTo("1");
        }
        {
            final String matchAllQuery = Base64.getEncoder().encodeToString("{\"match_all\":{}}".getBytes(StandardCharsets.UTF_8));
            final SearchResponse<Void> response = client.search(sr -> sr
                            .index(indexName)
                            .query(q -> q.wrapper(wq -> wq.query(matchAllQuery))),
                    Void.class);
            assertThat(response.hits().total()).isNotNull();
            assertThat(response.hits().total().value()).isEqualTo(1);
            assertThat(response.hits().hits().get(0).id()).isEqualTo("1");
        }
        {
            final SearchResponse<Void>  response = client.search(sr -> sr
                            .index(indexName)
                            .query(q -> q.matchAll(maq -> maq))
                            .trackScores(true),
                    Void.class);
            assertThat(response.hits().total()).isNotNull();
            assertThat(response.hits().total().value()).isEqualTo(1);
            assertThat(response.hits().hits().get(0).id()).isEqualTo("1");
        }
    }

    @Test
    void translateSqlQuery() throws IOException {
        client.index(ir -> ir.index(indexName).id("1")
                .withJson(new StringReader("{\"foo\":\"bar\"}")));
        client.indices().refresh(rr -> rr.index(indexName));

        final TranslateResponse translateResponse = client.sql().translate(tr -> tr
                .query("SELECT * FROM " + indexName + " WHERE foo='bar' limit 10"));
        assertThat(translateResponse.query()).isNotNull();
        assertThat(translateResponse.size())
                .isNotNull()
                .isEqualTo(10);

        final SearchResponse<Void> response = client.search(sr -> sr
                        .index(indexName)
                        .query(translateResponse.query())
                        .size(translateResponse.size().intValue()),
                Void.class);
        assertThat(response.hits().total()).isNotNull();
        assertThat(response.hits().total().value()).isEqualTo(1);
        assertThat(response.hits().hits().get(0).id()).isEqualTo("1");
    }

    @Test
    void transformApi() throws IOException {
        final var id = "test-get";
        try {
            client.transform().deleteTransform(dtr -> dtr.transformId(id));
        } catch (ElasticsearchException ignored) { /* Might throw a 404 which we don't care about */ }
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
        assertThat(putTransformResponse.acknowledged()).isTrue();

        final GetTransformResponse getTransformResponse = client.transform().getTransform(gt -> gt.transformId(id));
        assertThat(getTransformResponse.count()).isEqualTo(1);
    }

    @Test
    void highlight() throws IOException {
        client.index(ir -> ir.index(indexName)
                .withJson(new StringReader("{\"foo\":\"bar baz\"}")));
        client.indices().refresh(rr -> rr.index(indexName));
        final SearchResponse<Void> response = client.search(sr -> sr
                        .index(indexName)
                        .query(q -> q.match(mq -> mq.field("foo").query("bar")))
                        .highlight(h -> h
                                .fields(NamedValue.of("foo", HighlightField.of(hf -> hf.maxAnalyzedOffset(10)))))
                , Void.class);
        assertThat(response.hits().total()).isNotNull();
        assertThat(response.hits().total().value()).isEqualTo(1);
        assertThat(response.hits().hits().get(0).highlight())
                .isNotNull()
                .containsExactly(entry("foo", Collections.singletonList("<em>bar</em> baz")));
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
        assertThat(response.aggregations())
                .isNotNull()
                .containsKey("top10foo");
        assertThat(response.aggregations().get("top10foo").sterms()).isNotNull();
        assertThat(response.aggregations().get("top10foo").sterms().buckets()).isNotNull();
        assertThat(response.aggregations().get("top10foo").sterms().buckets().array())
                .hasSize(1)
                .allSatisfy(bucket -> {
            assertThat(bucket.key()).isNotNull();
            assertThat(bucket.key().stringValue()).isEqualTo("bar");
            assertThat(bucket.docCount()).isEqualTo(2);
        });
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
        assertThat(response.hits().total()).isNotNull();
        assertThat(response.hits().total().value()).isEqualTo(size);
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
            assertThat(response.hits().total()).isNotNull();
            // We can not assert "isEqualTo(size)" as the flush might not send the last batch
            assertThat(response.hits().total().value()).isLessThanOrEqualTo(size);
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
        assertThat(response.hits().total()).isNotNull();
        assertThat(response.hits().total().value()).isEqualTo(1);
        assertThat(response.hits().hits().get(0).id()).isEqualTo("1");
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
            assertThat(response.items())
                    .filteredOn(item -> item.error() != null)
                    .allSatisfy(item -> {
                        assertThat(item.id()).isNotNull();
                        assertThat(item.error()).isNotNull();
                        assertThat(item.error().reason()).isNotNull();
                        logger.trace("Error {} for id {}", item.error().reason(), item.id());
                    });
        }

        client.indices().refresh(rr -> rr.index(indexName));
        final SearchResponse<Void> searchResponse = client.search(sr -> sr.index(indexName), Void.class);
        assertThat(searchResponse.hits().total()).isNotNull();
        assertThat(searchResponse.hits().total().value()).isEqualTo(goodData.get());
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
        assertThat(response.hits()).isNotNull();
        assertThat(response.hits().hits()).allSatisfy(hit -> {
            assertThat(hit.id()).isNotNull();
            assertThat(hit.source()).isNotNull();
            assertThat(hit.source().getId()).isEqualTo(hit.id());
            assertThat(hit.source().getName()).isNotNull();
        });
    }

    @Test
    void reindex() throws IOException {
        // Check the error is thrown when the source index does not exist
        assertThatThrownBy(() -> client.reindex(rr -> rr
                .source(s -> s.index(PREFIX + "does-not-exists")).dest(d -> d.index("foo"))))
                .isInstanceOfSatisfying(ElasticsearchException.class, e -> assertThat(e.status()).isEqualTo(404));

        // A regular reindex operation
        setAndRemoveIndex(indexName + "-dest");

        client.index(ir -> ir.index(indexName).id("1").withJson(new StringReader("{\"foo\":1}")));
        client.indices().refresh(rr -> rr.index(indexName));
        final ReindexResponse reindexResponse = client.reindex(rr -> rr
                .source(s -> s.index(indexName)).dest(d -> d.index(indexName + "-dest")));
        assertThat(reindexResponse.total()).isEqualTo(1);
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

        assertThat(response.hits().total()).isNotNull();
        assertThat(response.hits().total().value()).isEqualTo(2);
        assertThat(response.hits().hits()).satisfiesExactly(hit1 -> {
            assertThat(hit1.id()).isEqualTo("1");
            assertThat(hit1.sort()).hasSize(1);
            assertThat(hit1.sort().get(0).doubleValue()).isEqualTo(0.0);
        }, hit2 -> {
            assertThat(hit2.id()).isEqualTo("2");
            assertThat(hit2.sort()).hasSize(1);
            assertThat(hit2.sort().get(0).doubleValue()).isEqualTo(8187.4318605250455);
        });
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

        assertThat(response.hits().total()).isNotNull();
        assertThat(response.hits().total().value()).isEqualTo(1);
        assertThat(response.hits().hits()).satisfiesExactly(hit -> assertThat(hit.id()).isEqualTo("1"));
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
        assertThat(future.get()).isNull();
        assertThat(timeoutException.get()).isTrue();

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
        assertThat(timeoutException.get()).isFalse();
        assertThat(response.hits().total()).isNotNull();
        assertThat(response.hits().total().value()).isEqualTo(1);
    }

    @Test
    void catApi() throws IOException {
        final ThreadPoolResponse threadPool = client.cat().threadPool();
        assertThat(threadPool).isNotNull();
        assertThat(threadPool.threadPools()).allSatisfy(threadPoolRecord -> {
            assertThat(threadPoolRecord.nodeName()).isNotNull();
            assertThat(threadPoolRecord.name()).isNotNull();
            assertThat(threadPoolRecord.active()).isNotNull();
            assertThat(threadPoolRecord.queue()).isNotNull();
            assertThat(threadPoolRecord.rejected()).isNotNull();
        });
        final IndicesResponse indices = client.cat().indices();
        assertThat(indices).isNotNull();
        assertThat(indices.indices()).allSatisfy(indicesRecord -> {
            assertThat(indicesRecord.index()).isNotNull();
            assertThat(indicesRecord.docsCount()).isNotNull();
            assertThat(indicesRecord.docsDeleted()).isNotNull();
        });
        final ShardsResponse shards = client.cat().shards();
        assertThat(shards).isNotNull();
        assertThat(shards.shards()).allSatisfy(shardsRecord -> {
            assertThat(shardsRecord.index()).isNotNull();
            assertThat(shardsRecord.state()).isIn("STARTED", "UNASSIGNED");
            assertThat(shardsRecord.prirep()).isIn("p", "r");
        });
    }

    @Test
    void ingestPipelines() throws IOException {
        // Define some pipelines
        try {
            client.ingest().deletePipeline(pr -> pr.id("my-pipeline"));
        } catch (final ElasticsearchException ignored) { /* Might throw a 404 which we don't care about */ }
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
            assertThat(response.acknowledged()).isTrue();
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
            assertThat(response.acknowledged()).isTrue();
        }
        {
            final SimulateResponse response = client.ingest().simulate(sir -> sir
                    .id("my-pipeline")
                    .docs(d -> d
                            .source(JsonData.fromJson("{\"foo\":\"baz\"}"))
                    )
            );
            assertThat(response.docs())
                    .hasSize(1)
                    .allSatisfy(doc -> {
                        assertThat(doc.doc()).isNotNull();
                        assertThat(doc.doc().source()).isNotNull();
                        assertThat(doc.doc().source()).allSatisfy((key, value) -> {
                            assertThat(key).isEqualTo("foo");
                            assertThat(value).satisfies(jsonData -> assertThat(jsonData.to(String.class)).isEqualTo("bar"));
                        });
                    });
        }
    }

    @Test
    void sourceRequest() throws IOException {
        client.index(ir -> ir.index(indexName).id("1").withJson(new StringReader("{\"foo\":\"bar\"}")));
        client.indices().refresh(rr -> rr.index(indexName));
        final GetSourceResponse<ObjectNode> source = client.getSource(gsr -> gsr.index(indexName).id("1"), ObjectNode.class);
        assertThat(source.source())
                .isNotNull()
                .satisfies(jsonData -> assertThat(jsonData).hasToString("{\"foo\":\"bar\"}"));
    }

    @Test
    void deleteByQuery() throws IOException {
        client.index(ir -> ir.index(indexName).id("1").withJson(new StringReader("{\"foo\":\"bar\"}")));
        client.indices().refresh(rr -> rr.index(indexName));
        final SearchResponse<Void> response1 = client.search(sr -> sr.index(indexName), Void.class);
        assertThat(response1.hits().total()).isNotNull();
        assertThat(response1.hits().total().value()).isEqualTo(1);
        final DeleteByQueryResponse deleteByQueryResponse = client.deleteByQuery(dbq -> dbq
                .index(indexName)
                .query(q -> q
                        .match(mq -> mq
                                .field("foo")
                                .query("bar"))));
        assertThat(deleteByQueryResponse.deleted()).isEqualTo(1);
        client.indices().refresh(rr -> rr.index(indexName));
        final SearchResponse<Void> response2 = client.search(sr -> sr.index(indexName), Void.class);
        assertThat(response2.hits().total()).isNotNull();
        assertThat(response2.hits().total().value()).isZero();
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
        assertThat(response.source())
                .isNotNull()
                .satisfies(o -> assertThat(o).hasToString("{\"show_count\":1}"));
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
            assertThat(response.acknowledged()).isTrue();
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
            assertThat(response.acknowledged()).isTrue();
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
        assertThat(response.acknowledged()).isTrue();
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
        assertThatThrownBy(() -> {
            // Search
            client.search(sr -> sr
                    .index(indexName)
                    .query(q -> q.sparseVector(sv -> sv
                            .field("content_embedding")
                            .inferenceId("elser-v2-test")
                            .query("How to avoid muscle soreness after running?")
                    )), ObjectNode.class);
        })
                .withFailMessage("We are expecting an exception as the model is not deployed")
                .isInstanceOfSatisfying(ElasticsearchException.class, exception -> {
                    assertThat(exception.error().reason()).isEqualTo("[elser-v2-test] is not an inference service model or a deployed ml model");
                    assertThat(exception.status()).isEqualTo(404);
                });
    }

    @Test
    void testIlm() throws IOException {
        try {
            client.ilm().deleteLifecycle(dlr -> dlr.name(indexName + "-ilm"));
        } catch (IOException | ElasticsearchException ignored) { /* Might throw a 404 which we don't care about */ }
        PutLifecycleResponse response = client.ilm().putLifecycle(plr -> plr
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
        assertThat(response.acknowledged()).isTrue();
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
        assertThat(response.hits().total()).isNotNull();
        assertThat(response.hits().total().value()).isEqualTo(1);
        assertThat(response.hits().hits()).satisfiesExactly(hit -> assertThat(hit.id()).isEqualTo("2"));
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

        assertThat(response.aggregations())
                .isNotNull()
                .hasEntrySatisfying("country", countries -> {
                    assertThat(countries.sterms()).isNotNull();
                    assertThat(countries.sterms().buckets()).isNotNull();
                    assertThat(countries.sterms().buckets().array())
                            .hasSize(3)
                            .anySatisfy(country -> {
                                assertThat(country.key()).isNotNull();
                                assertThat(country.key().stringValue()).isEqualTo("france");
                                assertThat(country.docCount()).isEqualTo(1);
                                assertThat(country.aggregations())
                                        .hasEntrySatisfying("state", state -> {
                                            assertThat(state.sterms()).isNotNull();
                                            assertThat(state.sterms().buckets()).isNotNull();
                                            assertThat(state.sterms().buckets().array())
                                                    .hasSize(1)
                                                    .satisfiesExactly(stateBucket -> {
                                                        assertThat(stateBucket.key()).isNotNull();
                                                        assertThat(stateBucket.key().stringValue()).isEqualTo("paris");
                                                        assertThat(stateBucket.docCount()).isEqualTo(1);
                                                        assertThat(stateBucket.aggregations())
                                                                .containsKey("city")
                                                                .hasEntrySatisfying("city", city -> {
                                                                    assertThat(city.sterms()).isNotNull();
                                                                    assertThat(city.sterms().buckets()).isNotNull();
                                                                    assertThat(city.sterms().buckets().array())
                                                                            .hasSize(1)
                                                                            .satisfiesExactly(cityBucket -> {
                                                                                assertThat(cityBucket.key()).isNotNull();
                                                                                assertThat(cityBucket.key().stringValue()).isEqualTo("paris");
                                                                                assertThat(cityBucket.docCount()).isEqualTo(1);
                                                                            });
                                                                });
                                                    });
                                        });
                            });
                });
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
                // The response object is {"took":221,"is_partial":false,"completion_time_in_millis":1770203889851,"documents_found":1,"values_loaded":1,"start_time_in_millis":1770203889630,"expiration_time_in_millis":1770635889773,"columns":[{"name":"name","type":"text"}],"values":[["David"]]}
                final ObjectMapper mapper = new ObjectMapper();
                final JsonNode jsonNode = mapper.readTree(is);
                assertThat(jsonNode).isNotNull().hasSize(9);
                assertThat(jsonNode.get("columns")).isNotNull().hasSize(1).first().satisfies(column -> assertThat(column.get("name").asText()).isEqualTo("name"));
                assertThat(jsonNode.get("values")).isNotNull().hasSize(1).first().satisfies(value -> assertThat(value).hasSize(1).first().satisfies(singleValue -> assertThat(singleValue.asText()).isEqualTo("David")));
                assertThat(jsonNode.get("took").asInt()).isGreaterThan(0);
                assertThat(jsonNode.get("is_partial").asBoolean()).isFalse();
                assertThat(jsonNode.get("documents_found").asLong()).isEqualTo(1);
                assertThat(jsonNode.get("values_loaded").asLong()).isEqualTo(1);
                // Added in 9.3.0
                assertThat(jsonNode.get("completion_time_in_millis").asLong()).isGreaterThan(0);
                assertThat(jsonNode.get("start_time_in_millis").asLong()).isGreaterThan(0);
                assertThat(jsonNode.get("expiration_time_in_millis").asLong()).isGreaterThan(0);
            }
        }

        {
            // Using the JDBC ResultSet ES|QL API
            try (final ResultSet resultSet = client.esql().query(ResultSetEsqlAdapter.INSTANCE, query)) {
                assertThat(resultSet).isNotNull().satisfies(resultSetResult -> {
                    assertThat(resultSetResult.next()).isTrue();
                    assertThat(resultSetResult.getString("name")).isEqualTo("David");
                });
            }
        }

        {
            // Using the Object ES|QL API
            final Iterable<Person> persons = client.esql().query(ObjectsEsqlAdapter.of(Person.class), query);
            for (final Person person : persons) {
                assertThat(person.getId()).isNull();
                assertThat(person.getName()).isNotNull();
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
                assertThat(person.getId()).isNull();
                assertThat(person.getName()).isNotNull();
            }
        }
    }

    @Test
    void callHotThreads() throws IOException {
        TextResponse textResponse = client.nodes().hotThreads();
        assertThat(textResponse).isNotNull();
        /* Response is something like:
        ::: {71f9f45241a7}{pyRlgV_ATriPB3F6mE9p0w}{ZgfiKVLXQfGe8fWK-tL6LQ}{71f9f45241a7}{172.17.0.5}{172.17.0.5:9300}{cdfhilmrstw}{9.2.4}{8000099-9039003}{ml.allocated_processors=12, ml.allocated_processors_double=12.0, ml.max_jvm_size=2147483648, ml.config_version=12.0.0, xpack.installed=true, transform.config_version=10.0.0, ml.machine_memory=16748077056}
           Hot threads at 2026-02-04T11:56:58.259Z, interval=500ms, busiestThreads=3, ignoreIdleThreads=true:
         */
        assertThat(textResponse.value())
                .contains("Hot threads")
                .contains(elasticsearchVersion);
    }

    @Test
    void withAliases() throws IOException {
        setAndRemoveIndex(indexName + "-v2");
        assertThat(client.indices().create(cir -> cir.index(indexName)
                .aliases(indexName + "_alias", a -> a)).acknowledged()).isTrue();
        assertThat(client.indices().create(cir -> cir.index(indexName + "-v2")).acknowledged()).isTrue();

        // Check the alias existence by its name
        assertThat(client.indices().existsAlias(ga -> ga.name(indexName + "_alias")).value()).isTrue();

        // Check we have one alias on indexName
        assertThat(client.indices().getAlias(ga -> ga.index(indexName)).aliases().get(indexName).aliases()).hasSize(1);
        // Check we have no alias on indexName-v2
        assertThat(client.indices().getAlias(ga -> ga.index(indexName + "-v2")).aliases().get(indexName + "-v2").aliases()).isEmpty();

        // Switch the alias indexName_alias from indexName to indexName-v2
        client.indices().updateAliases(ua -> ua
                .actions(a -> a.add(aa -> aa.alias(indexName + "_alias").index(indexName + "-v2")))
                .actions(a -> a.remove(ra -> ra.alias(indexName + "_alias").index(indexName)))
        );

        // Check we have no alias on indexName
        assertThat(client.indices().getAlias(ga -> ga.index(indexName)).aliases().get(indexName).aliases()).isEmpty();
        // Check we have one alias on indexName-v2
        assertThat(client.indices().getAlias(ga -> ga.index(indexName + "-v2")).aliases().get(indexName + "-v2").aliases()).hasSize(1);

        // Check the alias existence by its name
        assertThat(client.indices().existsAlias(ga -> ga.name(indexName + "_alias")).value()).isTrue();

        // Delete the alias
        client.indices().deleteAlias(da -> da.name(indexName + "_alias").index("*"));

        // Check the alias non-existence by its name
        assertThat(client.indices().existsAlias(ga -> ga.name(indexName + "_alias")).value()).isFalse();
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
        assertThat(response.hits().total().value()).isEqualTo(1);
        assertThat(response.hits().hits().get(0).score()).isEqualTo(0.4063275);
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
        assertThat(response.hits().total()).isNotNull();
        assertThat(response.hits().total().value()).isEqualTo(1);
        assertThat(response.hits().hits()).hasSize(1);
        assertThat(response.hits().hits().get(0).id()).isEqualTo("3");
    }

    /**
     * This method adds the index name we want to use to the list
     * and deletes the index if it exists.
     * @param name the index name
     */
    private void setAndRemoveIndex(final String name) {
        elasticsearchCreatedIndices.add(name);
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
        } catch (final IOException | ElasticsearchException ignored) { /* Might throw a 404 which we don't care about */ }
    }
}

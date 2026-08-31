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
import com.carrotsearch.randomizedtesting.jupiter.Randomized;
import com.carrotsearch.randomizedtesting.jupiter.generators.RandomNumbers;
import com.carrotsearch.randomizedtesting.jupiter.generators.RandomStrings;
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

import static fr.pilato.test.elasticsearch.hlclient.SSLUtils.createContextFromCaCert;
import static fr.pilato.test.elasticsearch.hlclient.SSLUtils.createTrustAllCertsContext;
import static org.assertj.core.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

@Randomized
class EsClientIT {

    private static final Logger logger = LogManager.getLogger();
    private static ElasticsearchClient client = null;
    private static ElasticsearchAsyncClient asyncClient = null;
    private static final String PASSWORD = "changeme";
    private static final String PREFIX = "esclientit_";
    private static String elasticsearchVersion;
    private static String apiKey;

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
        try (ElasticsearchClient bootstrap = ElasticsearchClient.of(b -> b
                .host("https://" + container.getHttpHostAddress())
                .sslContext(createContextFromCaCert(certAsBytes))
                .usernameAndPassword("elastic", PASSWORD))) {
            apiKey = bootstrap.security().createApiKey(k -> k.name("test-key")).encoded();
            logger.info("Generated API key: {}.", apiKey);
        }

        try {
            client = getClient("https://" + container.getHttpHostAddress(), certAsBytes);
            asyncClient = getAsyncClient("https://" + container.getHttpHostAddress(), certAsBytes);
        } catch (Exception e) {
            logger.debug("No cluster is running yet at https://{}.", container.getHttpHostAddress());
        }

        assumeTrue(client != null);
        assumeTrue(asyncClient != null);
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
                .apiKey(apiKey)
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
                .apiKey(apiKey)
        );
        final InfoResponse info = client.info().get();
        logger.info("Async Client connected to a cluster running version {} at {}.", info.version().number(), elasticsearchServiceAddress);
        return client;
    }

    List<String> elasticsearchCreatedIndices;
    String indexName;
    Random random;

    @BeforeEach
    void cleanIndexBeforeRun(final TestInfo testInfo, final Random random) {
        this.random = random;
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
        final String id = randomId();
        final int applicationId = randomInt(1, 100);
        final String foo = randomToken();
        final String json = String.format(Locale.ROOT, "{\"foo\":\"%s\", \"application_id\": %d}", foo, applicationId);

        client.index(ir -> ir.index(indexName).id(id).withJson(new StringReader(json)));
        {
            final GetResponse<ObjectNode> getResponse = client.get(gr -> gr.index(indexName).id(id), ObjectNode.class);
            assertThat(getResponse.source()).hasToString("{\"foo\":\"" + foo + "\",\"application_id\":" + applicationId + "}");
        }
        {
            // With source filtering
            final GetResponse<ObjectNode> getResponse = client.get(gr -> gr.index(indexName).id(id).sourceIncludes("application_id"), ObjectNode.class);
            assertThat(getResponse.source()).hasToString("{\"application_id\":" + applicationId + "}");
            assertThat(getResponse.source()).doesNotHaveToString("{\"foo\":\"" + foo + "\"}");
        }
        {
            // Get as Map
            final GetResponse<ObjectNode> getResponse = client.get(gr -> gr.index(indexName).id(id), ObjectNode.class);
            final ObjectMapper mapper = new ObjectMapper();
            final Map<String, Object> result = mapper.convertValue(getResponse.source(), new TypeReference<>() {});
            assertThat(result)
                    .contains(entry("foo", foo))
                    .contains(entry("application_id", applicationId));
        }
    }

    @Test
    void exists() throws IOException {
        final String id = randomId();
        final String missingId = randomIdDistinctFrom(id);
        final String foo = randomToken();
        client.index(ir -> ir.index(indexName).id(id)
                .withJson(new StringReader("{\"foo\":\"" + foo + "\"}")));
        assertThat(client.exists(gr -> gr.index(indexName).id(id)).value()).isTrue();
        assertThat(client.exists(gr -> gr.index(indexName).id(missingId)).value()).isFalse();
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
        final String id = randomId();
        final String foo = randomToken();
        final IndexResponse indexResponse = client.index(ir -> ir.index(indexName).id(id)
                .withJson(new StringReader("{\"foo\":\"" + foo + "\"}")));
        assertThat(indexResponse.result()).isEqualTo(Result.Created);
        client.indices().refresh(rr -> rr.index(indexName));
        final SearchResponse<Void> response = client.search(sr -> sr.index(indexName), Void.class);
        assertThat(response.hits().total()).isNotNull();
        assertThat(response.hits().total().value()).isEqualTo(1);
    }

    @Test
    void searchData() throws IOException {
        final String id = randomId();
        final String foo = randomToken();
        client.index(ir -> ir.index(indexName).id(id)
                .withJson(new StringReader("{\"foo\":\"" + foo + "\"}")));
        client.indices().refresh(rr -> rr.index(indexName));
        {
            final SearchResponse<Void> response = client.search(sr -> sr
                            .index(indexName)
                            .query(q -> q.match(mq -> mq.field("foo").query(foo))),
                    Void.class);
            assertThat(response.hits().total()).isNotNull();
            assertThat(response.hits().total().value()).isEqualTo(1);
            assertThat(response.hits().hits().get(0).id()).isEqualTo(id);
        }
        {
            final SearchResponse<Void> response = client.search(sr -> sr
                            .index(indexName)
                            .query(q -> q.term(tq -> tq.field("foo").value(foo))),
                    Void.class);
            assertThat(response.hits().total()).isNotNull();
            assertThat(response.hits().total().value()).isEqualTo(1);
            assertThat(response.hits().hits().get(0).id()).isEqualTo(id);
        }
        {
            final String matchAllQuery = Base64.getEncoder().encodeToString("{\"match_all\":{}}".getBytes(StandardCharsets.UTF_8));
            final SearchResponse<Void> response = client.search(sr -> sr
                            .index(indexName)
                            .query(q -> q.wrapper(wq -> wq.query(matchAllQuery))),
                    Void.class);
            assertThat(response.hits().total()).isNotNull();
            assertThat(response.hits().total().value()).isEqualTo(1);
            assertThat(response.hits().hits().get(0).id()).isEqualTo(id);
        }
        {
            final SearchResponse<Void>  response = client.search(sr -> sr
                            .index(indexName)
                            .query(q -> q.matchAll(maq -> maq))
                            .trackScores(true),
                    Void.class);
            assertThat(response.hits().total()).isNotNull();
            assertThat(response.hits().total().value()).isEqualTo(1);
            assertThat(response.hits().hits().get(0).id()).isEqualTo(id);
        }
    }

    @Test
    void translateSqlQuery() throws IOException {
        final String id = randomId();
        final String foo = randomToken();
        final int limit = randomInt(5, 20);
        client.index(ir -> ir.index(indexName).id(id)
                .withJson(new StringReader("{\"foo\":\"" + foo + "\"}")));
        client.indices().refresh(rr -> rr.index(indexName));

        final TranslateResponse translateResponse = client.sql().translate(tr -> tr
                .query("SELECT * FROM " + indexName + " WHERE foo='" + foo + "' limit " + limit));
        assertThat(translateResponse.query()).isNotNull();
        assertThat(translateResponse.size())
                .isNotNull()
                .isEqualTo(limit);

        final SearchResponse<Void> response = client.search(sr -> sr
                        .index(indexName)
                        .query(translateResponse.query())
                        .size(translateResponse.size().intValue()),
                Void.class);
        assertThat(response.hits().total()).isNotNull();
        assertThat(response.hits().total().value()).isEqualTo(1);
        assertThat(response.hits().hits().get(0).id()).isEqualTo(id);
    }

    @Test
    void transformApi() throws IOException {
        final var id = randomResourceName("transform");
        final String destIndex = extraIndex("dest");
        try {
            client.transform().deleteTransform(dtr -> dtr.transformId(id));
        } catch (ElasticsearchException ignored) { /* Might throw a 404 which we don't care about */ }
        client.index(ir -> ir.index(indexName).id(randomId())
                .withJson(new StringReader("{\"foo\":\"" + randomToken() + "\"}")));
        client.indices().refresh(rr -> rr.index(indexName));
        final PutTransformResponse putTransformResponse = client.transform().putTransform(ptr -> ptr
                .transformId(id)
                .source(s -> s.index(indexName).query(q -> q.matchAll(maq -> maq)))
                .dest(d -> d.index(destIndex))
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
        final String foo = randomToken();
        final int size = randomInt(2, 10);
        for (int i = 0; i < size; i++) {
            client.index(ir -> ir.index(indexName)
                    .withJson(new StringReader("{\"foo\":\"" + foo + "\"}")));
        }
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
            assertThat(bucket.key().stringValue()).isEqualTo(foo);
            assertThat(bucket.docCount()).isEqualTo(size);
        });
    }

    @Test
    void bulkIngester() throws IOException {
        final var size = randomInt(20, 200);
        final var maxOperations = randomInt(5, 50);
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
                .maxOperations(maxOperations)
                .maxSize(1_000_000)
                .flushInterval(5, TimeUnit.SECONDS)
        )) {
            final var data = BinaryData.of(("{\"foo\":\"" + randomToken() + "\"}").getBytes(StandardCharsets.UTF_8), ContentType.APPLICATION_JSON);
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
        final var maxOperations = randomInt(100, 1_000);
        final var size = maxOperations * randomInt(2, 5);
        try (final BulkIngester<Void> ingester = BulkIngester.of(b -> b
                .client(client)
                .globalSettings(gs -> gs
                        .index(indexName)
                )
                .maxOperations(maxOperations)
                .flushInterval(5, TimeUnit.SECONDS)
        )) {
            final var data = BinaryData.of(("{\"foo\":\"" + randomToken() + "\"}").getBytes(StandardCharsets.UTF_8), ContentType.APPLICATION_JSON);
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
        final String idLow = randomId();
        final String idHigh = randomIdDistinctFrom(idLow);
        final int low = randomInt(1, 50);
        final int high = low + randomInt(1, 50);
        client.index(ir -> ir.index(indexName).id(idLow).withJson(new StringReader("{\"foo\":" + low + "}")));
        client.index(ir -> ir.index(indexName).id(idHigh).withJson(new StringReader("{\"foo\":" + high + "}")));
        client.indices().refresh(rr -> rr.index(indexName));
        final SearchResponse<ObjectNode> response = client.search(sr -> sr.index(indexName)
                        .query(q -> q.range(rq -> rq
                                .number(nrq -> nrq.field("foo").gte((double) low).lte((double) low))
                        ))
                , ObjectNode.class);
        assertThat(response.hits().total()).isNotNull();
        assertThat(response.hits().total().value()).isEqualTo(1);
        assertThat(response.hits().hits().get(0).id()).isEqualTo(idLow);
    }

    @Test
    void bulk() throws IOException {
        final var size = randomInt(20, 200);
        final var goodData = new AtomicInteger();
        final var foo = randomToken();
        final var data = BinaryData.of(("{\"foo\":\"" + foo + "\"}").getBytes(StandardCharsets.UTF_8), ContentType.APPLICATION_JSON);
        final var wrongData = BinaryData.of(("{\"foo\":\"" + foo).getBytes(StandardCharsets.UTF_8), ContentType.APPLICATION_JSON);
        final BulkResponse response = client.bulk(br -> {
            br.index(indexName);
            for (int i = 0; i < size; i++) {
                if (random.nextBoolean()) {
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
        final var p1 = randomPerson();
        final var p2 = randomPersonDistinctFrom(p1);
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
        final String missingIndex = randomResourceName("missing");
        final String missingDest = randomResourceName("dest");
        // Check the error is thrown when the source index does not exist
        assertThatThrownBy(() -> client.reindex(rr -> rr
                .source(s -> s.index(missingIndex)).dest(d -> d.index(missingDest))))
                .isInstanceOfSatisfying(ElasticsearchException.class, e -> assertThat(e.status()).isEqualTo(404));

        // A regular reindex operation
        final String destIndex = extraIndex("dest");

        client.index(ir -> ir.index(indexName).id(randomId()).withJson(new StringReader("{\"foo\":" + randomInt(1, 100) + "}")));
        client.indices().refresh(rr -> rr.index(indexName));
        final ReindexResponse reindexResponse = client.reindex(rr -> rr
                .source(s -> s.index(indexName)).dest(d -> d.index(destIndex)));
        assertThat(reindexResponse.total()).isEqualTo(1);
    }

    @Test
    void geoPointSort() throws IOException {
        client.indices().create(cir -> cir.index(indexName));
        client.indices().putMapping(pmr -> pmr.index(indexName).properties("location", p -> p.geoPoint(gp -> gp)));
        final var p1 = randomPerson();
        p1.setLocation(new GeoPoint(49.0404, 2.0174));
        final var p2 = randomPersonDistinctFrom(p1);
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
            assertThat(hit1.id()).isEqualTo(p1.getId());
            assertThat(hit1.sort()).hasSize(1);
            assertThat(hit1.sort().get(0).doubleValue()).isEqualTo(0.0);
        }, hit2 -> {
            assertThat(hit2.id()).isEqualTo(p2.getId());
            assertThat(hit2.sort()).hasSize(1);
            assertThat(hit2.sort().get(0).doubleValue()).isEqualTo(8187.4318605250455);
        });
    }

    @Test
    void geoPointSearch() throws IOException {
        client.indices().create(cir -> cir.index(indexName));
        client.indices().putMapping(pmr -> pmr.index(indexName).properties("location", p -> p.geoPoint(gp -> gp)));
        final var p1 = randomPerson();
        p1.setLocation(new GeoPoint(49.0404, 2.0174));
        final var p2 = randomPersonDistinctFrom(p1);
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
        assertThat(response.hits().hits()).satisfiesExactly(hit -> assertThat(hit.id()).isEqualTo(p1.getId()));
    }

    @Test
    void searchWithTimeout() throws IOException, ExecutionException, InterruptedException {
        final String foo = randomToken();
        client.index(ir -> ir.index(indexName).id(randomId()).withJson(new StringReader("{\"foo\":\"" + foo + "\"}")));
        client.indices().refresh(rr -> rr.index(indexName));

        final var timeoutException = new AtomicReference<>(false);

        final CompletableFuture<SearchResponse<Void>> future = asyncClient.search(sr -> sr
                                .index(indexName)
                                .query(q -> q.match(mq -> mq.field("foo").query(foo))),
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
                                .query(q -> q.match(mq -> mq.field("foo").query(foo))),
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
        // CI starts a fresh cluster; system indices (deprecation logs, …) can still be INITIALIZING.
        client.cluster().health(h -> h.waitForNoInitializingShards(true).timeout(t -> t.time("30s")));
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
        final String pipelineId = randomResourceName("pipeline");
        final String scriptValue = randomToken();
        final String setValue = randomToken();
        // Define some pipelines
        try {
            client.ingest().deletePipeline(pr -> pr.id(pipelineId));
        } catch (final ElasticsearchException ignored) { /* Might throw a 404 which we don't care about */ }
        {
            final PutPipelineResponse response = client.ingest().putPipeline(pr -> pr
                    .id(pipelineId)
                    .processors(p -> p
                            .script(s -> s
                                    .lang(ScriptLanguage.Painless)
                                    .source(src -> src.scriptString("ctx.foo = '" + scriptValue + "'"))
                            )
                    )
            );
            assertThat(response.acknowledged()).isTrue();
        }
        {
            final PutPipelineResponse response = client.ingest().putPipeline(pr -> pr
                    .id(pipelineId)
                    .processors(p -> p
                            .set(s -> s
                                    .field("foo")
                                    .value(JsonData.of(setValue))
                                    .ignoreFailure(true)
                            )
                    )
            );
            assertThat(response.acknowledged()).isTrue();
        }
        {
            final SimulateResponse response = client.ingest().simulate(sir -> sir
                    .id(pipelineId)
                    .docs(d -> d
                            .source(JsonData.fromJson("{\"foo\":\"" + randomToken() + "\"}"))
                    )
            );
            assertThat(response.docs())
                    .hasSize(1)
                    .allSatisfy(doc -> {
                        assertThat(doc.doc()).isNotNull();
                        assertThat(doc.doc().source()).isNotNull();
                        assertThat(doc.doc().source()).allSatisfy((key, value) -> {
                            assertThat(key).isEqualTo("foo");
                            assertThat(value).satisfies(jsonData -> assertThat(jsonData.to(String.class)).isEqualTo(setValue));
                        });
                    });
        }
    }

    @Test
    void sourceRequest() throws IOException {
        final String id = randomId();
        final String foo = randomToken();
        client.index(ir -> ir.index(indexName).id(id).withJson(new StringReader("{\"foo\":\"" + foo + "\"}")));
        client.indices().refresh(rr -> rr.index(indexName));
        final GetSourceResponse<ObjectNode> source = client.getSource(gsr -> gsr.index(indexName).id(id), ObjectNode.class);
        assertThat(source.source())
                .isNotNull()
                .satisfies(jsonData -> assertThat(jsonData).hasToString("{\"foo\":\"" + foo + "\"}"));
    }

    @Test
    void deleteByQuery() throws IOException {
        final String foo = randomToken();
        client.index(ir -> ir.index(indexName).id(randomId()).withJson(new StringReader("{\"foo\":\"" + foo + "\"}")));
        client.indices().refresh(rr -> rr.index(indexName));
        final SearchResponse<Void> response1 = client.search(sr -> sr.index(indexName), Void.class);
        assertThat(response1.hits().total()).isNotNull();
        assertThat(response1.hits().total().value()).isEqualTo(1);
        final DeleteByQueryResponse deleteByQueryResponse = client.deleteByQuery(dbq -> dbq
                .index(indexName)
                .query(q -> q
                        .match(mq -> mq
                                .field("foo")
                                .query(foo))));
        assertThat(deleteByQueryResponse.deleted()).isEqualTo(1);
        client.indices().refresh(rr -> rr.index(indexName));
        final SearchResponse<Void> response2 = client.search(sr -> sr.index(indexName), Void.class);
        assertThat(response2.hits().total()).isNotNull();
        assertThat(response2.hits().total().value()).isZero();
    }

    @Test
    void updateDocument() throws IOException {
        final String id = randomId();
        final int showCount = randomInt(0, 50);
        client.index(ir -> ir.index(indexName).id(id).withJson(new StringReader("{\"show_count\":" + showCount + "}")));
        client.update(ur -> ur.index(indexName).id(id).script(
                s -> s
                        .lang(ScriptLanguage.Painless)
                        .source(src -> src.scriptString("ctx._source.show_count += 1"))
        ), ObjectNode.class);
        final GetResponse<ObjectNode> response = client.get(gr -> gr.index(indexName).id(id), ObjectNode.class);
        assertThat(response.source())
                .isNotNull()
                .satisfies(o -> assertThat(o).hasToString("{\"show_count\":" + (showCount + 1) + "}"));
    }

    @Test
    void createComponentTemplate() throws IOException {
        final String templateName = randomResourceName("component");
        {
            final PutComponentTemplateResponse response = client.cluster().putComponentTemplate(pct -> pct
                    .name(templateName)
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
                    .name(templateName)
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
        final String componentName = randomResourceName("component");
        final String templateName = randomResourceName("template");
        final String aliasName = randomResourceName("alias");
        final String routing = randomToken();
        client.cluster().putComponentTemplate(pct -> pct
                .name(componentName)
                .template(t -> t
                        .settings(s -> s.numberOfShards("1").numberOfReplicas("0"))
                        .mappings(m -> m
                                .properties("foo", p -> p.text(tp -> tp))
                        )
                )
        );
        final PutIndexTemplateResponse response = client.indices().putIndexTemplate(pit -> pit
                .name(templateName)
                .indexPatterns(randomResourceName("idx") + "-*")
                .composedOf(componentName)
                .template(t -> t
                        .aliases(aliasName, a -> a
                                .indexRouting(routing)
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
        final String pipelineId = randomResourceName("elser");
        // Create the index with sparse vector
        client.indices().create(cir -> cir.index(indexName).mappings(m -> m
                .properties("content", p -> p.text(tp -> tp))
                .properties("content_embedding", p -> p.sparseVector(sp -> sp))
        ));

        // Create the pipeline
        // This requires to have the elserv2 model deployed and started
        client.ingest().putPipeline(pr -> pr
                .id(pipelineId)
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
                            .inferenceId(pipelineId)
                            .query("How to avoid muscle soreness after running?")
                    )), ObjectNode.class);
        })
                .withFailMessage("We are expecting an exception as the model is not deployed")
                .isInstanceOfSatisfying(ElasticsearchException.class, exception -> {
                    assertThat(exception.error().reason()).isEqualTo("[" + pipelineId + "] is not an inference service model or a deployed ml model");
                    assertThat(exception.status()).isEqualTo(404);
                });
    }

    @Test
    void testIlm() throws IOException {
        final String policyName = randomResourceName("ilm");
        try {
            client.ilm().deleteLifecycle(dlr -> dlr.name(policyName));
        } catch (IOException | ElasticsearchException ignored) { /* Might throw a 404 which we don't care about */ }
        PutLifecycleResponse response = client.ilm().putLifecycle(plr -> plr
                .name(policyName)
                .policy(p -> p
                        .phases(ph -> ph
                                .hot(h -> h
                                        .actions(a -> a
                                                .rollover(r -> r
                                                        .maxAge(t -> t.time("5d"))
                                                        .maxPrimaryShardSize("10gb")
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
        final String idWithoutBar = randomId();
        final String idWithBar = randomIdDistinctFrom(idWithoutBar);
        final String foo = randomToken();
        final String bar = randomToken();
        client.index(ir -> ir.index(indexName).id(idWithoutBar).withJson(new StringReader("{\"foo\":\"" + foo + "\"}")));
        client.index(ir -> ir.index(indexName).id(idWithBar).withJson(new StringReader("{\"foo\":\"" + foo + "\", \"bar\":\"" + bar + "\"}")));
        client.indices().refresh(rr -> rr.index(indexName));
        final SearchResponse<Void> response = client.search(sr -> sr
                        .index(indexName)
                        .query(q -> q.exists(eq -> eq.field("bar")))
                , Void.class);
        assertThat(response.hits().total()).isNotNull();
        assertThat(response.hits().total().value()).isEqualTo(1);
        assertThat(response.hits().hits()).satisfiesExactly(hit -> assertThat(hit.id()).isEqualTo(idWithBar));
    }

    @Test
    void multipleAggs() throws IOException {
        final String country = randomToken();
        final String state = randomToken();
        final String city = randomToken();
        client.index(ir -> ir.index(indexName).withJson(new StringReader(
                "{\"country\":\"" + country + "\",\"state\":\"" + state + "\",\"city\":\"" + city + "\"}")));
        client.index(ir -> ir.index(indexName).withJson(new StringReader(
                "{\"country\":\"" + randomToken() + "\",\"state\":\"" + randomToken() + "\",\"city\":\"" + randomToken() + "\"}")));
        client.index(ir -> ir.index(indexName).withJson(new StringReader(
                "{\"country\":\"" + randomToken() + "\",\"state\":\"" + randomToken() + "\",\"city\":\"" + randomToken() + "\"}")));
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
                            .anySatisfy(countryBucket -> {
                                assertThat(countryBucket.key()).isNotNull();
                                assertThat(countryBucket.key().stringValue()).isEqualTo(country);
                                assertThat(countryBucket.docCount()).isEqualTo(1);
                                assertThat(countryBucket.aggregations())
                                        .hasEntrySatisfying("state", stateAgg -> {
                                            assertThat(stateAgg.sterms()).isNotNull();
                                            assertThat(stateAgg.sterms().buckets()).isNotNull();
                                            assertThat(stateAgg.sterms().buckets().array())
                                                    .hasSize(1)
                                                    .satisfiesExactly(stateBucket -> {
                                                        assertThat(stateBucket.key()).isNotNull();
                                                        assertThat(stateBucket.key().stringValue()).isEqualTo(state);
                                                        assertThat(stateBucket.docCount()).isEqualTo(1);
                                                        assertThat(stateBucket.aggregations())
                                                                .containsKey("city")
                                                                .hasEntrySatisfying("city", cityAgg -> {
                                                                    assertThat(cityAgg.sterms()).isNotNull();
                                                                    assertThat(cityAgg.sterms().buckets()).isNotNull();
                                                                    assertThat(cityAgg.sterms().buckets().array())
                                                                            .hasSize(1)
                                                                            .satisfiesExactly(cityBucket -> {
                                                                                assertThat(cityBucket.key()).isNotNull();
                                                                                assertThat(cityBucket.key().stringValue()).isEqualTo(city);
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
        final var p1 = randomPerson();
        final var p2 = randomPersonDistinctFrom(p1);
        client.index(ir -> ir.index(indexName).id(p1.getId()).document(p1));
        client.index(ir -> ir.index(indexName).id(p2.getId()).document(p2));
        client.indices().refresh(rr -> rr.index(indexName));

        String query = """
            FROM indexName
            | WHERE name == "personName"
            | KEEP name
            | LIMIT 1
            """.replaceFirst("indexName", indexName).replaceFirst("personName", p1.getName());

        {
            // Using the Raw ES|QL API
            try (final BinaryResponse response = client.esql().query(q -> q.query(query)); InputStream is = response.content()) {
                // The response object is {"took":6,"is_partial":false,"completion_time_in_millis":1786952355420,"documents_found":1,"values_loaded":1,"rows_emitted":7,"bytes_read":0,"read_nanos":0,"cpu_nanos":693792,"start_time_in_millis":1786952355414,"expiration_time_in_millis":1787384355371,"columns":[{"name":"name","type":"text"}],"values":[["David"]]}
                final ObjectMapper mapper = new ObjectMapper();
                final JsonNode jsonNode = mapper.readTree(is);
                assertThat(jsonNode).isNotNull().hasSize(13);
                assertThat(jsonNode.get("columns")).isNotNull().hasSize(1).first().satisfies(column -> assertThat(column.get("name").asText()).isEqualTo("name"));
                assertThat(jsonNode.get("values")).isNotNull().hasSize(1).first().satisfies(value -> assertThat(value).hasSize(1).first().satisfies(singleValue -> assertThat(singleValue.asText()).isEqualTo(p1.getName())));
                assertThat(jsonNode.get("took").asInt()).isGreaterThan(0);
                assertThat(jsonNode.get("is_partial").asBoolean()).isFalse();
                assertThat(jsonNode.get("documents_found").asLong()).isEqualTo(1);
                assertThat(jsonNode.get("values_loaded").asLong()).isEqualTo(1);
                // Added in 9.3.0
                assertThat(jsonNode.get("completion_time_in_millis").asLong()).isGreaterThan(0);
                assertThat(jsonNode.get("start_time_in_millis").asLong()).isGreaterThan(0);
                assertThat(jsonNode.get("expiration_time_in_millis").asLong()).isGreaterThan(0);
                // Added in 9.5.0
                assertThat(jsonNode.get("rows_emitted").asLong()).isEqualTo(7);
                assertThat(jsonNode.get("bytes_read").asLong()).isGreaterThanOrEqualTo(0);
                assertThat(jsonNode.get("read_nanos").asLong()).isGreaterThanOrEqualTo(0);
                assertThat(jsonNode.get("cpu_nanos").asLong()).isGreaterThanOrEqualTo(0);
            }
        }

        {
            // Using the JDBC ResultSet ES|QL API
            try (final ResultSet resultSet = client.esql().query(ResultSetEsqlAdapter.INSTANCE, query)) {
                assertThat(resultSet).isNotNull().satisfies(resultSetResult -> {
                    assertThat(resultSetResult.next()).isTrue();
                    assertThat(resultSetResult.getString("name")).isEqualTo(p1.getName());
                });
            }
        }

        {
            // Using the Object ES|QL API
            final Iterable<Person> persons = client.esql().query(ObjectsEsqlAdapter.of(Person.class), query);
            for (final Person person : persons) {
                assertThat(person.getId()).isNull();
                assertThat(person.getName()).isEqualTo(p1.getName());
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
                            Map.of("name", p1.getName())
                    );
            for (final Person person : persons) {
                assertThat(person.getId()).isNull();
                assertThat(person.getName()).isEqualTo(p1.getName());
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
        final String v2Index = extraIndex("v2");
        final String aliasName = randomResourceName("alias");
        assertThat(client.indices().create(cir -> cir.index(indexName)
                .aliases(aliasName, a -> a)).acknowledged()).isTrue();
        assertThat(client.indices().create(cir -> cir.index(v2Index)).acknowledged()).isTrue();

        // Check the alias existence by its name
        assertThat(client.indices().existsAlias(ga -> ga.name(aliasName)).value()).isTrue();

        // Check we have one alias on indexName
        assertThat(client.indices().getAlias(ga -> ga.index(indexName)).aliases().get(indexName).aliases()).hasSize(1);
        // Check we have no alias on v2
        assertThat(client.indices().getAlias(ga -> ga.index(v2Index)).aliases().get(v2Index).aliases()).isEmpty();

        // Switch the alias from indexName to v2
        client.indices().updateAliases(ua -> ua
                .actions(a -> a.add(aa -> aa.alias(aliasName).index(v2Index)))
                .actions(a -> a.remove(ra -> ra.alias(aliasName).index(indexName)))
        );

        // Check we have no alias on indexName
        assertThat(client.indices().getAlias(ga -> ga.index(indexName)).aliases().get(indexName).aliases()).isEmpty();
        // Check we have one alias on v2
        assertThat(client.indices().getAlias(ga -> ga.index(v2Index)).aliases().get(v2Index).aliases()).hasSize(1);

        // Check the alias existence by its name
        assertThat(client.indices().existsAlias(ga -> ga.name(aliasName)).value()).isTrue();

        // Delete the alias
        client.indices().deleteAlias(da -> da.name(aliasName).index("*"));

        // Check the alias non-existence by its name
        assertThat(client.indices().existsAlias(ga -> ga.name(aliasName)).value()).isFalse();
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

        assumeTrue(response.hits().total() != null);
        assertThat(response.hits().total().value()).isEqualTo(1);
        assertThat(response.hits().hits().get(0).score()).isEqualTo(0.40630677);
    }

    @Test
    void boolQuery() throws IOException {
        final String id1 = randomId();
        final String id2 = randomIdDistinctFrom(id1);
        final String id3 = randomIdDistinctFrom(id1, id2);
        final String id4 = randomIdDistinctFrom(id1, id2, id3);
        client.index(ir -> ir.index(indexName).id(id1).withJson(new StringReader("""
                {
                    "number":1,
                    "effective_date":"2024-10-01T00:00:00.000Z"
                }""")));
        client.index(ir -> ir.index(indexName).id(id2).withJson(new StringReader("""
                {
                    "number":2,
                    "effective_date":"2024-10-02T00:00:00.000Z"
                }""")));
        client.index(ir -> ir.index(indexName).id(id3).withJson(new StringReader("""
                {
                    "number":3,
                    "effective_date":"2024-10-03T00:00:00.000Z"
                }""")));
        client.index(ir -> ir.index(indexName).id(id4).withJson(new StringReader("""
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
        assertThat(response.hits().hits().get(0).id()).isEqualTo(id3);
    }

    private String randomId() {
        return RandomStrings.randomAsciiAlphanumOfLengthBetween(random, 4, 8);
    }

    private String randomIdDistinctFrom(final String... others) {
        final Set<String> existing = Set.of(others);
        String id;
        do {
            id = randomId();
        } while (existing.contains(id));
        return id;
    }

    private String randomToken() {
        return RandomStrings.randomAsciiAlphanumOfLengthBetween(random, 3, 10).toLowerCase(Locale.ROOT);
    }

    private int randomInt(final int min, final int max) {
        return RandomNumbers.randomIntInRange(random, min, max);
    }

    private String randomResourceName(final String prefix) {
        return (indexName + "-" + prefix + "-" + randomToken()).toLowerCase(Locale.ROOT);
    }

    private String extraIndex(final String suffix) {
        final String name = (indexName + "-" + suffix + "-" + randomToken()).toLowerCase(Locale.ROOT);
        setAndRemoveIndex(name);
        return name;
    }

    private Person randomPerson() {
        final var person = new Person();
        person.setId(randomId());
        person.setName(randomToken());
        return person;
    }

    private Person randomPersonDistinctFrom(final Person other) {
        Person person;
        do {
            person = randomPerson();
        } while (person.getId().equals(other.getId()) || person.getName().equals(other.getName()));
        return person;
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

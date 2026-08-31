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

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import com.carrotsearch.randomizedtesting.jupiter.Randomized;
import com.carrotsearch.randomizedtesting.jupiter.generators.RandomNumbers;
import com.carrotsearch.randomizedtesting.jupiter.generators.RandomStrings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.*;
import org.testcontainers.containers.Network;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.elasticsearch.KibanaContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static fr.pilato.test.elasticsearch.hlclient.SSLUtils.createContextFromCaCert;
import static org.assertj.core.api.Assertions.assertThat;

@Randomized
class KibanaClientIT {

    private static final Logger logger = LogManager.getLogger();
    private static HttpClient kibanaClient = null;
    private static ElasticsearchClient elasticsearchClient = null;
    private static final String PASSWORD = "changeme";
    private static final String PREFIX = "kibanaclientit_";
    private static final String BASIC_AUTH = Base64.getEncoder()
            .encodeToString(("elastic:" + PASSWORD).getBytes(StandardCharsets.UTF_8));
    private static String kibanaUrl;
    private String indexName;
    private Random random;

    @BeforeAll
    static void startElasticsearchContainer() throws IOException, InterruptedException {
        final var props = new Properties();
        props.load(KibanaClientIT.class.getResourceAsStream("/version.properties"));
        String elasticsearchVersion = props.getProperty("elasticsearch.version");
        logger.info("Starting testcontainers with Elasticsearch/Kibana {}.", elasticsearchVersion);
        // Same network from the start so ES stays reachable on localhost after Kibana starts (CI).
        final var network = Network.newNetwork();
        ElasticsearchContainer elasticsearchContainer = new ElasticsearchContainer(
                DockerImageName.parse("docker.elastic.co/elasticsearch/elasticsearch")
                        .withTag(elasticsearchVersion))
                .withNetwork(network)
                .withPassword(PASSWORD);
                // this does not work yet. Wait for TC 2.0.6 to be released.
                // See https://github.com/testcontainers/testcontainers-java/pull/11986
                // .withReuse(true);
        KibanaContainer kibanaContainer = new KibanaContainer(elasticsearchContainer)
                .withNetwork(network);
                // this does not work yet. Wait for TC 2.0.6 to be released.
                // See https://github.com/testcontainers/testcontainers-java/pull/11986
                // .withReuse(true);
        elasticsearchContainer.start();
        kibanaContainer.start();

        // Create the Elasticsearch client
        final byte[] certAsBytes = elasticsearchContainer.copyFileFromContainer(
                "/usr/share/elasticsearch/config/certs/http_ca.crt",
                InputStream::readAllBytes);
        elasticsearchClient = ElasticsearchClient.of(b -> b
                .host("https://" + elasticsearchContainer.getHttpHostAddress())
                .sslContext(createContextFromCaCert(certAsBytes))
                .usernameAndPassword("elastic", PASSWORD));
        logger.info("Elasticsearch started at https://{}.", elasticsearchContainer.getHttpHostAddress());

        // Create the Kibana client
        kibanaUrl = "http://" + kibanaContainer.getHttpHostAddress();
        kibanaClient = HttpClient.newBuilder()
                .followRedirects(HttpClient.Redirect.NEVER)
                .build();
        final var response = kibana("GET", "/", null);
        logger.info("Kibana started at {} with status code {}.", kibanaUrl, response.statusCode());
    }

    @BeforeEach
    void cleanIndexBeforeRun(final TestInfo testInfo, final Random random) {
        this.random = random;
        final var methodName = testInfo.getTestMethod().orElseThrow().getName();
        indexName = PREFIX + methodName.toLowerCase(Locale.ROOT);

        logger.debug("Using [{}] as the index name", indexName);
        removeIndex(indexName);
    }

    @Test
    void testCreateDataAndDashboard() throws IOException, InterruptedException {
        // We generate some data in Elasticsearch to be able to create a dashboard in Kibana
        final int size = randomInt(2, 10);
        final var persons = randomPersons(size);
        for (final Person person : persons) {
            elasticsearchClient.index(ir -> ir.index(indexName).id(person.getId()).document(person));
        }
        elasticsearchClient.indices().refresh(rr -> rr.index(indexName));

        final String dataViewName = randomToken();
        final String dashboardTitle = "Person dashboard " + randomToken();
        final String metricTitle = randomToken() + " count";

        // Create a data view if missing (no time field — Person has none)
        if (kibana("GET", "/api/data_views/data_view/" + indexName, null).statusCode() == 404) {
            final var dataViewResponse = kibana("POST", "/api/data_views/data_view", """
                    {
                      "data_view": {
                        "id": "%s",
                        "title": "%s",
                        "name": "%s"
                      }
                    }
                    """.formatted(indexName, indexName, dataViewName));
            assertThat(dataViewResponse.statusCode()).as(dataViewResponse.body()).isEqualTo(200);
        }

        // Markdown + document count, then upsert via PUT /api/dashboards/{id}
        final var dashboardResponse = kibana("PUT", "/api/dashboards/" + indexName, """
                {
                  "title": "%s",
                  "panels": [
                    {
                      "type": "markdown",
                      "grid": { "x": 0, "y": 0, "w": 24, "h": 8 },
                      "config": { "content": "# %s\\n\\n%d documents indexed for this IT." }
                    },
                    {
                      "type": "vis",
                      "grid": { "x": 24, "y": 0, "w": 24, "h": 8 },
                      "config": {
                        "type": "metric",
                        "title": "%s",
                        "data_source": {
                          "type": "esql",
                          "query": "FROM %s | STATS count = COUNT()"
                        },
                        "metrics": [{ "type": "primary", "column": "count" }]
                      }
                    }
                  ]
                }
                """.formatted(dashboardTitle, dashboardTitle, size, metricTitle, indexName));
        assertThat(dashboardResponse.statusCode()).as(dashboardResponse.body()).isIn(200, 201);
        assertThat(dashboardResponse.body()).contains(dashboardTitle);
        logger.info("Dashboard available at {}/app/dashboards#/view/{}", kibanaUrl, indexName);

        logger.info("You can add a breakpoint on this line and then open Kibana.");
    }

    private String randomId() {
        return RandomStrings.randomAsciiAlphanumOfLengthBetween(random, 4, 8);
    }

    private String randomToken() {
        return RandomStrings.randomAsciiAlphanumOfLengthBetween(random, 3, 10).toLowerCase(Locale.ROOT);
    }

    private int randomInt(final int min, final int max) {
        return RandomNumbers.randomIntInRange(random, min, max);
    }

    private Person randomPerson() {
        final var person = new Person();
        person.setId(randomId());
        person.setName(randomToken());
        return person;
    }

    private List<Person> randomPersons(final int count) {
        final var persons = new ArrayList<Person>();
        while (persons.size() < count) {
            final var candidate = randomPerson();
            if (persons.stream().noneMatch(p -> p.getId().equals(candidate.getId())
                    || p.getName().equals(candidate.getName()))) {
                persons.add(candidate);
            }
        }
        return persons;
    }

    private static HttpResponse<String> kibana(final String method, final String path, final String json)
            throws IOException, InterruptedException {
        final var request = HttpRequest.newBuilder()
                .uri(URI.create(kibanaUrl + path))
                .header("Authorization", "Basic " + BASIC_AUTH)
                .header("kbn-xsrf", "true")
                .header("Content-Type", "application/json")
                .method(method, json == null
                        ? HttpRequest.BodyPublishers.noBody()
                        : HttpRequest.BodyPublishers.ofString(json))
                .build();
        final var response = kibanaClient.send(request, HttpResponse.BodyHandlers.ofString());
        logger.debug("{} {} -> {} {}", method, path, response.statusCode(), response.body());
        return response;
    }

    /**
     * This method deletes the index if it exists.
     * @param name the index name
     */
    private void removeIndex(final String name) {
        try {
            elasticsearchClient.indices().delete(dir -> dir.index(name));
            logger.debug("Index [{}] has been removed", name);
        } catch (final IOException | ElasticsearchException ignored) { /* Might throw a 404 which we don't care about */ }
    }
}

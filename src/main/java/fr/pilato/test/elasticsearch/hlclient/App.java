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

import org.apache.http.HttpHost;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
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
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;

public class App {
    public static void main(String[] args) {
        callInfo();
        createIndex();
        createMapping();
        createData();
        exist();
        getWithFilter();
        nodeStatsWithLowLevelClient();
        searchData();
    }

    private static void getWithFilter() {
        try (RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(HttpHost.create("http://localhost:9200")))) {
            try {
                client.indices().delete(new DeleteIndexRequest("test"), RequestOptions.DEFAULT);
            } catch (ElasticsearchStatusException ignored) { }
            client.index(new IndexRequest("test").id("1").source("{\"foo\":\"bar\", \"application_id\": 6}", XContentType.JSON), RequestOptions.DEFAULT);
            GetResponse getResponse = client.get(new GetRequest("test", "1").fetchSourceContext(
                    new FetchSourceContext(true, new String[]{"application_id"}, null)
            ), RequestOptions.DEFAULT);
            System.out.println("doc = " + getResponse);
        } catch (Exception e) {
            e.printStackTrace(System.err);
        }
    }

    private static void nodeStatsWithLowLevelClient() {
        try (RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(HttpHost.create("http://localhost:9200")))) {
            Response response = client.getLowLevelClient().performRequest(new Request("GET", "/_nodes/stats/thread_pool"));
            String s = EntityUtils.toString(response.getEntity());
            System.out.println("thread_pool = " + s);
        } catch (Exception e) {
            e.printStackTrace(System.err);
        }
    }

    private static void exist() {
        try (RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(HttpHost.create("http://localhost:9200")))) {
            try {
                client.indices().delete(new DeleteIndexRequest("test"), RequestOptions.DEFAULT);
            } catch (ElasticsearchStatusException ignored) { }
            client.index(new IndexRequest("test").id("1").source("{\"foo\":\"bar\"}", XContentType.JSON), RequestOptions.DEFAULT);
            boolean exists1 = client.exists(new GetRequest("test", "1"), RequestOptions.DEFAULT);
            boolean exists2 = client.exists(new GetRequest("test", "2"), RequestOptions.DEFAULT);
            System.out.println("exists1 = " + exists1);
            System.out.println("exists2 = " + exists2);
        } catch (Exception e) {
            e.printStackTrace(System.err);
        }
    }

    private static void createIndex() {
        String settings = "{\n" +
                "  \"mappings\": {\n" +
                "      \"properties\": {\n" +
                "        \"content\": {\n" +
                "          \"type\": \"text\"\n" +
                "        }\n" +
                "      }\n" +
                "  }\n" +
                "}\n";

        try (RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(HttpHost.create("http://localhost:9200")))) {
            try {
                client.indices().delete(new DeleteIndexRequest("test"), RequestOptions.DEFAULT);
            } catch (ElasticsearchStatusException ignored) { }
            CreateIndexRequest createIndexRequest = new CreateIndexRequest("test");
            createIndexRequest.source(settings, XContentType.JSON);
            client.indices().create(createIndexRequest, RequestOptions.DEFAULT);
        } catch (Exception e) {
            e.printStackTrace(System.err);
        }
    }

    private static void callInfo() {
        try (RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(HttpHost.create("http://localhost:9200")))) {
            MainResponse info = client.info(RequestOptions.DEFAULT);
            String version = info.getVersion().getNumber();
            System.out.println("version = " + version);
        } catch (Exception e) {
            e.printStackTrace(System.err);
        }
    }

    private static void createMapping() {
        try (RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(HttpHost.create("http://localhost:9200")))) {
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
        } catch (Exception e) {
            e.printStackTrace(System.err);
        }
    }

    private static void createData() {
        try (RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(HttpHost.create("http://localhost:9200")))) {
            try {
                client.indices().delete(new DeleteIndexRequest("test"), RequestOptions.DEFAULT);
            } catch (ElasticsearchStatusException ignored) { }
            client.index(new IndexRequest("test").id("1").source("{\"foo\":\"bar\"}", XContentType.JSON), RequestOptions.DEFAULT);
            client.indices().refresh(new RefreshRequest("test"), RequestOptions.DEFAULT);
            SearchResponse response = client.search(new SearchRequest("test"), RequestOptions.DEFAULT);
            System.out.println("response.getHits().totalHits = " + response.getHits().getTotalHits().value);
        } catch (Exception e) {
            e.printStackTrace(System.err);
        }
    }

    private static void searchData() {
        try (RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(HttpHost.create("http://localhost:9200")))) {
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
            System.out.println("response.getHits().totalHits = " + response.getHits().getTotalHits().value);
            response = client.search(new SearchRequest("test").source(
                    new SearchSourceBuilder().query(
                            QueryBuilders.wrapperQuery("{\"match_all\":{}}")
                    )
            ), RequestOptions.DEFAULT);
            System.out.println("response.getHits().totalHits = " + response.getHits().getTotalHits().value);
        } catch (Exception e) {
            e.printStackTrace(System.err);
        }
    }
}

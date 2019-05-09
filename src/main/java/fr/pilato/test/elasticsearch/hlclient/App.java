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
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.PutMappingRequest;
import org.elasticsearch.common.xcontent.XContentType;

public class App {
    public static void main(String[] args) {
        createMapping();
        // createData();
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
}

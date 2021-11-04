package com.xiaohu.read;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Search;
import io.searchbox.core.Bulk;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.MaxAggregation;
import io.searchbox.core.search.aggregation.MetricAggregation;
import io.searchbox.core.search.aggregation.TermsAggregation;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class read {
     public static void main(String[] args) throws IOException {
         //创建es客户端连接池
         JestClientFactory jestClientFactory = new JestClientFactory();
         //创建es客户端连接地址
         HttpClientConfig httpClientConfig = new HttpClientConfig.Builder("http://hadoop102:9200").build();
         //设置es链接地址
         jestClientFactory.setHttpClientConfig(httpClientConfig);
         //获取链接
         JestClient jestClient = jestClientFactory.getObject();
         //构建查询对象
         /*Search build = new Search.Builder("{\n" +
                 "  \"query\": {\n" +
                 "    \"bool\": {\n" +
                 "      \"filter\": {\n" +
                 "        \"term\": {\n" +
                 "          \"sex\": \"famale\"\n" +
                 "        }\n" +
                 "        }\n" +
                 "        ,\"must\": {\n" +
                 "          \"match\": {\n" +
                 "            \"favo\": \"海狗人参丸\"\n" +
                 "          }\n" +
                 "      }\n" +
                 "    }\n" +
                 "  }\n" +
                 "}").addIndex("student").addType("_doc").build();*/
         Search build = new Search.Builder("{\n" +
                 "  \"aggs\": {\n" +
                 "    \"groupbyclassN\": {\n" +
                 "      \"terms\": {\n" +
                 "        \"field\": \"class_id\",\n" +
                 "        \"size\": 10\n" +
                 "      },\n" +
                 "      \"aggs\": {\n" +
                 "        \"maxage\": {\n" +
                 "          \"max\": {\n" +
                 "            \"field\": \"age\"\n" +
                 "          }\n" +
                 "        }\n" +
                 "      }\n" +
                 "    }\n" +
                 "  }\n" +
                 "}").addIndex("student").addType("_doc").build();




         SearchResult searchResult = jestClient.execute(build);
         //获取条数
         System.out.println("总数量 = " + searchResult.getTotal());
         //获取数据的明细
         /*List<SearchResult.Hit<Map, Void>> hits = searchResult.getHits(Map.class);
         for (SearchResult.Hit<Map, Void> hit : hits) {
             System.out.println("hit.index = " + hit.index);
             System.out.println("hit.type = " + hit.type);
             System.out.println("hit.id = " + hit.id);
             Map source = hit.source;
             System.out.println("source.keySet() = " + source.keySet());
             for (Object o : source.keySet()) {
                 System.out.println("o = " + o);
             }
         }*/
         //获取聚合组的数据
         MetricAggregation aggregations = searchResult.getAggregations();
         TermsAggregation groupByClass = aggregations.getTermsAggregation("groupbyclassN");
         List<TermsAggregation.Entry> buckets = groupByClass.getBuckets();
         for (TermsAggregation.Entry bucket : buckets) {
             System.out.println("bucket.getKey() = " + bucket.getKey());
             System.out.println("bucket.getCount() = " + bucket.getCount());
             //獲取年紀聚合組數據
         }
         jestClient.shutdownClient();
     }
}

package com.xiaohu.read;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.MaxAggregation;
import io.searchbox.core.search.aggregation.MetricAggregation;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.max.MaxAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class readall {
 public static void main(String[] args) throws IOException {
     JestClientFactory jestClientFactory = new JestClientFactory();
     HttpClientConfig httpClientConfig = new HttpClientConfig.Builder("http://hadoop102:9200").build();
     jestClientFactory.setHttpClientConfig(httpClientConfig);
     JestClient jestClient = jestClientFactory.getObject();
     //-----------------------------------
     SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
     BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
     List<QueryBuilder> filter = boolQueryBuilder.filter();
     TermQueryBuilder termQueryBuilder = new TermQueryBuilder("sex", "male");
     searchSourceBuilder.query(boolQueryBuilder);
     MatchQueryBuilder matchQueryBuilder = new MatchQueryBuilder("favo", "洗澡");
     boolQueryBuilder.must(matchQueryBuilder);
     TermsAggregationBuilder field = AggregationBuilders.terms("groupByclass").field("class_id");
     MaxAggregationBuilder field1 = AggregationBuilders.max("groupByAge").field("age");
     //TODO -----------------------------aggs------------------------------
     searchSourceBuilder.aggregation(field.subAggregation(field1));

     //TODO -----------------------------from------------------------------
     searchSourceBuilder.from(0);

     //TODO -----------------------------size------------------------------
     searchSourceBuilder.size(2);

     //---------------------------
     Search build = new Search.Builder(searchSourceBuilder.toString()).addIndex("student").addType("_doc").build();
     SearchResult res = jestClient.execute(build);
     //条数
     System.out.println("res.getTotal() = " + res.getTotal());
     //数据明细
     List<SearchResult.Hit<Map, Void>> hits = res.getHits(Map.class);
     for (SearchResult.Hit<Map, Void> hit : hits) {
         System.out.println("hit.index = " + hit.index);
         System.out.println("hit.type = " + hit.type);
         Map map = hit.source;
         for (Object o : map.keySet()) {
             System.out.println("o = " + o+"+"+map.get(o));
             //获取聚合组数据
             MetricAggregation aggregations = res.getAggregations();
             TermsAggregation groupByclass = aggregations.getTermsAggregation("groupByclass");
             List<TermsAggregation.Entry> buckets = groupByclass.getBuckets();
             for (TermsAggregation.Entry bucket : buckets) {
                 System.out.println("bucket.getKey() = " + bucket.getKey());
                 System.out.println("bucket.getCount() = " + bucket.getCount());
                 //获取年纪组
                 MaxAggregation group = bucket.getMaxAggregation("groupByAge");
                 System.out.println("group = " + group);
             }

         }
         jestClient.shutdownClient();
     }


 }
}

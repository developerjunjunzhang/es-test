package com.iflyteck.estest;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Test;

import javax.swing.text.Highlighter;
import java.net.InetAddress;
import java.util.Iterator;
import java.util.Map;

public class SearchIndexTest {

    /*
    根据id查询
     */
    @Test
    public void searchById() throws Exception {
        Settings settings = Settings.builder().put("cluster.name", "my-elasticsearch").build();
        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9301))
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9302))
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9303));
        IdsQueryBuilder query = QueryBuilders.idsQuery().addIds("2","4");
        SearchResponse res = client.prepareSearch("es-hello")
                .setTypes("article")
                .setQuery(query)
                .get();
        SearchHits hits = res.getHits();
        System.out.println("查询结果总数：" + hits.getTotalHits());
        Iterator<SearchHit> iterator = hits.iterator();
        while (iterator.hasNext()) {
            SearchHit hit = iterator.next();
            System.out.println(hit.getSourceAsString());
            // 获取文档属性
            Map<String, Object> document = hit.getSource();
            System.out.println(document.get("id"));
            System.out.println(document.get("title"));
            System.out.println(document.get("content"));
        }
        // 关闭client
        client.close();
    }

    /**
     * 根据关键字查询
     * @throws Exception
     */
    @Test
    public void testQueryByTerm() throws Exception {
        TermQueryBuilder queryBuilder = QueryBuilders.termQuery("title", "习近平");
        searchUtils(queryBuilder);
    }

    /**
     * QueryString查询
     * @throws Exception
     */
    @Test
    public void testQueryString() throws Exception {
        QueryStringQueryBuilder queryBuilder = QueryBuilders.queryStringQuery("激动人心的讲话").defaultField("content");
        searchUtils(queryBuilder);
    }

    /**
     * 查询高亮显示
     * @throws Exception
     */
    @Test
    public void searchHighLight() throws Exception {
        Settings settings = Settings.builder().put("cluster.name", "my-elasticsearch").build();
        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9301))
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9302))
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9303));
        QueryStringQueryBuilder queryBuilder = QueryBuilders.queryStringQuery("这个美好的时代").defaultField("title");
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        highlightBuilder.field("title");
        highlightBuilder.preTags("<em>");
        highlightBuilder.postTags("</em>");
        SearchResponse res = client.prepareSearch("es-hello")
                .setTypes("article")
                .setQuery(queryBuilder)
                .setFrom(0)
                .setSize(20)
                .highlighter(highlightBuilder)
                .get();
        SearchHits hits = res.getHits();
        System.out.println("查询结果总数：" + hits.getTotalHits());
        Iterator<SearchHit> iterator = hits.iterator();
        while (iterator.hasNext()) {
            SearchHit hit = iterator.next();
            System.out.println(hit.getSourceAsString());
            // 获取文档属性
            Map<String, Object> document = hit.getSource();
            System.out.println(document.get("id"));
            System.out.println(document.get("title"));
            System.out.println(document.get("content"));
            System.out.println("********高亮结果**********");
            Map<String, HighlightField> highlightFields = hit.getHighlightFields();
            System.out.println(highlightFields);
            HighlightField highlightField = highlightFields.get("title");
            Text[] fragments = highlightField.fragments();
            for (int i = 0; i < fragments.length; i++) {
                if (fragments[i] != null) {
                    String title = fragments[i].toString();
                    System.out.println(title);
                }
            }
            System.out.println("=================================");
        }
        // 关闭client
        client.close();
    }

    private void searchUtils(QueryBuilder queryBuilder) throws Exception{
        Settings settings = Settings.builder().put("cluster.name", "my-elasticsearch").build();
        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9301))
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9302))
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9303));
        SearchResponse searchResponse = client.prepareSearch("es-hello")
                .setTypes("article")
                .setQuery(queryBuilder)
                // 设置分页信息
                .setFrom(0)
                .setSize(20)
                .get();
        SearchHits hits = searchResponse.getHits();
        System.out.println("查询的总记录数：" + hits.getTotalHits());
        Iterator<SearchHit> iterator = hits.iterator();
        while (iterator.hasNext()) {
            SearchHit hit = iterator.next();
            System.out.println(hit.getSourceAsString());
            Map<String, Object> source = hit.getSource();
            System.out.println(source.get("id"));
            System.out.println(source.get("title"));
            System.out.println(source.get("content"));
        }
        // 关闭client
        client.close();
    }
}

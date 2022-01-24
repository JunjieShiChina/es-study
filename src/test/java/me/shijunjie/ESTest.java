package me.shijunjie;

import com.alibaba.fastjson.JSON;
import org.apache.http.HttpHost;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.IdsQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.util.concurrent.TimeUnit;


public class ESTest {

    RestHighLevelClient client = null;

    @Before
    public void before() throws IOException {
        // 创建client对象
        CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
//        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));
        RestClientBuilder builder = RestClient.builder(new HttpHost("127.0.0.1", 9200))
                .setHttpClientConfigCallback(httpClientBuilder -> {
                    RequestConfig.Builder requestConfigBuilder = RequestConfig.custom()
                            // 设置连接超时时间，单位毫秒。指的是连接目标url的连接超时时间，即客服端发送请求到与目标url建立起连接的最大时间。如果在该时间范围内还没有建立起连接，会抛出connectionTimeOut异常。
                            .setConnectTimeout(5000)
                            // 请求获取数据的超时时间(即响应时间)，单位毫秒。即在与目标url建立了连接之后，等待返回数据的时间的最大时间，如果超出时间没有响应，就会抛出socketTimeoutException异常。
                            .setSocketTimeout(5000)
                            // 设置从connect Manager(连接池)获取Connection 超时时间，单位毫秒。HttpClient中的要用连接时尝试从连接池中获取，若是在等待了一定的时间后还没有获取到可用连接（比如连接池中没有空闲连接了）则会抛出获取连接超时异常。
                            .setConnectionRequestTimeout(5000);
                    httpClientBuilder.setDefaultRequestConfig(requestConfigBuilder.build());
                    httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    return httpClientBuilder;
                });

        client = new RestHighLevelClient(builder);
    }

    @After
    public void after() throws IOException {
        client.close();
    }

    @Test
    public void testCreateIndex1() throws IOException {
        // 创建名称为blog_java索引
        client.indices().create(new CreateIndexRequest("blog_java"), RequestOptions.DEFAULT);
    }

    @Test
    public void testCreateIndex2() throws IOException {
        // 创建名称为blog_java索引
        client.indices().create(new CreateIndexRequest("blog_java2"), RequestOptions.DEFAULT);
    }

    @Test
    public void testCreateIndex3WithMapping() throws IOException {
        // 创建名称为blog_java索引
        CreateIndexRequest createIndexRequest = new CreateIndexRequest("blog_java3");
        XContentBuilder contentBuilder = XContentFactory.jsonBuilder().startObject()
                    .startObject("properties")
                        .startObject("id")
                            .field("type", "keyword")
                            .field("store", "true")
                            .field("index", "false")
                        .endObject()
                        .startObject("title")
                            .field("type", "text")
                            .field("store", "true")
                            .field("index", "true")
                        .endObject()
                        .startObject("content")
                            .field("type", "text")
                            .field("store", "true")
                            .field("index", "true")
                        .endObject()
                    .endObject()
                .endObject();
        createIndexRequest.mapping(contentBuilder);
        client.indices().create(createIndexRequest, RequestOptions.DEFAULT);
    }

    @Test
    public void testCreateIndex3WithMapping2() throws IOException {
        // 创建名称为blog_java索引
        CreateIndexRequest createIndexRequest = new CreateIndexRequest("blog_java888");
        XContentBuilder contentBuilder = XContentFactory.jsonBuilder().startObject()
                .startObject("properties")
                .startObject("id")
                .field("type", "keyword")
                .field("store", "true")
                .field("index", "false")
                .endObject()
                .startObject("title")
                .field("type", "text")
                .field("store", "true")
                .field("index", "true")
                .endObject()
                .startObject("content")
                .field("type", "text")
                .field("store", "true")
                .field("index", "true")
                .endObject()
                .startObject("createTime")
                .field("type", "date")
                .field("store", "true")
                .field("index", "true")
                .endObject()
                .endObject()
                .endObject();
        createIndexRequest.mapping(contentBuilder);
        client.indices().create(createIndexRequest, RequestOptions.DEFAULT);
    }

    @Test
    public void testCreateIndex4WithMapping() throws IOException {
        // 创建名称为blog_java索引
        Settings settings = Settings.builder().put("number_of_shards", 5).put("number_of_replicas", 5).build();
        CreateIndexRequest createIndexRequest = new CreateIndexRequest("blog_java4");
        createIndexRequest.settings(settings);
        XContentBuilder contentBuilder = XContentFactory.jsonBuilder().startObject()
                .startObject("properties")
                .startObject("id")
                .field("type", "keyword")
                .field("store", "true")
                .field("index", "false")
                .endObject()
                .startObject("title")
                .field("type", "text")
                .field("store", "true")
                .field("index", "true")
                .endObject()
                .startObject("content")
                .field("type", "text")
                .field("store", "true")
                .field("index", "true")
                .endObject()
                .endObject()
                .endObject();
        createIndexRequest.mapping(contentBuilder);
        client.indices().create(createIndexRequest, RequestOptions.DEFAULT);
    }

    @Test
    public void testCreateDocument() throws IOException {
        XContentBuilder contentBuilder = XContentFactory.jsonBuilder().startObject()
                .field("id", 1).field("title", "hahaha").field("content", "士大夫zhesh")
                .endObject();
        contentBuilder.close();

        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(new IndexRequest().index("blog_java4").id("2").source(contentBuilder));
        client.bulk(bulkRequest, RequestOptions.DEFAULT);
    }

    @Test
    public void testCreateDocumentUseJson() throws IOException {
        Artical artical = new Artical();
        artical.setId(3L);
        artical.setTitle("这条数据来自于JSON");
        artical.setContent("对对对，你说的都对");
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(new IndexRequest().index("blog_java4").id("3").source(JSON.toJSONString(artical), XContentType.JSON));
        client.bulk(bulkRequest, RequestOptions.DEFAULT);
    }

    @Test
    public void testCreateDocumentUseJson2() throws IOException {
        Artical artical = new Artical();
        artical.setId(3L);
        artical.setTitle("这是一个标题3");
        artical.setContent("对对对，你说的都对3");
        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.DATE, 1);
        artical.setCreateTime(calendar.getTime());
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(new IndexRequest().index("blog_java888").id("3").source(JSON.toJSONString(artical), XContentType.JSON));
        client.bulk(bulkRequest, RequestOptions.DEFAULT);
    }

    @Test
    public void testUpdate() throws IOException {
        UpdateRequest updateRequest = new UpdateRequest("blog_java4", "1");
        XContentBuilder contentBuilder = XContentFactory.jsonBuilder().startObject()
                .field("id", 1).field("title", "hahaha2").field("content", "士大夫zhesh")
                .endObject();
        contentBuilder.close();
        updateRequest.doc(contentBuilder);
        updateRequest.id("2");
        client.update(updateRequest, RequestOptions.DEFAULT);
    }

    /**
     * termQuery有bug,用matchPhraseQuery替代
     * @throws IOException
     */
    @Test
    public void testTermSearch() throws IOException {

        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices("blog_java4");
        
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.from(0);
        searchSourceBuilder.size(10);
        searchSourceBuilder.sort("id", SortOrder.ASC);
        QueryBuilder queryBuilder = QueryBuilders.termQuery("title", "这条数据来自于JSON");
        searchSourceBuilder.query(queryBuilder);
        
        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        System.out.println(searchResponse);
    }

    @Test
    public void testMatchPhraseQuery() throws IOException {

        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices("blog_java4");

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.from(0);
        searchSourceBuilder.size(10);
        searchSourceBuilder.sort("id", SortOrder.ASC);
        QueryBuilder queryBuilder = QueryBuilders.matchPhraseQuery("title", "这条数据来自于JSON");
        searchSourceBuilder.query(queryBuilder);
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        System.out.println(searchResponse);
    }

    /**
     * .文本模糊匹配
     * @throws IOException
     */
    @Test
    public void testWildcardQueryQuery() throws IOException {
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices("blog_java4");

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.from(0);
        searchSourceBuilder.size(10);
        searchSourceBuilder.sort("id", SortOrder.ASC);
        QueryBuilder queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.wildcardQuery("content", "夫"));
        searchSourceBuilder.query(queryBuilder);
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        System.out.println(searchResponse);
    }

    /**
     * 时间范围匹配
     * @throws IOException
     */
    @Test
    public void testRangeQuery() throws IOException {
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices("blog_java888");

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.from(0);
        searchSourceBuilder.size(10);
        searchSourceBuilder.sort("id", SortOrder.ASC);
        searchSourceBuilder.fetchSource(new String[]{"content"}, null);
        // es超时时间
        searchSourceBuilder.timeout(new TimeValue(3, TimeUnit.SECONDS));
        QueryBuilder queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.rangeQuery("createTime").gte(new Date()));
        searchSourceBuilder.query(queryBuilder);
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        System.out.println(searchResponse);
    }

    /**
     * 根据id查询
     * @throws IOException
     */
    @Test
    public void testIdQuery() throws IOException {
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices("blog_java4");

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        QueryBuilder queryBuilder = QueryBuilders.idsQuery().addIds("0", "1", "3");
        searchSourceBuilder.query(queryBuilder);
        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        System.out.println(searchResponse);

    }

    /**
     * 高亮显示
     */
    @Test
    public void testHighlightFields() throws IOException {
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices("blog_java4");

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.from(0);
        searchSourceBuilder.size(10);
        searchSourceBuilder.sort("id", SortOrder.ASC);

        QueryBuilder queryBuilder = QueryBuilders.matchPhraseQuery("title", "这条数据来自于JSON");
        searchSourceBuilder.query(queryBuilder);

        HighlightBuilder highlightBuilder = new HighlightBuilder();
        highlightBuilder.preTags("<font style='color:red'>");
        highlightBuilder.postTags("</font>");
        highlightBuilder.field("title");
        searchSourceBuilder.highlighter(highlightBuilder);

        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits hits = searchResponse.getHits();
        for (SearchHit hit : hits) {
            System.out.println(hit.getHighlightFields());
            Text[] titles = hit.getHighlightFields().get("title").getFragments();
            for (Text title : titles) {
                System.out.println(title);
            }
        }


    }


}

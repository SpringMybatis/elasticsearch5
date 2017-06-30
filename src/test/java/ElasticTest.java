/**
 * Created by Administrator on 2017/6/29.
 */

import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.*;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * elastic测试类
 *
 * @author lierl
 * @create 2017-06-29 10:11
 **/
public class ElasticTest {

    private Client client;

    @Before
    public void before(){
        String clusterNode = "127.0.0.1:9300";

        Settings settings = Settings.builder().put("cluster.name", "lierl").build();
        client = new PreBuiltTransportClient(settings);
        try {
                String ip = clusterNode.split(":")[0];
                String port = clusterNode.split(":")[1];
                ((TransportClient) client).addTransportAddress(
                        new InetSocketTransportAddress(InetAddress.getByName(ip), Integer.parseInt(port)));
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void createIndex() throws IOException {
        for (int i = 0; i < 100; i++) {

            XContentBuilder builder = XContentFactory.jsonBuilder().startObject()
                    .field("sourceIp", "10.10.16." + i)
                    .field("sourcePort", 389)
                    .field("destIp", "114.114.114.114")
                    .endObject();


            IndexResponse response = client.prepareIndex("logs","logs2017",i+"")
                            .setSource(builder)
                            .execute().actionGet();

            DocWriteResponse.Result result = response.getResult();

            RestStatus status = response.status();
        }
    }

    @Test
    public void add(){

    }
    /*
    *Delete index 删除文档，相当于删除一行数据
    */
    @Test
    public void delete(){
        DeleteResponse deleteresponse = client.prepareDelete("logs", "log2015","150")
                .execute()
                .actionGet();
        deleteresponse.status();
        System.out.println(deleteresponse.getVersion());
    }

    @Test
    public void update() throws IOException {
        XContentBuilder endObject = XContentFactory.jsonBuilder().startObject().field("name","zs").endObject();

        UpdateResponse response = client.prepareUpdate("", "", "5").setDoc(endObject).get();

        response.getVersion();
        response.getResult();
    }

    /*
     * Get index 获取文档相当于读取数据库的一行数据
     */
    @Test
    public void query(){
        GetResponse response = client.prepareGet("logs", "log2015", "1")
                                        .execute()
                                        .actionGet();
        System.out.println(response.getSourceAsString());
    }

    @Test
    /*
     *search 查询相当于关系型数据库的查询
    */
    public void search(){


        SearchResponse searchresponse = client.prepareSearch("logs")
                .setTypes("log2015")
                /**
                 * QUERY_THEN_FETCH:查询是针对所有的块执行的，但返回的是足够的信息，而不是文档内容（Document）。结果会被排序和分级，基于此，只有相关的块的文档对象会被返回。由于被取到的仅仅是这些，故而返回的 hit 的大小正好等于指定的 size。这对于有许多块的 index 来说是很便利的（返回结果不会有重复的，因为块被分组了）
                   QUERY_AND_FETCH:最原始（也可能是最快的）实现就是简单的在所有相关的 shard上执行检索并返回结果。每个 shard 返回一定尺寸的结果。由于每个shard已经返回了一定尺寸的hit，这种类型实际上是返回多个 shard的一定尺寸的结果给调用者。
                   DFS_QUERY_THEN_FETCH：与 QUERY_THEN_FETCH 相同，预期一个初始的散射相伴用来为更准确的 score 计算分配了的term频率。
                   DFS_QUERY_AND_FETCH:与 QUERY_AND_FETCH 相同，预期一个初始的散射相伴用来为更准确的 score 计算分配了的term频率。
                   SCAN：在执行了没有进行任何排序的检索时执行浏览。此时将会自动的开始滚动结果集。
                   COUNT：只计算结果的数量，也会执行 facet。
                 */
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(QueryBuilders.termQuery("destIp", "114.114.114.114"))//匹配关键字
                .addSort("", SortOrder.DESC)//排序
                .setPostFilter(// Filter 表示范围
                    QueryBuilders.rangeQuery("sourceIp")
                            .from("10.10.16.57")
                            .to("10.10.16.68")
                ).setFrom(0)
                .setSize(3).setExplain(true)
                .execute().actionGet();
    }

    /**
     * 批量操作  新增、删除
     */
    @Test
    public void batch() throws IOException {
        BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
        IndexRequest indexRequest = new IndexRequest("index","type","id");

        XContentBuilder builder = XContentFactory.jsonBuilder().startObject()
                                        .field("name","miao")
                                        .field("age",26).endObject();
        indexRequest.source(builder);
        bulkRequestBuilder.add(indexRequest);

        DeleteRequest deleteRequest = new DeleteRequest("index","type","id");
        bulkRequestBuilder.add(deleteRequest);

        BulkResponse bulkResponse = bulkRequestBuilder.get();

        if(bulkResponse.hasFailures()){
            System.out.println("执行失败");
            BulkItemResponse[] items = bulkResponse.getItems();
            for (BulkItemResponse item : items) {
                String failureMessage = item.getFailureMessage();
                System.out.println(failureMessage);
            }
        }else{
            System.out.println("执行成功");
        }

    }

    /**
     * 查询，排序，分页，高亮，过滤
     * lt:小于
     * lte：小于等于
     * gt：大于
     * gte：大于等于
     * @throws Exception
     */
    @Test
    public void all(){

        HighlightBuilder builder = new HighlightBuilder();
        builder.field("name");
        builder.preTags("<font color='red'>");
        builder.postTags("</font>");


        SearchResponse searchResponse = client.prepareSearch("index")
                    .setTypes("type")
                    .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)//设置查询类型
                    .setQuery(QueryBuilders.matchQuery("name","miao"))
                    .setPostFilter(QueryBuilders.rangeQuery("age").gt(20).lt(30))//设置年龄范围
                    .setFrom(0).setSize(10)//设置分页
                    .highlighter(builder)//高亮显示
                    .addSort("age",SortOrder.ASC)
                    .get();
        SearchHits hits = searchResponse.getHits();
        long totalHits = hits.getTotalHits();
        for (SearchHit hit : hits) {
            Map<String, HighlightField> highlightFields = hit.getHighlightFields();

            HighlightField field = highlightFields.get("name");

            Text[] fragments = field.getFragments();

            System.out.println(hit.getSourceAsString());
            for (Text text : fragments) {
                System.out.println("高亮内容"+text);
            }
        }

    }

    /**
     * 类似于这个select count(*),name from table group by name;
     */
    @Test
    public void count(){
        SearchResponse response = client.prepareSearch("index")
                        .setTypes("type")
                        .addAggregation(AggregationBuilders.terms("nameterm").field("name").size(0))
                        .get();

        Terms terms = response.getAggregations().get("nameterm");
        List<Terms.Bucket> buckets = terms.getBuckets();

        for (Terms.Bucket bucket : buckets) {
            System.out.println(bucket.getKey()+"-->"+bucket.getDocCount());
        }

    }

    /**
     * 类似于select sum(age),name from table group by name;
     */
    @Test
    public void sum(){
        SearchResponse searchResponse = client.prepareSearch("index")
                        .setTypes("type")
                        .addAggregation(AggregationBuilders.terms("nameterm").field("name").
                                subAggregation(AggregationBuilders.sum("agesum").field("age")).size(0)).get();

        Terms terms = searchResponse.getAggregations().get("nameterm");

        List<Terms.Bucket> buckets = terms.getBuckets();
        for (Terms.Bucket bucket : buckets) {
            Sum sum = bucket.getAggregations().get("agesum");

            System.out.println(bucket.getKey()+"--->"+sum.getValue());
        }
    }

    /**
     * 测试multi get api
     * 从不同的index, type, 和id中获取
     */
    @Test
    public void multiGet(){
        MultiGetResponse multiGetItemResponses = client.prepareMultiGet().add("index1", "type1", "id1")
                .add("index2", "type2", "id2")
                .add("index3", "type3", "id3")
                .get();
        for (MultiGetItemResponse itemResponse : multiGetItemResponses) {
            GetResponse response = itemResponse.getResponse();
            if (response.isExists()) {
                String sourceAsString = response.getSourceAsString();
                response.getSourceAsMap();
                System.out.println(sourceAsString);
            }
        }
    }

    //bulk API允许批量提交index和delete请求

    /**
     * 使用bulk processor
     * 当请求超过10000个（default=1000）或者总大小超过1GB（default=5MB）时，触发批量提交动作
     */
    @Test
    public void bulkproccessor() throws InterruptedException {
        // 创建BulkPorcessor对象
        BulkProcessor bulkProcessor = BulkProcessor.builder(client, new BulkProcessor.Listener() {

            //beforeBulk会在批量提交之前执行，可以从BulkRequest中获取请求信息
            // request.requests()或者请求数量request.numberOfActions()
            public void beforeBulk(long paramLong, BulkRequest paramBulkRequest) {
            }

            //在批量失败后执行
            public void afterBulk(long paramLong, BulkRequest paramBulkRequest, Throwable paramThrowable) {
            }
            //在批量成功后执行，可以跟beforeBulk配合计算批量所需时间
            public void afterBulk(long paramLong, BulkRequest paramBulkRequest, BulkResponse paramBulkResponse) {
            }
        })
        // 1w次请求执行一次bulk
        .setBulkActions(10000)
        // 1gb的数据刷新一次bulk
        .setBulkSize(new ByteSizeValue(1, ByteSizeUnit.GB))
        // 固定5s必须刷新一次
        .setFlushInterval(TimeValue.timeValueSeconds(5))
        // 并发请求数量, 0不并发, 1并发允许执行
        .setConcurrentRequests(1)
        // 设置退避, 100ms后执行, 最大请求3次
        .setBackoffPolicy(
                BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), 3))
        .build();

        // 添加单次请求
        bulkProcessor.add(new IndexRequest("twitter", "tweet", "1"));
        bulkProcessor.add(new DeleteRequest("twitter", "tweet", "2"));

        // 关闭
        bulkProcessor.awaitClose(10, TimeUnit.MINUTES);
        // 或者
        bulkProcessor.close();
    }


    @After
    public void after(){
        client.close();
    }
}

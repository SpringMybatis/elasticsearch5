package com.elasticsearch.service.impl;

import com.elasticsearch.service.IElasticSearchService;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.ObjectUtils;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * Created by Administrator on 2017/6/27.
 */
@Service
public class ElasticSearchServiceImpl implements IElasticSearchService {

    @Autowired
    private Client client;

    /**
     * 将Map转换成builder
     *c
     * @param mapParam
     * @return
     * @throws Exception
     */
    private XContentBuilder createMapJson(Map<String, String> mapParam) throws Exception {
        XContentBuilder source = XContentFactory.jsonBuilder().startObject();

        for (Map.Entry<String, String> entry : mapParam.entrySet()) {
            source.field(entry.getKey(), entry.getValue());
        }

        source.endObject();

        return source;
    }

    /**
     * 将实体转换成json
     *
     * @param entity 实体
     * @param fieldNames 实体中待转换成json的字段
     * @return 返回json
     * @throws Exception
     */
    private XContentBuilder createEntityJson(Object entity, String... fieldNames) throws Exception {
        // 创建json对象, 其中一个创建json的方式
        XContentBuilder source = XContentFactory.jsonBuilder().startObject();
        try {
            if(ArrayUtils.isNotEmpty(fieldNames)) {
                for (String fieldName : fieldNames) {
                    Field field = entity.getClass().getDeclaredField(fieldName);

                    if (field == null) {
                        throw new Exception("实体类中无此属性");
                    }

                    String fieldValue = ObjectUtils.toString(field.get(entity));
                    // 避免和elasticSearch中id字段重复
                    if (fieldName == "_id") {
                        fieldName = "id";
                    }

                    source.field(fieldName, fieldValue);
                }
            }else{
                Field[] fields = entity.getClass().getDeclaredFields();
                if(fields.length == 0){
                    throw new Exception("实体类无属性");
                }

                for (Field field : fields) {

                    String fieldName = field.getName();

                    String fieldValue = ObjectUtils.toString(field.get(entity));
                    // 避免和elasticSearch中id字段重复
                    if (fieldName == "_id") {
                        fieldName = "id";
                    }

                    source.field(fieldName, fieldValue);

                }
            }
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
            System.out.println("未找到方法！");
        }

        source.endObject();
        return source;
    }

    /**
     * 将一个Map格式的数据（key,value）插入索引 （私有方法）
     *
     * @param type 类型（对应数据库表）
     * @param docId id，对应elasticSearch中的_id字段
     * @param mapParam Map格式的数据
     * @return
     */
    public boolean addMapDocToIndex(String type, String docId,String indexname, Map<String, String> mapParam) {
        boolean result = false;

        XContentBuilder source = null;
        try {
            source = createMapJson(mapParam);
        } catch (Exception e) {
            e.printStackTrace();
        }

        // 存json入索引中
        IndexResponse response = null;
        if (docId == null) {
            // 使用默认的id
            response = client.prepareIndex(indexname, type).setSource(source).get();
        } else {
            response = client.prepareIndex(indexname, type, docId).setSource(source).get();
        }

        // 插入结果获取
        String index = response.getIndex();
        String gettype = response.getType();
        String id = response.getId();
        long version = response.getVersion();
        RestStatus status = response.status();

        String strResult = "新增文档成功：" + index + " : " + gettype + ": " + id + ": " + version + ": " + status.getStatus();
        System.out.println(strResult);

        if (status.getStatus() == 201) {
            result = true;
        }

        // 关闭client
        client.close();
        return result;
    }

    /**
     * 将一个实体存入到默认索引的类型中（指定_id，一般是业务数据的id，及elasticSearch和关系型数据使用同一个id，方便同关系型数据库互动）
     * （私有方法）
     *
     * @param type 类型（对应数据库表）
     * @param docId id，对应elasticSearch中的_id字段
     * @param entity 要插入的实体
     * @param fieldNames 需要将实体中哪些属性作为字段,可以传递 ""来表示所有的属性都作为字段添加到es中
     * @return
     */
    public boolean addEntityDoc(String type, String docId,String indexname, Object entity, String... fieldNames) {
        boolean result = false;

        XContentBuilder source = null;
        try {
            source = createEntityJson(entity, fieldNames);
        } catch (Exception e) {
            e.printStackTrace();
        }

        // 存json入索引中
        IndexResponse response = null;
        if (docId == null) {
            // 使用默认的id
            response = client.prepareIndex(indexname, type).setSource(source).get();
        } else {
            response = client.prepareIndex(indexname, type, docId).setSource(source).get();
        }

        // 插入结果获取
        String index = response.getIndex();
        String gettype = response.getType();
        String id = response.getId();
        long version = response.getVersion();
        RestStatus status = response.status();

        String strResult = "新增文档成功：" + index + " : " + gettype + ": " + id + ": " + version
                + ": " + status.getStatus();
        System.out.println(strResult);

        if (status.getStatus() == 201) {
            result = true;
        }

        // 关闭client
        client.close();

        return result;
    }

    /**
     * 删除文档
     *
     * @param type 类型（对应数据库表）
     * @param docId 类型中id
     * @return
     */
    public boolean deleteDoc(String type, String docId,String indexname) {
        boolean result = false;

        DeleteResponse deleteresponse = client.prepareDelete(indexname, type, docId).get();

        System.out.println("删除结果：" + deleteresponse.getResult().toString());
        if (deleteresponse.getResult().toString() == "DELETED") {
            result = true;
        }

        // 关闭client
        client.close();

        return result;
    }

    /**
     * 修改文档
     *
     * @param type 类型
     * @param docId 文档id
     * @param updateParam 需要修改的字段和值
     * @return
     */
    public boolean updateDoc(String type, String docId,String indexname, Map<String, String> updateParam) throws Exception {
        String strResult = "";
        boolean result = false;

        //设置查询条件, 查找不到则添加生效

        //IndexRequest indexRequest = new IndexRequest(indexname,type,docId).source(createMapJson(updateParam));


        UpdateRequest updateRequest = new UpdateRequest();
        updateRequest.index(indexname);
        updateRequest.type(type);
        updateRequest.id(docId);
        try {
            XContentBuilder json = createMapJson(updateParam);//需要更新的数据
            updateRequest.doc(json);
                    //.upsert(indexRequest);//查找不到则更新
        } catch (Exception e) {
            e.printStackTrace();
        }
        try {
            strResult = client.update(updateRequest).get().getResult().toString();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        System.out.println(strResult);

        if (strResult == "UPDATED") {
            result = true;
        }

        return result;
    }

    /**
     * 高亮搜索
     *
     * @param type 类型
     * @param fieldName 段
     * @param keyword 关键词
     * @param from 开始行数
     * @param size 每页大小
     * @return
     */
    public Map<String, Object> searchDocHighlight(String type,String indexname, String fieldName, String keyword, int from, int size) {
        // 高亮
        HighlightBuilder hiBuilder = new HighlightBuilder();
        hiBuilder.preTags("<span style=\"color:red\">");
        hiBuilder.postTags("</span>");
        hiBuilder.field(fieldName);

        QueryBuilder queryBuilder = QueryBuilders.matchPhraseQuery(fieldName, keyword);

        SearchRequestBuilder responsebuilder = client.prepareSearch(indexname).setTypes(type);
        responsebuilder.setQuery(queryBuilder);
        responsebuilder.highlighter(hiBuilder);
        responsebuilder.setFrom(from);
        responsebuilder.setSize(size);
        responsebuilder.setExplain(true);

        SearchResponse myresponse = responsebuilder.execute().actionGet();
        SearchHits searchHits = myresponse.getHits();

        // 总命中数
        long total = searchHits.getTotalHits();
        Map<String, Object> map = Maps.newHashMap();
        map.put("total", total);
        List<Map<String, Object>> list = Lists.newArrayList();

        for (int i = 0; i < searchHits.getHits().length; i++) {
            Map<String, HighlightField> highlightFields = searchHits.getHits()[i].getHighlightFields();

            // 段高亮
            HighlightField titleField = highlightFields.get(fieldName);
            Map<String, Object> source = searchHits.getHits()[i].getSource();
            if (titleField != null) {
                Text[] fragments = titleField.fragments();
                String name = "";
                for (Text text : fragments) {
                    name += text;
                }
                source.put(fieldName, name);
            }

            list.add(source);
        }
        map.put("rows", list);

        return map;
    }

    /**
     * or条件查询高亮
     *
     * @param type 类型
     * @param shouldMap or条件和值
     * @param from 开始行数
     * @param size 每页大小
     * @return
     */
    public Map<String, Object> multiOrSearchDocHigh(String type,String indexname, Map<String, String> shouldMap, int from,
                                                    int size) {
        SearchRequestBuilder responsebuilder = client.prepareSearch(indexname).setTypes(type);
        responsebuilder.setFrom(from);
        responsebuilder.setSize(size);
        responsebuilder.setExplain(true);

        // 高亮
        HighlightBuilder hiBuilder = new HighlightBuilder();
        hiBuilder.preTags("<span style=\"color:red\">");
        hiBuilder.postTags("</span>");

        // 高亮每个字段
        for (String key : shouldMap.keySet()) {
            hiBuilder.field(key);
        }

        responsebuilder.highlighter(hiBuilder);

        if (null != shouldMap && shouldMap.size() > 0) {
            // 创建一个查询
            BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();

            // 这里查询的条件用map传递
            for (String key : shouldMap.keySet()) {
                queryBuilder.should(QueryBuilders.matchPhraseQuery(key, shouldMap.get(key)));// or连接条件
            }
            // 查询
            responsebuilder.setQuery(queryBuilder);
        }

        SearchResponse myresponse = responsebuilder.execute().actionGet();
        SearchHits searchHits = myresponse.getHits();

        // 总命中数
        long total = searchHits.getTotalHits();
        Map<String, Object> map = Maps.newHashMap();
        map.put("total", total);
        List<Map<String, Object>> list = Lists.newArrayList();
        for (int i = 0; i < searchHits.getHits().length; i++) {
            Map<String, HighlightField> highlightFields = searchHits.getHits()[i].getHighlightFields();
            Map<String, Object> source = searchHits.getHits()[i].getSource();

            for (String key : shouldMap.keySet()) {
                // 各个段进行高亮
                HighlightField titleField = highlightFields.get(key);
                if (titleField != null) {
                    Text[] fragments = titleField.fragments();
                    String name = "";
                    for (Text text : fragments) {
                        name += text;
                    }
                    source.put(key, name);
                }
            }

            list.add(source);
        }
        map.put("rows", list);

        return map;
    }

    /**
     * 搜索
     *
     * @param type 类型
     * @param fieldName 待搜索的字段
     * @param keyword 待搜索的关键词
     * @param from 开始行数
     * @param size 每页大小
     * @return
     */
    public Map<String, Object> searchDoc(String type,String indexname, String fieldName, String keyword, int from, int size) {
        List<String> hitResult = Lists.newArrayList();

        QueryBuilder queryBuilder = QueryBuilders.matchPhraseQuery(fieldName, keyword);

        SearchRequestBuilder responsebuilder = client.prepareSearch(indexname).setTypes(type);
        responsebuilder.setQuery(queryBuilder);
        responsebuilder.setFrom(from);
        responsebuilder.setSize(size);
        responsebuilder.setExplain(true);

        SearchResponse myresponse = responsebuilder.execute().actionGet();
        SearchHits hits = myresponse.getHits();
        for (int i = 0; i < hits.getHits().length; i++) {
            hitResult.add(hits.getHits()[i].getSourceAsString());
        }

        // 将命中结果转换成Map输出
        Map<String, Object> modelMap = Maps.newHashMapWithExpectedSize(2);
        modelMap.put("total", hitResult.size());
        modelMap.put("rows", hitResult);

        return modelMap;
    }

    /**
     * 多个条件进行or查询
     *
     * @param type 类型
     * @param shouldMap 进行or查询的段和值
     * @param from 开始行数
     * @param size 每页大小
     * @return
     */
    public Map<String, Object> multiOrSearchDoc(String type,String indexname, Map<String, String> shouldMap, int from, int size) {
        List<String> hitResult = Lists.newArrayList();

        SearchRequestBuilder responsebuilder = client.prepareSearch(indexname).setTypes(type);
        responsebuilder.setFrom(from);
        responsebuilder.setSize(size);
        responsebuilder.setExplain(true);

        if (null != shouldMap && shouldMap.size() > 0) {
            // 创建一个查询
            BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();

            // 这里查询的条件用map传递
            for (String key : shouldMap.keySet()) {
                queryBuilder.should(QueryBuilders.matchPhraseQuery(key, shouldMap.get(key)));// or连接条件
            }
            // 查询
            responsebuilder.setQuery(queryBuilder);
        }

        SearchResponse myresponse = responsebuilder.execute().actionGet();
        SearchHits hits = myresponse.getHits();
        for (int i = 0; i < hits.getHits().length; i++) {
            hitResult.add(hits.getHits()[i].getSourceAsString());
        }

        // 将命中结果转换成Map输出
        Map<String, Object> modelMap = Maps.newHashMapWithExpectedSize(2);
        modelMap.put("total", hitResult.size());
        modelMap.put("rows", hitResult);

        return modelMap;
    }

    /**
     * 多个条件进行and查询
     *
     * @param type 类型
     * @param mustMap 进行and查询的段和值
     * @param from 开始行数
     * @param size 每页大小
     * @return
     */
    public Map<String, Object> multiAndSearchDoc(String type,String indexname, Map<String, String> mustMap, int from, int size) {
        List<String> hitResult = Lists.newArrayList();

        SearchRequestBuilder responsebuilder = client.prepareSearch(indexname).setTypes(type);
        //设置查询类型
        responsebuilder.setSearchType(SearchType.DFS_QUERY_THEN_FETCH);
        //设置分页信息
        responsebuilder.setFrom(from).setSize(size);
        //按照某属性排序
//        responsebuilder.addSort("", SortOrder.ASC);
        // 设置是否按查询匹配度排序
        responsebuilder.setExplain(true);

        if (null != mustMap && mustMap.size() > 0) {
            // 创建一个查询
            BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();

            // 这里查询的条件用map传递
            for (String key : mustMap.keySet()) {
                queryBuilder.must(QueryBuilders.matchPhraseQuery(key, mustMap.get(key)));// and查询
            }
            // 查询
            responsebuilder.setQuery(queryBuilder);
        }

        SearchResponse myresponse = responsebuilder.execute().actionGet();
        SearchHits hits = myresponse.getHits();
        hits.getTotalHits();//总数
        for (int i = 0; i < hits.getHits().length; i++) {
            hitResult.add(hits.getHits()[i].getSourceAsString());
        }

        // 将命中结果转换成Map输出
        Map<String, Object> modelMap = Maps.newHashMapWithExpectedSize(2);
        modelMap.put("total", hitResult.size());
        modelMap.put("rows", hitResult);

        return modelMap;
    }

    public boolean addMapDocToIndex(String type,String indexname, Map<String, String> mapParam) {
        return addMapDocToIndex(type, null, indexname, mapParam);
    }

    public boolean addEntityDoc(String type,String indexname, Object entity, String... methodNameParm) {
        return addEntityDoc(type,null, indexname, entity, methodNameParm);
    }
}


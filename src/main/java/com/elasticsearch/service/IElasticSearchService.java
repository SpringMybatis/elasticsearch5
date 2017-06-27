package com.elasticsearch.service;

import java.util.Map;

/**
 * Created by Administrator on 2017/6/27.
 */
public interface IElasticSearchService {
    /**
     * 将一个Map格式的数据（key,value）插入索引（指定_id，一般是业务数据的id，及elasticSearch和关系型数据使用同一个id，方便同关系型数据库互动）
     *
     * @param type 类型（对应数据库表）
     * @param docId id，对应elasticSearch中的_id字段
     * @param mapParam Map格式的数据
     * @return
     */
    public boolean addMapDocToIndex(String type, String docId,String indexname,Map<String, String> mapParam);

    /**
     * 将一个Map格式的数据（key,value）插入索引 （使用默认_id）
     *
     * @param type 类型（对应数据库表）
     * @param mapParam Map格式的数据
     * @return
     */
    public boolean addMapDocToIndex(String type,String indexname, Map<String, String> mapParam) ;

    /**
     * 将一个实体存入到默认索引的类型中（默认_id）
     *
     * @param type 类型（对应数据库表）
     * @param entity 要插入的实体
     * @param methodNameParm 需要将实体中哪些属性作为字段
     * @return
     */
    public boolean addEntityDoc(String type,String indexname, Object entity, String... methodNameParm);

    /**
     * 将一个实体存入到默认索引的类型中（指定_id，一般是业务数据的id，及elasticSearch和关系型数据使用同一个id，方便同关系型数据库互动）
     *
     * @param type 类型（对应数据库表）
     * @param docId id，对应elasticSearch中的_id字段
     * @param entity 要插入的实体
     * @param methodNameParm 需要将实体中哪些属性作为字段
     * @return
     */
    public boolean addEntityDoc(String type, String docId, String indexname,Object entity, String... methodNameParm);

    /**
     * 删除文档
     *
     * @param type 类型（对应数据库表）
     * @param docId 类型中id
     * @return
     */
    public boolean deleteDoc(String type, String docId,String indexname);

    /**
     * 修改文档
     *
     * @param type 类型
     * @param docId 文档id
     * @param updateParam 需要修改的字段和值
     * @return
     */
    public boolean updateDoc(String type, String docId,String indexname, Map<String, String> updateParam);

    // --------------------以下是各种搜索方法--------------------------

    /**
     * 高亮搜索
     *
     * @param type 类型
     * @param fieldName 段
     * @param keyword 段值
     * @return
     */
    public Map<String, Object> searchDocHighlight(String type,String indexname, String fieldName, String keyword, int from, int size);

    /**
     * or条件查询高亮
     *
     * @param type 类型
     * @param shouldMap or条件和值
     * @return
     */
    public Map<String, Object> multiOrSearchDocHigh(String type,String indexname, Map<String, String> shouldMap, int from,int size);

    /**
     * 搜索
     *
     * @param type 类型
     * @param fieldName 待搜索的字段
     * @param keyword 待搜索的关键词
     */
    public Map<String, Object> searchDoc(String type,String indexname, String fieldName, String keyword, int from, int size);

    /**
     * 多个条件进行or查询
     *
     * @param type 类型
     * @param shouldMap 进行or查询的段和值
     * @return
     */
    public Map<String, Object> multiOrSearchDoc(String type,String indexname, Map<String, String> shouldMap, int from, int size);

    /**
     * 多个条件进行and查询
     *
     * @param type 类型
     * @param mustMap 进行and查询的段和值
     * @return
     */
    public Map<String, Object> multiAndSearchDoc(String type,String indexname, Map<String, String> mustMap, int from, int size);
}

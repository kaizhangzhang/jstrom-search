package com.netease.kaola.distmerchant.common;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.FailedException;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.netease.kaola.distmerchant.common.util.BoltUtil;
import com.netease.kaola.distmerchant.common.util.ElasticSearchFactory;
import com.netease.kaola.distmerchant.common.util.LogInfoFormat;
import com.netease.kaola.distmerchant.common.util.PageParam;
import com.netease.kaola.distmerchant.topology.xml.BoltXmlBean;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * @author kai.zhang
 * @description 搜索器，默认根据docId查询，存在则根据docId更新，否则插入
 * @since 2019/1/30
 */
public class ElasticSearchSaveBolt extends BaseBasicBolt {
    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchSaveBolt.class);
    protected BoltXmlBean boltXmlBean;
    protected Map<String, Object> searchConf = new HashMap<>();
    protected RestHighLevelClient client;
    protected String elasticIndex;
    protected String elasticType;

    public ElasticSearchSaveBolt(BoltXmlBean boltXmlBean) {
        this.boltXmlBean = boltXmlBean;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        this.searchConf = (Map<String, Object>) stormConf.get(boltXmlBean.getDatasource());
        //初始化search
        int port = Integer.valueOf(searchConf.get(Constants.ELASTIC_PORT).toString());
        elasticIndex = (String) searchConf.get(Constants.ELASTIC_INDEX);
        elasticType = (String) searchConf.get(Constants.ELASTIC_TYPE);
        client = ElasticSearchFactory.getClient((String)this.searchConf.get(Constants.ELASTIC_HOST), port, (String)this.searchConf.get(Constants.ELASTIC_SCHEMA));
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        try {
            Map<String, Object> allReciveEmitData = BoltUtil.tuple2Map(input);
            //构建查询条件字段
            List<String> queryConditions = this.buildSearchGetCondition(input);
            //构建推送search数据
            Map<String, Object> searchParams = this.buildSend2SearchParams(allReciveEmitData);
            //获取routingkey
            String routingKey = getRoutingKey(allReciveEmitData);
            save2Search(searchParams, queryConditions, allReciveEmitData, routingKey);
            if (StringUtils.isNotBlank(boltXmlBean.getOutputfield()) && !allReciveEmitData.isEmpty()) {
                collector.emit(new Values(allReciveEmitData));
            }
        } catch (Exception e) {
            logger.error(LogInfoFormat.formatBoltLog(boltXmlBean.getId()), e);
            throw new FailedException(LogInfoFormat.formatBoltLog(boltXmlBean.getId()), e);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        if (StringUtils.isNotBlank(boltXmlBean.getOutputfield())) {
            declarer.declare(new Fields(boltXmlBean.getOutputfield()));
        }
    }

    /**
     * 根据outputcolums构建推送到elastic search的内容信息
     * @return
     */
    public Map<String, Object> buildSend2SearchParams(Map<String, Object> allReciveEmitData) {
        Map<String, Object> searchParams = new HashMap<>();
        List<String> outputFileds = BoltUtil.parseOutputcolumsFields(boltXmlBean.getOutputcolums());
        if (CollectionUtils.isNotEmpty(outputFileds)) {
            for (String field : outputFileds) {
                searchParams.put(field, allReciveEmitData.get(field));
            }
        }
        return searchParams;
    }

    /**
     * 构建查询字段，默认根据上一个节点outputField查询，可覆盖此方法修改查询条件
     * @return
     */
    public List<String> buildSearchGetCondition(Tuple input) {
        List<String> queryList = new ArrayList<>();
        if (input.getFields() != null && input.getFields().size() > 0) {
            queryList.addAll(input.getFields().toList());
        }
        return queryList;
    }

    public String getRoutingKey(Map<String, Object> allReciveEmitData) {
        return null;
    }
    public void save2Search(Map<String,Object> searchParams, List<String> queryConditions, Map<String, Object> allReciveEmitData, String routingKey) throws IOException {
        Object docId = null;
        if (CollectionUtils.isNotEmpty(queryConditions)) {
            docId = allReciveEmitData.get(queryConditions.get(0));
        }
        if (docId == null) {
            logger.error(LogInfoFormat.formatBoltLog(boltXmlBean.getId()) + "save2Search docId is null, queryConditions = " + queryConditions);
            throw new FailedException(LogInfoFormat.formatBoltLog(boltXmlBean.getId()) + "save2Search docId is null, queryConditions = " + queryConditions);
        }
        this.insertSearch(String.valueOf(docId), searchParams, routingKey);
    }

    public boolean updateSearch(String docId, Map<String, Object> updateMap, String routingKey) throws IOException {
        UpdateRequest updateRequest = new UpdateRequest(elasticIndex, elasticType, docId);
        updateRequest.doc(updateMap);
        if (StringUtils.isNotBlank(routingKey)) {
            updateRequest.routing(routingKey);
        }
        UpdateResponse updateResponse = client.update(updateRequest, RequestOptions.DEFAULT);
        return DocWriteResponse.Result.UPDATED.equals(updateResponse.getResult());
    }

    /**
     * 根据docId批量更新
     * @param updateDocIdDataMap
     * @return
     */
    public boolean batchUpdateSearch(Map<String, Map<String, Object>> updateDocIdDataMap, String routingKey) throws IOException {
        if (updateDocIdDataMap == null || updateDocIdDataMap.isEmpty()) {
            throw new RuntimeException("batchUpdateSearch updateDocIdDataMap is empty");
        }
        BulkRequest bulkRequest = new BulkRequest();
        for (Map.Entry<String, Map<String, Object>> entry : updateDocIdDataMap.entrySet()) {
            UpdateRequest updateRequest = new UpdateRequest(elasticIndex, elasticType, entry.getKey());
            updateRequest.doc(entry.getValue());
            if (StringUtils.isNotBlank(routingKey)) {
                updateRequest.routing(routingKey);
            }
            bulkRequest.add(updateRequest);
        }
        BulkResponse bulk = client.bulk(bulkRequest, RequestOptions.DEFAULT);
        if (bulk.hasFailures()) {
            logger.error(LogInfoFormat.formatBoltLog(boltXmlBean.getId()) + "batchUpdateSearch Exception: " + bulk.buildFailureMessage());
            throw new RuntimeException(bulk.buildFailureMessage());
        }
        return bulk.hasFailures();

    }
    /**
     * 根据主键id删除
     * @param docId
     * @return
     */
    public boolean deleteSearch(String docId, String routingKey) throws IOException {
        DeleteRequest deleteRequest = new DeleteRequest(elasticIndex, elasticType, docId);
        if (StringUtils.isNotBlank(routingKey)) {
            deleteRequest.routing(routingKey);
        }
        DeleteResponse deleteResponse = client.delete(deleteRequest, RequestOptions.DEFAULT);
        return DocWriteResponse.Result.DELETED.equals(deleteResponse.getResult());
    }

    /**
     * 批量删除docIds
     * @param docIdList
     * @return
     * @throws IOException
     */
    public boolean batchDeteteSerach(List<String> docIdList, String routingKey) throws IOException {
        if (CollectionUtils.isEmpty(docIdList)) {
            throw new RuntimeException("empty docIdList");
        }
        BulkRequest bulkRequest = new BulkRequest();
        for (String docId : docIdList) {
            DeleteRequest deleteRequest = new DeleteRequest(elasticIndex, elasticType, docId);
            if (StringUtils.isNotBlank(routingKey)) {
                deleteRequest.routing(routingKey);
            }
            bulkRequest.add(deleteRequest);
        }
        BulkResponse bulk = client.bulk(bulkRequest, RequestOptions.DEFAULT);
        if (bulk.hasFailures()) {
            logger.error(LogInfoFormat.formatBoltLog(boltXmlBean.getId()) + "batchDeteteSerach Exception: " + bulk.buildFailureMessage());
            throw new RuntimeException(bulk.buildFailureMessage());
        }
        return bulk.hasFailures();
    }

    /**
     * 根据查询条件删除
     * @param deleteParam
     * @return
     * @throws IOException
     */
    public long deleteByQuery(Map<String, Object> deleteParam, String routingKey) throws IOException {
        DeleteByQueryRequest deleteByQueryRequest = new DeleteByQueryRequest();
        deleteByQueryRequest.indices(elasticIndex);
        deleteByQueryRequest.types(elasticType);
        if (StringUtils.isNotBlank(routingKey)) {
            deleteByQueryRequest.setRouting(routingKey);
        }
        BoolQueryBuilder boolQueryBuilder = buildBooleanQueryBuilder(deleteParam);
        deleteByQueryRequest.setQuery(boolQueryBuilder);
        BulkByScrollResponse bulkByScrollResponse = client.deleteByQuery(deleteByQueryRequest, RequestOptions.DEFAULT);
        return bulkByScrollResponse.getDeleted();
    }
    /**
     * 插入, elastic index()会判断docId是否存在，自行决策执行更新还是插入
     * @param docId null值: docid不传elastic search自动生成
     * @param insertMap
     * @return
     * @throws IOException
     */
    public boolean insertSearch(String docId, Map<String, Object> insertMap, String routingKey) throws IOException {
        if (insertMap == null || insertMap.isEmpty()) {
            throw new RuntimeException(LogInfoFormat.formatBoltLog(boltXmlBean.getId()) + "insertSearch empty map");
        }
        IndexRequest indexRequest = new IndexRequest(elasticIndex, elasticType);
        indexRequest.source(insertMap);
        if (docId != null) {
            indexRequest.id(docId);
        }
        if (StringUtils.isNotBlank(routingKey)) {
            indexRequest.routing(routingKey);
        }
        IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
        return DocWriteResponse.Result.CREATED.equals(indexResponse.getResult());
    }

    /**
     *  elastic index()会判断docId是否存在，自行决策执行更新还是插入
     * @param docId null值: docid不传elastic search自动生成
     * @param parentId
     * @param insertMap
     * @return
     * @throws IOException
     */
    public boolean insertSearch(String docId, String parentId, Map<String, Object> insertMap) throws IOException {
        if (insertMap == null || insertMap.isEmpty()) {
            throw new RuntimeException(LogInfoFormat.formatBoltLog(boltXmlBean.getId()) + "insertSearch empty map");
        }
        IndexRequest indexRequest = new IndexRequest(elasticIndex, elasticType);
        indexRequest.source(insertMap);
        indexRequest.routing(parentId);
        if (docId != null) {
            indexRequest.id(docId);
        }
        IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
        return DocWriteResponse.Result.CREATED.equals(indexResponse.getResult());
    }

    /**
     * 根据docId批量插入 elastic index()会判断docId是否存在，自行决策执行更新还是插入
     * @param docIdDataMap key: docId, value: search value map
     * @return
     */
    public boolean batchInsertByDocIds(Map<String, Map<String, Object>> docIdDataMap, String routingKey) throws IOException {
        if (docIdDataMap == null || docIdDataMap.isEmpty()) {
            throw new RuntimeException("batchInsertByDocIds docIdDataMap is empty");
        }
        BulkRequest bulkRequest = new BulkRequest();
        for (Map.Entry<String, Map<String, Object>> entry : docIdDataMap.entrySet()) {
            IndexRequest indexRequest = new IndexRequest(elasticIndex, elasticType);
            indexRequest.source(entry.getValue());
            indexRequest.id(entry.getKey());
            if (StringUtils.isNotBlank(routingKey)) {
                indexRequest.routing(routingKey);
            }
            bulkRequest.add(indexRequest);
        }
        BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
        if (bulkResponse.hasFailures()) {
            logger.error(LogInfoFormat.formatBoltLog(boltXmlBean.getId()) + "batchInsertByDocIds Exception: " + bulkResponse.buildFailureMessage());
            throw new RuntimeException(bulkResponse.buildFailureMessage());
        }
        return bulkResponse.hasFailures();
    }
    /**
     * 根据主键id查询
     * @param docId
     * @return  _doc_id取主键
     * @throws IOException
     */
    public Map<String, Object> querySearch(String docId, String routingKey) throws IOException {
        Map<String, Object> queryResult = new HashMap<>();
        GetRequest getRequest = new GetRequest(elasticIndex, elasticType, docId);
        if (StringUtils.isNotBlank(routingKey)) {
            getRequest.routing(routingKey);
        }
        GetResponse getResponse = client.get(getRequest, RequestOptions.DEFAULT);
        queryResult = getResponse.getSourceAsMap();
        queryResult.put(Constants.ELASTIC_DOC_ID, getResponse.getId());
        return queryResult;
    }

    /**
     *
     * @param searchQueryParams
     * @param pageParam
     * @return _doc_id取主键
     * @throws IOException
     */
    public List<Map<String, Object>> querySearch(Map<String, Object> searchQueryParams, PageParam pageParam, String routingKey) throws IOException {
        if (searchQueryParams == null || searchQueryParams.isEmpty()) {
            return Collections.emptyList();
        }
        List<Map<String, Object>> resultList = new ArrayList<>();
        BoolQueryBuilder boolQueryBuilder = this.buildBooleanQueryBuilder(searchQueryParams);
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.from(pageParam.getLimitStart());
        sourceBuilder.size(pageParam.getPageSize());
        sourceBuilder.query(boolQueryBuilder);

        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices(elasticIndex);
        searchRequest.types(elasticType);
        searchRequest.source(sourceBuilder);
        if (StringUtils.isNotBlank(routingKey)) {
            searchRequest.routing(routingKey);
        }
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        for (SearchHit searchHit : searchResponse.getHits()) {
            Map<String, Object> sourceAsMap = searchHit.getSourceAsMap();
            sourceAsMap.put(Constants.ELASTIC_DOC_ID, searchHit.getId());
            resultList.add(sourceAsMap);
        }
        return resultList;
    }
    public BoolQueryBuilder buildBooleanQueryBuilder(Map<String, Object> searchQueryParams) {
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();
        for (Map.Entry<String, Object> entry : searchQueryParams.entrySet()) {
            queryBuilder.must(QueryBuilders.termQuery(entry.getKey(), entry.getValue()));
        }
        return queryBuilder;
    }
}

package com.pt;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.*;
import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedStringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.avg.AvgAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.avg.ParsedAvg;
import org.elasticsearch.search.aggregations.metrics.sum.ParsedSum;
import org.elasticsearch.search.aggregations.metrics.sum.SumAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.valuecount.ParsedValueCount;
import org.elasticsearch.search.aggregations.metrics.valuecount.ValueCountAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.util.CollectionUtils;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.stream.Collectors;

/**
 * @author nate-pt
 * @date 2021/12/8 15:09
 * @Since 1.8
 * @Description sql解析器
 */
public class SqlParser {

    private final Integer _default_to = 1;

    private final Integer _default_size = 10;

    /**
     * 利用栈的数据接口，模拟上一层的builder 被下一行栈中的数据包含   采用先进后出的概念
     */
    private final Stack<AggregationBuilder> groupColumnStack = new Stack<>();
    /**
     * 采用先进先出的概念进行设置
     */
    private final Queue<String> queue = new LinkedBlockingDeque<>();

    /**
     * 获取本地对应的es查询的客户端查询es中对应的数据
     */
    private RestHighLevelClient client = new RestHighLevelClient(
            RestClient.builder(
                    new HttpHost("127.0.0.1", 9200, "http")));

    /**
     * 设置查询语句的body内容
     */
    private PlainSelect body;

    public SqlParser(String sql) throws JSQLParserException {
        Select select = (Select) CCJSqlParserUtil.parse(sql);
        this.body = (PlainSelect) select.getSelectBody();
    }


    public SearchResult search0() throws IOException {
        // 查询的索引的名称
        String table = body.getFromItem().toString();
        SearchRequest searchRequest = new SearchRequest(table);
        //创建搜索文档内容对象
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        // 设置查询字段
        _build_select(searchSourceBuilder);
        // 设置分页
        _build_page(searchSourceBuilder);
        // 设置where 条件
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        List<QueryBuilder> must = boolQueryBuilder.must();
        List<QueryBuilder> mustNot = boolQueryBuilder.mustNot();
        _build_where(must, mustNot, body.getWhere());
        searchSourceBuilder.query(boolQueryBuilder);
        // 设置orderBy
        _build_order_by(searchSourceBuilder);

        // 设置Group by
        _build_group_by(searchSourceBuilder);

        searchRequest.source(searchSourceBuilder);
        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);

        // 查询最终汇总之后的结果对象
        return _build_result(response);
    }

    /**
     * 生成最终结果对象
     * @param response
     * @return
     */
    private SearchResult _build_result(SearchResponse response) {
        SearchResult result = new SearchResult();
        List<Map<String, Object>> collect = Arrays.stream(response.getHits().getHits()).map(SearchHit::getSourceAsMap).collect(Collectors.toList());
        result.setInfo(collect);
        if (queue.size() != 0) {
            // 查询agg
            Aggregations aggregations = response.getAggregations();
            ParsedStringTerms aggregation = aggregations.get(queue.poll());
            List<Map<String, Object>> agg = new ArrayList<>();
            List<String> polls = new ArrayList<>();
            String poll = null;
            while ((poll = queue.poll()) != null) {
                polls.add(poll);
            }
            _recursion_value(aggregation,"",agg,polls,-1);
            result.setAgg(agg);
        }
        return result;
    }

    /**
     * 通过计算获取对应value值
     * @param aggregation
     * @param key
     * @param agg
     * @param polls
     * @param i
     */
    private void _recursion_value(Aggregation aggregation, String key, List<Map<String, Object>> agg,List<String> polls,int i) {
        if (i==polls.size()-1){
            i=0;
        }
        String fin = key;
        if (aggregation instanceof ParsedStringTerms) {
            i++;
            for (Terms.Bucket bucket : ((ParsedStringTerms) aggregation).getBuckets()) {
                _recursion_value(bucket.getAggregations().get(polls.get(i)), fin+"_"+String.valueOf(bucket.getKey()), agg,polls,i);
            }

        } else {
            if(aggregation instanceof ParsedValueCount){
                ParsedValueCount count = (ParsedValueCount) aggregation;
                Map<String, Object> map = new HashMap<>();
                map.put("key", key.substring(1));
                map.put("value", count.value());
                agg.add(map);
            }

            if (aggregation instanceof ParsedSum) {
                ParsedSum count = (ParsedSum) aggregation;
                Map<String, Object> map = new HashMap<>();
                map.put("key", key.substring(1));
                map.put("value", count.value());
                agg.add(map);
            }

            if (aggregation instanceof ParsedAvg) {
                ParsedAvg count = (ParsedAvg) aggregation;
                Map<String, Object> map = new HashMap<>();
                map.put("key", key.substring(1));
                map.put("value", count.value());
                agg.add(map);
            }

        }
    }


//    private void _recursion_value(Terms.Bucket bucket, String key, List<Map<String, Object>> agg,String poll){
//        String s = String.valueOf(bucket.getKey());
//
//        Aggregation aggregation = bucket.getAggregations().get(poll);
//        if (aggregation instanceof ParsedStringTerms) {
//            String poll1 = queue.poll();
//            ParsedStringTerms aggregation1 = (ParsedStringTerms) aggregation;
//            for (Terms.Bucket aggregation1Bucket : aggregation1.getBuckets()) {
//                _recursion_value(aggregation1Bucket,key+s,agg,poll1);
//            }
//        }else {
//            ParsedValueCount count = (ParsedValueCount) aggregation;
//            Map<String,Object> map = new HashMap<>();
//            map.put("key",key);
//            map.put("value",count.value());
//            agg.add(map);
//        }
//    }

    private void _build_group_by(SearchSourceBuilder searchSourceBuilder) {
        GroupByElement groupBy = body.getGroupBy();
        if (groupBy == null) {
            return;
        }
        List<SelectItem> collect = body.getSelectItems().stream().filter(selectItem -> {
            SelectExpressionItem selectItem1 = (SelectExpressionItem) selectItem;
            return selectItem1.getExpression() instanceof Function;
        }).collect(Collectors.toList());

        if (CollectionUtils.isEmpty(collect)) {
            return;
        }
        List<String> columnNames = groupBy.getGroupByExpressions().stream().map(expression -> ((Column) expression).getColumnName()).collect(Collectors.toList());


        // 采用数据接口栈，进行实现
        Function expression = (Function) ((SelectExpressionItem) collect.get(0)).getExpression();
        String columnName1 = ((Column) expression.getParameters().getExpressions().get(0)).getColumnName();
        String name = expression.getName();

        for (String columnName : columnNames) {
            TermsAggregationBuilder field = AggregationBuilders.terms(columnName).field(columnName);
            groupColumnStack.push(field);
            queue.offer(columnName);
        }
        String key = name + "_" + columnName1;
        AggregationBuilder aggregationBuilder = null;
        if (name.equals("count")) {
            aggregationBuilder = AggregationBuilders.count(key).field("_id");
        } else if ("avg".equals(name)) {
            aggregationBuilder = AggregationBuilders.avg(key).field(columnName1);
        } else {
            aggregationBuilder = AggregationBuilders.sum(key).field(columnName1);
        }
        groupColumnStack.push(aggregationBuilder);
        queue.offer(key);
        AggregationBuilder finalP = groupColumnStack.pop();
        while (!groupColumnStack.isEmpty()) {
            AggregationBuilder pop = groupColumnStack.pop();
            pop.subAggregation(finalP);
            finalP = pop;
        }
        searchSourceBuilder.aggregation(finalP);
    }

    private void _build_order_by(SearchSourceBuilder searchSourceBuilder) {
        List<OrderByElement> orderByElements = body.getOrderByElements();
        if (CollectionUtils.isEmpty(orderByElements)) {
            return;
        }
        List<SortBuilder<?>> sorts = searchSourceBuilder.sorts();
        for (OrderByElement orderByElement : orderByElements) {
            FieldSortBuilder order = SortBuilders.fieldSort(orderByElement.getExpression().toString()).order(orderByElement.isAsc() ? SortOrder.ASC : SortOrder.DESC);
            sorts.add(order);
        }
    }

    /**
     * 设置where 查询条件
     *
     * @param must
     * @param mustNot
     */
    private void _build_where( List<QueryBuilder> must, List<QueryBuilder> mustNot, Expression where) {

        if (where instanceof AndExpression) {
            // 带条件查询
            AndExpression where1 = (AndExpression) where;
            Expression leftExpression = where1.getLeftExpression();
            _build_where( must, mustNot, leftExpression);
            Expression rightExpression = where1.getRightExpression();
            _build_where( must, mustNot, rightExpression);
        } else {
            // =
            if (where instanceof EqualsTo) {
                EqualsTo where1 = (EqualsTo) where;
                Expression leftExpression = where1.getLeftExpression();
                Expression rightExpression = where1.getRightExpression();
                MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery(leftExpression.toString(), _get_value(rightExpression));
                must.add(matchQueryBuilder);
            }
            // !=
            if (where instanceof NotEqualsTo) {
                NotEqualsTo where1 = (NotEqualsTo) where;
                Expression rightExpression = where1.getRightExpression();
                Expression leftExpression = where1.getLeftExpression();
                TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery(where1.getLeftExpression().toString(), _get_value(rightExpression));
                must.add(termQueryBuilder);
            }
            // in
            if (where instanceof InExpression) {
                InExpression where1 = (InExpression) where;
                ExpressionList rightExpression = (ExpressionList) where1.getRightExpression();
                Object[] objects = rightExpression.getExpressions().stream().map(this::_get_value).collect(Collectors.toList()).toArray();
                TermsQueryBuilder termsQueryBuilder = QueryBuilders.termsQuery(where1.getLeftExpression().toString(), objects);
                must.add(termsQueryBuilder);
            }
            // like
            if (where instanceof LikeExpression) {
                LikeExpression where1 = (LikeExpression) where;
                String s = (String) _get_value(where1.getRightExpression());
                FuzzyQueryBuilder fuzzyQueryBuilder = QueryBuilders.fuzzyQuery(where1.getLeftExpression().toString(), s.replace("%", ""));
                must.add(fuzzyQueryBuilder);
            }
            // > >= < <=
            if (where instanceof GreaterThan) {
                GreaterThan where1 = (GreaterThan) where;
                RangeQueryBuilder gt = QueryBuilders.rangeQuery(where1.getLeftExpression().toString()).gt(_get_value(where1.getRightExpression()));
                must.add(gt);
            }

            if (where instanceof GreaterThanEquals) {
                GreaterThanEquals where1 = (GreaterThanEquals) where;
                RangeQueryBuilder gte = QueryBuilders.rangeQuery(where1.getLeftExpression().toString()).gte(_get_value(where1.getRightExpression()));
                must.add(gte);
            }

            if (where instanceof MinorThan) {
                MinorThan where1 = (MinorThan) where;
                RangeQueryBuilder lt = QueryBuilders.rangeQuery(where1.getLeftExpression().toString()).lt(_get_value(where1.getRightExpression()));
                must.add(lt);
            }

            if (where instanceof MinorThanEquals) {
                MinorThanEquals where1 = (MinorThanEquals) where;
                RangeQueryBuilder lte = QueryBuilders.rangeQuery(where1.getLeftExpression().toString()).lte(_get_value(where1.getRightExpression()));
                must.add(lte);
            }


        }
    }

    /**
     * 设置分页
     *
     * @param searchSourceBuilder
     */
    private void _build_page(SearchSourceBuilder searchSourceBuilder) {
        Limit limit = body.getLimit();
        if (limit == null) {
            searchSourceBuilder.from((_default_to - 1) * _default_size);
            searchSourceBuilder.size(_default_size);
        } else {
            Integer offset = Integer.valueOf(limit.getOffset().toString());
            Integer rowCount = Integer.valueOf(limit.getRowCount().toString());

            searchSourceBuilder.from((offset - 1) * rowCount);
            searchSourceBuilder.size(rowCount);
        }
    }

    /**
     * 设置查询字段
     *
     * @param searchSourceBuilder
     */
    private void _build_select(SearchSourceBuilder searchSourceBuilder) {
        List<SelectItem> selectItems = body.getSelectItems();

        if (selectItems.size() == 1 || selectItems.get(0) instanceof AllColumns) {
            // 查询对应表中所有的字段
            // TODO 目前不做操作

        } else {
            // 制定字段查询
            List<String> collect = selectItems.stream().filter(selectItem -> {
                SelectExpressionItem selectItem1 = (SelectExpressionItem) selectItem;
                return selectItem1.getExpression() instanceof Column;
            }).map(selectItem -> selectItem.toString()).collect(Collectors.toList());
            String[] strings = collect.toArray(new String[collect.size()]);
            searchSourceBuilder.fetchSource(strings, new String[]{});
        }
    }


    private Object _get_value(Expression rightExpression) {
        if (rightExpression instanceof LongValue) {
            return ((LongValue) rightExpression).getValue();
        } else if (rightExpression instanceof StringValue) {
            return ((StringValue) rightExpression).getValue();
        } else if (rightExpression instanceof DoubleValue) {
            return ((DoubleValue) rightExpression).getValue();
        } else if (rightExpression instanceof TimeValue) {
            return ((TimeValue) rightExpression).getValue();
        } else {
            return rightExpression.toString();
        }


    }


}

package com.pt;


import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.NamedExpressionList;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.statement.values.ValuesStatement;
import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedStringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.valuecount.ParsedValueCount;
import org.elasticsearch.search.aggregations.metrics.valuecount.ValueCountAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * @author nate-pt
 * @date 2021/12/8 14:31
 * @Since 1.8
 * @Description
 */
public class Test {

    @org.junit.jupiter.api.Test
    public void exec1() throws JSQLParserException {
        String sql = " select a,b,sum(q) from user where a = '3' and c=4 and (q=3 or j=7)  and creatTime>=0";
        Select select = (Select) CCJSqlParserUtil.parse(sql);
        PlainSelect selectBody = (PlainSelect) select.getSelectBody();
        Expression where = selectBody.getWhere();
        System.out.println(selectBody);

    }

    @org.junit.jupiter.api.Test
    public void exec2() throws JSQLParserException {
        String sql = "select * from user";
        Select select = (Select) CCJSqlParserUtil.parse(sql);
        PlainSelect selectBody = (PlainSelect) select.getSelectBody();
        List<SelectItem> selectItems = selectBody.getSelectItems();
        selectItems.forEach(System.out::println);
    }

    @org.junit.jupiter.api.Test
    public void exec3() throws JSQLParserException {
        String sql = "select avg(c) from user";
        Select select = (Select) CCJSqlParserUtil.parse(sql);
        PlainSelect selectBody = (PlainSelect) select.getSelectBody();
        List<SelectItem> selectItems = selectBody.getSelectItems();
        selectItems.forEach(selectItem -> {
            SelectExpressionItem selectItem1 = (SelectExpressionItem) selectItem;
            Expression expression = selectItem1.getExpression();
            Function expression1 = (Function) expression;
            System.out.println(expression1.getAttributeName());
            NamedExpressionList namedParameters = expression1.getNamedParameters();

        });
        selectBody.getSelectItems().forEach(System.out::println);
    }

    @org.junit.jupiter.api.Test
    public void exec4() throws JSQLParserException {
        String sql = "select * from user limit 1,10";
        Select select = (Select) CCJSqlParserUtil.parse(sql);
        PlainSelect selectBody = (PlainSelect) select.getSelectBody();
        Limit limit = selectBody.getLimit();
        System.out.println(limit.getOffset());
        System.out.println(limit.getRowCount());
        System.out.println(limit);
    }


    @org.junit.jupiter.api.Test
    public void exec5() throws JSQLParserException {
        String sql = "select * from user where a not in ('3','4')";
        Select select = (Select) CCJSqlParserUtil.parse(sql);
        PlainSelect selectBody = (PlainSelect) select.getSelectBody();
        Expression where = selectBody.getWhere();
        InExpression where1 = (InExpression) where;
        System.out.println(where1);
    }

    @org.junit.jupiter.api.Test
    public void exec6() throws JSQLParserException {
        String sql = "select a from user group by a";
        Select select = (Select) CCJSqlParserUtil.parse(sql);
        PlainSelect selectBody = (PlainSelect) select.getSelectBody();
        GroupByElement groupBy = selectBody.getGroupBy();
        System.out.println(groupBy);
    }

    @org.junit.jupiter.api.Test
    public void exec7() throws JSQLParserException {
        String sql = "select a from user where a!=4";
        Select select = (Select) CCJSqlParserUtil.parse(sql);
        PlainSelect selectBody = (PlainSelect) select.getSelectBody();
        Expression where = selectBody.getWhere();

        System.out.println(where);
    }

    @org.junit.jupiter.api.Test
    public void exec8() throws JSQLParserException {
        String sql = "select a from user";
        Select select = (Select) CCJSqlParserUtil.parse(sql);
        PlainSelect selectBody = (PlainSelect) select.getSelectBody();

        System.out.println(selectBody);
    }

    @org.junit.jupiter.api.Test
    public void exec9() throws JSQLParserException {
        String sql = "select count(0),a from user";
        Select select = (Select) CCJSqlParserUtil.parse(sql);
        PlainSelect selectBody = (PlainSelect) select.getSelectBody();
        System.out.println(selectBody);
        List<SelectItem> selectItems = selectBody.getSelectItems();
        selectItems.stream().filter(selectItem -> {
            SelectExpressionItem selectItem1 = (SelectExpressionItem) selectItem;
            return selectItem1.getExpression() instanceof Column;
        }).forEach(System.out::println);
    }

    @org.junit.jupiter.api.Test
    public void exec11() throws JSQLParserException {
        String sql = "select *  from user where a=3 and b=3 and c=4 and q is not null";
        Select select = (Select) CCJSqlParserUtil.parse(sql);
        PlainSelect selectBody = (PlainSelect) select.getSelectBody();
        System.out.println(selectBody);
    }

    @org.junit.jupiter.api.Test
    public void exec12() throws JSQLParserException {
        String sql = "select *  from user where a like '%b%'";
        Select select = (Select) CCJSqlParserUtil.parse(sql);
        PlainSelect selectBody = (PlainSelect) select.getSelectBody();
        System.out.println(selectBody);
    }

    @org.junit.jupiter.api.Test
    public void exec13() throws JSQLParserException {
        String sql ="select * from user where a<=3";
        Select select = (Select) CCJSqlParserUtil.parse(sql);
        PlainSelect selectBody = (PlainSelect) select.getSelectBody();
        System.out.println(selectBody);
    }

    @org.junit.jupiter.api.Test
    public void exec14() throws JSQLParserException {
        String sql = "select * from user order by a,b desc";
        Select select = (Select) CCJSqlParserUtil.parse(sql);
        PlainSelect selectBody = (PlainSelect) select.getSelectBody();
        System.out.println(selectBody);
    }

    @org.junit.jupiter.api.Test
    public void exe15() throws IOException {
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("127.0.0.1",9200,"http")));

        //创建movie索引的搜索请求
        SearchRequest searchRequest = new SearchRequest("comment");
        //创建搜索文档内容对象
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        TermsAggregationBuilder terms = AggregationBuilders.terms("sum").field("userName");
        TermsAggregationBuilder field1 = AggregationBuilders.terms("appMark").field("appMark");
        ValueCountAggregationBuilder field = AggregationBuilders.count("count").field("_id");
        field1.subAggregation(field);
        terms.subAggregation(field1);
        searchSourceBuilder.aggregation(terms);

        searchRequest.source(searchSourceBuilder);
        SearchResponse search = client.search(searchRequest, RequestOptions.DEFAULT);

        Aggregations aggregations = search.getAggregations();
        ParsedStringTerms sum = aggregations.get("sum");
        List<? extends Terms.Bucket> buckets = sum.getBuckets();
        List<Map<String,Object>> list = new ArrayList<>();
        for (Terms.Bucket bucket : buckets) {
            ParsedStringTerms  aggregation = bucket.getAggregations().get("appMark");
            for (Terms.Bucket bucket1 : aggregation.getBuckets()) {
                Map<String,Object> map = new HashMap<>();
                map.put("key",bucket.getKey()+"_"+bucket1.getKey());
                ParsedValueCount  count = bucket1.getAggregations().get("count");
                map.put("value",count.value());
                list.add(map);
            }
        }
        list.forEach(System.out::println);

    }


    @org.junit.jupiter.api.Test
    public void exec16() throws JSQLParserException {
        String sql  = "select sum(a) from user group by a,b";
        Select select = (Select) CCJSqlParserUtil.parse(sql);
        PlainSelect selectBody = (PlainSelect) select.getSelectBody();
        Function expression = (Function) ((SelectExpressionItem) selectBody.getSelectItems().get(0)).getExpression();
        System.out.println(selectBody);

    }


    @org.junit.jupiter.api.Test
    public void exec17(){
        Stack<String> strings = new Stack<>();
        strings.push("3");
        strings.push("5");

//        System.out.println(strings.pop());
//        System.out.println(strings.pop());
//        System.out.println(strings.pop());
//
//        System.out.println(strings.isEmpty());
        for (String string : strings) {
            System.out.println(string);
        }

    }


    @org.junit.jupiter.api.Test
    public void exec18(){
        Queue<String> queue = new LinkedBlockingDeque<>();
        queue.offer("1");
        queue.offer("2");
        queue.offer("3");
        queue.offer("4");

//        System.out.println(queue.poll());
//        System.out.println(queue.poll());
//        System.out.println(queue.poll());
//        System.out.println(queue.poll());
//        System.out.println(queue.poll());
//        System.out.println(queue.poll());
//        System.out.println(queue.poll());
        System.out.println(queue.poll());
        System.out.println(queue.peek());
        System.out.println(queue.poll());
        System.out.println(queue.peek());
        System.out.println(queue.poll());
        System.out.println(queue.peek());

    }

    @org.junit.jupiter.api.Test
    public void exec19(){
        SearchResult search = SearchFactory.createSearcher().search("select count" +
                " " +
                "" +
                "dfa" +
                "" +
                "" +
                "" +
                "" +
                "" +
                "dfa                    (promisetime),userName,appMark from comment  group by userName,appMark limit 1,20000");
        List<Map<String, Object>> info = search.getInfo();
        System.out.println("---------------------------------------"+info.size()+"----------------------------------------------");
        List<Map<String, Object>> agg = search.getAgg();
        info.forEach(System.out::println);
        agg.forEach(System.out::println);
    }

    @org.junit.jupiter.api.Test
    public void exec20(){
        SearchResult search = SearchFactory.createSearcher().search("select dbIid,appName,branchName,branchMark,eventType from jcpt_event_fixed_log360000000000_2021  limit 1,10000");
        List<Map<String, Object>> info = search.getInfo();
        info.forEach(System.out::println);
    }

    @org.junit.jupiter.api.Test
    public void exec21(){
        SearchResult search = SearchFactory.createSearcher().search(" select count(_id),branchMark,branchName,eventType,appTheme from jcpt_event_fixed_log360000000000_2021 where eventType='3' and source='1' group by branchMark,source limit 1,100");
        search.getInfo().forEach(System.out::println);
        search.getAgg().forEach(System.out::println);
    }

    @org.junit.jupiter.api.Test
    public void exec22() {
        CompletableFuture<Void> voidCompletableFuture = CompletableFuture.runAsync(() -> {
            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.out.println("797979");
            throw new RuntimeException("异常了！！");
        });
        System.out.println("111");
        try {
            voidCompletableFuture.get();
        } catch (Exception e) {
        }
    }

    @org.junit.jupiter.api.Test
    public void exec23(){
        String sql = "select * from comment";
        SearchResult search = SearchFactory.createSearcher().search(sql);
        List<Map<String, Object>> info = search.getInfo();
        info.forEach(stringObjectMap -> {
            System.out.println(stringObjectMap);
        });
    }

   









}

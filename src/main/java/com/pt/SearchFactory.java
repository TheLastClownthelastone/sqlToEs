package com.pt;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cglib.proxy.Enhancer;
import org.springframework.cglib.proxy.MethodInterceptor;
import org.springframework.cglib.proxy.MethodProxy;
import org.springframework.test.context.jdbc.Sql;

import java.lang.reflect.Method;

/**
 * @author nate-pt
 * @date 2021/12/8 11:29
 * @Since 1.8
 * @Description
 */
public class SearchFactory {



    private static final ThreadLocal<SqlSearcher> threadLocal = new ThreadLocal<>();


    /**
     * 获取查询对象
     * <p>建议在主线程中创建</p>
     * @return
     */
    public static SqlSearcher createSearcher(){
        SqlSearcher sqlSearcher = threadLocal.get();
        if (sqlSearcher==null) {
            // 创建代理查询es对象
            sqlSearcher = _build_proxy();
            // put 进入threadLocal中
            threadLocal.set(sqlSearcher);
        }
        return sqlSearcher;
    }

    /**
     * 代理对象
     * @return
     */
    private static SqlSearcher _build_proxy(){
        Enhancer enhancer = new Enhancer();
        enhancer.setSuperclass(SqlSearcher.class);
        enhancer.setCallback(new SqlSearcherMethodHandler());
        return ((SqlSearcher) enhancer.create());
    }

    private static class SqlSearcherMethodHandler implements MethodInterceptor{



        @Override
        public Object intercept(Object o, Method method, Object[] objects, MethodProxy methodProxy) throws Throwable {
            SqlSearcherProxyHandler sqlSearcherProxyHandler = new SqlSearcherProxyHandler(o, method, objects, methodProxy);
            return sqlSearcherProxyHandler.proxyHandler();
        }
    }

}


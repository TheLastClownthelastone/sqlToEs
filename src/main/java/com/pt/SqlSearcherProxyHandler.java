package com.pt;

import net.sf.jsqlparser.JSQLParserException;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.cglib.proxy.MethodProxy;

import java.io.IOException;
import java.lang.reflect.Method;

/**
 * @author nate-pt
 * @date 2021/12/8 14:23
 * @Since 1.8
 * @Description
 */
public class SqlSearcherProxyHandler {

    private Object o;

    private Method method;

    private Object[] objects;

    private MethodProxy methodProxy;





    public SqlSearcherProxyHandler(Object o, Method method, Object[] objects, MethodProxy methodProxy) {
        this.o = o;
        this.method = method;
        this.objects = objects;
        this.methodProxy = methodProxy;
    }


    public Object proxyHandler() throws JSQLParserException, IOException {
        String sql = (String) objects[0];
        SqlParser sqlParser = new SqlParser(sql);
        return sqlParser.search0();
    }


}
package com.pt;

import java.util.List;
import java.util.Map;

/**
 * @author nate-pt
 * @date 2021/12/8 11:35
 * @Since 1.8
 * @Description
 */
public interface SqlSearcher {

    /**
     * 通过sql查询es 返回map
     * @param sql
     * @return
     */
    SearchResult search(String sql);

}

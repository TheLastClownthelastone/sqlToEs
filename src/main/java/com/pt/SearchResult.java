package com.pt;

import java.util.List;
import java.util.Map;

/**
 * @author nate-pt
 * @date 2021/12/9 16:30
 * @Since 1.8
 * @Description
 */
public class SearchResult {

    private List<Map<String,Object>> info;

    private List<Map<String,Object>> agg;

    public List<Map<String, Object>> getInfo() {
        return info;
    }

    public void setInfo(List<Map<String, Object>> info) {
        this.info = info;
    }

    public List<Map<String, Object>> getAgg() {
        return agg;
    }

    public void setAgg(List<Map<String, Object>> agg) {
        this.agg = agg;
    }
}

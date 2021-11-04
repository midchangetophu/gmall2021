package com.xiaohu.gmall.gmallpublisher.service;

import java.util.Map;

public interface PublisherService {
    //获取日活总数数据
    public int getDauTotal(String date);
    //获取日活分时数据
    public Map getDauTotalHours(String date);


}

package com.xiaohu.gmall.gmallpublisher.service.impl;

import com.xiaohu.gmall.gmallpublisher.mapper.mapper.DauMapper;
import com.xiaohu.gmall.gmallpublisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PublisherServiceImpl implements PublisherService {
    @Autowired
    DauMapper dauMapper;

    @Override
    public int getDauTotal(String date) {
        return dauMapper.selectDauTotal(date) ;
    }

    @Override
    public Map getDauTotalHours(String date) {
        List<Map> list = dauMapper.selectDauTotalHourMap(date);
        HashMap<Object, Object> result = new HashMap<>();
        for (Map map : list) {
            result.put((String)map.get("LH"), (Long)map.get("CT"));
        }
        return result;
    }
}

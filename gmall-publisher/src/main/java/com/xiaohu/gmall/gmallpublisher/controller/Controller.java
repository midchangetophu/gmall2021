package com.xiaohu.gmall.gmallpublisher.controller;
import com.xiaohu.gmall.gmallpublisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;


public class Controller {
    @Autowired
    private PublisherService publisherService;
@RequestMapping("realtime-total")
public String realtimeTotal(@RequestParam("date") String date) {
    //从service层获取日活总数数据
    Integer dauTotal = publisherService.getDauTotal(date);

    publisherService.getDauTotalHours(date);

    //创建list集合存放最终数据
    ArrayList<Map> result = new ArrayList<>();

    //创建存放新增日活的map集合
    HashMap<String, Object> dauMap = new HashMap<>();

    //创建存放新增设备的map集合
    HashMap<String, Object> devMap = new HashMap<>();

    dauMap.put("id", "dau");
    dauMap.put("name", "新增日活");
    dauMap.put("value", dauTotal);

    devMap.put("id", "new_mid");
    devMap.put("name", "新增设备");
    devMap.put("value", 233);

    result.add(dauMap);
    result.add(devMap);

    return JSONObject.toJSONString(result);
}

}

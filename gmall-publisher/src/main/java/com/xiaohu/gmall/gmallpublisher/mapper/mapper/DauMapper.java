package com.xiaohu.gmall.gmallpublisher.mapper.mapper;

import java.util.List;
import java.util.Map;

public interface DauMapper {
    public Integer selectDauTotal(String date);
    public List<Map> selectDauTotalHourMap(String date);

}
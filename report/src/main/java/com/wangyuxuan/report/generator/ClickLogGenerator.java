package com.wangyuxuan.report.generator;

import com.alibaba.fastjson.JSONObject;
import com.wangyuxuan.report.bean.ClickLog;
import com.wangyuxuan.report.send.KafkaMessageSend;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;

/**
 * @author wangyuxuan
 * @date 2020/3/29 4:33 下午
 * @description 点击流日志模拟器
 */
public class ClickLogGenerator {

    private static Long[] channelID = new Long[]{1l, 2l, 3l, 4l, 5l, 6l, 7l, 8l, 9l, 10l, 11l, 12l, 13l, 14l, 15l, 16l, 17l, 18l, 19l, 20l};//频道id集合
    private static Long[] categoryID = new Long[]{1l, 2l, 3l, 4l, 5l, 6l, 7l, 8l, 9l, 10l, 11l, 12l, 13l, 14l, 15l, 16l, 17l, 18l, 19l, 20l};//产品类别id集合
    private static Long[] commodityID = new Long[]{1l, 2l, 3l, 4l, 5l, 6l, 7l, 8l, 9l, 10l, 11l, 12l, 13l, 14l, 15l, 16l, 17l, 18l, 19l, 20l};//产品id集合
    private static Long[] userID = new Long[]{1l, 2l, 3l, 4l, 5l, 6l, 7l, 8l, 9l, 10l, 11l, 12l, 13l, 14l, 15l, 16l, 17l, 18l, 19l, 20l};//用户id集合

    /**
     * 地区
     */
    private static String[] contrys = new String[]{"china"}; // 地区-国家集合
    private static String[] provinces = new String[]{"HeNan", "HeBeijing"}; // 地区-省集合
    private static String[] citys = new String[]{"ShiJiaZhuang", "ZhengZhou", "LuoYang"}; // 地区-市集合

    /**
     * 网络方式
     */
    private static String[] networks = new String[]{"电信", "移动", "联通"};

    /**
     * 来源方式
     */
    private static String[] sources = new String[]{"直接输入", "百度跳转", "360搜索跳转", "必应跳转"};

    /**
     * 浏览器
     */
    private static String[] brower = new String[]{"火狐", "qq浏览器", "360浏览器", "谷歌浏览器"};

    /**
     * 打开时间 离开时间
     */
    private static List<Long[]> usetimelog = producetimes();

    // 获取时间
    private static List<Long[]> producetimes() {
        List<Long[]> usetimelog = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            Long[] timesarray = gettimes("2018-12-12 24:60:60:000");
            usetimelog.add(timesarray);
        }
        return usetimelog;
    }

    private static Long[] gettimes(String time) {
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");
        try {
            Date date = dateFormat.parse(time);
            long timetemp = date.getTime();
            Random random = new Random();
            int randomint = random.nextInt(10);
            long starttime = timetemp - randomint * 3600 * 1000;
            long endtime = starttime + randomint * 3600 * 1000;
            return new Long[]{starttime, endtime};
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return new Long[]{0L, 0L};
    }

    public static void main(String[] args) {
        Random random = new Random();
        for (int i = 0; i < 100; i++) {
            // 频道id 类别id 产品id 用户id 打开时间 离开时间 地区 网络方式 来源方式 浏览器
            ClickLog clickLog = new ClickLog();

            clickLog.setChannelID(channelID[random.nextInt(channelID.length)]);
            clickLog.setCategoryID(categoryID[random.nextInt(categoryID.length)]);
            clickLog.setProduceID(commodityID[random.nextInt(commodityID.length)]);
            clickLog.setUserID(userID[random.nextInt(userID.length)]);
            clickLog.setCountry(contrys[random.nextInt(contrys.length)]);
            clickLog.setProvince(provinces[random.nextInt(provinces.length)]);
            clickLog.setCity(citys[random.nextInt(citys.length)]);
            clickLog.setNetwork(networks[random.nextInt(networks.length)]);
            clickLog.setSource(sources[random.nextInt(sources.length)]);
            clickLog.setBrowserType(brower[random.nextInt(brower.length)]);

            Long[] times = usetimelog.get(random.nextInt(usetimelog.size()));
            clickLog.setEntryTime(times[0]);
            clickLog.setLeaveTime(times[1]);

            String jsonstr = JSONObject.toJSONString(clickLog);
            // 打印消息的内容
            System.out.println(jsonstr);
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            KafkaMessageSend.send("http://localhost:8888/ReportApplication/receiveData", jsonstr);
        }
    }
}

package com.wangyuxuan.report.bean;

/**
 * @author wangyuxuan
 * @date 2020/3/29 4:15 下午
 * @description 点击流日志
 */
public class ClickLog {
    //频道ID
    private long channelID;
    //产品的类别ID
    private long categoryID;
    //产品ID
    private long produceID;
    //国家
    private String country;
    //省份
    private String province;
    //城市
    private String city;
    //网络方式
    private String network;
    //来源方式
    private String source;

    //浏览器类型
    private String browserType;

    //进入网站时间
    private Long entryTime;

    //离开网站时间
    private long leaveTime;

    //用户的ID
    private long userID;

    public long getChannelID() {
        return channelID;
    }

    public void setChannelID(long channelID) {
        this.channelID = channelID;
    }

    public long getCategoryID() {
        return categoryID;
    }

    public void setCategoryID(long categoryID) {
        this.categoryID = categoryID;
    }

    public long getProduceID() {
        return produceID;
    }

    public void setProduceID(long produceID) {
        this.produceID = produceID;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public String getProvince() {
        return province;
    }

    public void setProvince(String province) {
        this.province = province;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getNetwork() {
        return network;
    }

    public void setNetwork(String network) {
        this.network = network;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public String getBrowserType() {
        return browserType;
    }

    public void setBrowserType(String browserType) {
        this.browserType = browserType;
    }

    public Long getEntryTime() {
        return entryTime;
    }

    public void setEntryTime(Long entryTime) {
        this.entryTime = entryTime;
    }

    public long getLeaveTime() {
        return leaveTime;
    }

    public void setLeaveTime(long leaveTime) {
        this.leaveTime = leaveTime;
    }

    public long getUserID() {
        return userID;
    }

    public void setUserID(long userID) {
        this.userID = userID;
    }

    @Override
    public String toString() {
        return "ClickLog{" +
                "channelID=" + channelID +
                ", categoryID=" + categoryID +
                ", produceID=" + produceID +
                ", country='" + country + '\'' +
                ", province='" + province + '\'' +
                ", city='" + city + '\'' +
                ", network='" + network + '\'' +
                ", source='" + source + '\'' +
                ", browserType='" + browserType + '\'' +
                ", entryTime=" + entryTime +
                ", leaveTime=" + leaveTime +
                ", userID=" + userID +
                '}';
    }
}

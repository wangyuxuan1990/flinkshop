package com.wangyuxuan.report.msg;

/**
 * @author wangyuxuan
 * @date 2020/3/29 3:48 下午
 * @description 定义一个类，封装消息
 */
public class Message {
    // json格式的消息内容
    private String content;
    // 消息的次数
    private int count;
    // 消息的时间
    private Long timestamp;

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "Message{" +
                "content='" + content + '\'' +
                ", count=" + count +
                ", timestamp=" + timestamp +
                '}';
    }
}

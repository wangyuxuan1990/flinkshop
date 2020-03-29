package com.wangyuxuan.report.send;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;

/**
 * @author wangyuxuan
 * @date 2020/3/29 4:17 下午
 * @description 发送消息到kafka集群
 */
public class KafkaMessageSend {

    public static void send(String address, String message) {
        try {
            // 请求URL
            URL url = new URL(address);
            // 打开http连接
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            // 设置post请求
            conn.setRequestMethod("POST");
            // 设置url连接可以用于输出,获取字节流
            conn.setDoOutput(true);
            // 允许用户进行上下文中对URL进行检查
            conn.setAllowUserInteraction(true);
            // 请求超时时间
            conn.setReadTimeout(6 * 1000);
            // 设置请求头信息 --用户代理
            conn.setRequestProperty("User-agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.86 Safari/537.36");
            conn.setRequestProperty("Content-Type", "application/json");
            // 连接网络
            conn.connect();

            // 使用io流发送消息
            BufferedOutputStream out = new BufferedOutputStream(conn.getOutputStream());
            out.write(message.getBytes());
            out.flush();

            // 发送完毕之后，客户端看到服务端返回结果
            // 得到响应流
            InputStream in = conn.getInputStream();
            // 将响应流转换成字符串
            String returnLine = getStringFromInputStream(in);
            System.out.println(returnLine);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 通过字节输入流返回一个字符串信息
     *
     * @param in
     * @return
     */
    private static String getStringFromInputStream(InputStream in) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        byte[] buffer = new byte[1024];
        int len = 0;
        while ((len = in.read(buffer)) != -1) {
            baos.write(buffer, 0, len);
        }
        in.close();
        // 把流中的数据转换成字符串, 采用的编码是: utf-8
        String status = baos.toString();
        baos.close();
        return status;
    }
}

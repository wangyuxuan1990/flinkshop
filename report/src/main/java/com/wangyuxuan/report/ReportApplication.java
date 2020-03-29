package com.wangyuxuan.report;

import com.alibaba.fastjson.JSON;
import com.wangyuxuan.report.msg.Message;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;

@SpringBootApplication
@Controller
@RequestMapping("ReportApplication")
public class ReportApplication {

    @Autowired
    private KafkaTemplate kafkaTemplate;

    /**
     * 接受客户端请求的数据
     *
     * @param json
     * @param request
     * @param response
     */
    @RequestMapping(value = "receiveData", method = RequestMethod.POST)
    public void receiveData(@RequestBody String json, HttpServletRequest request, HttpServletResponse response) {
        // 获取消息，将消息封装成Message对象
        Message message = new Message();
        message.setContent(json);
        message.setCount(1);
        message.setTimestamp(System.currentTimeMillis());
        String jsonString = JSON.toJSONString(message);
        System.out.println(jsonString);

        //业务开始   topic名称、内容
        kafkaTemplate.send("flinkshop", jsonString);

        // 业务结束，回现结果
        PrintWriter pw = getPrintWriter(response);
        pw.write("==========消息发送成功============");
        close(pw);
    }

    private PrintWriter getPrintWriter(HttpServletResponse response) {
        // 设置响应的内容格式json
        response.setContentType("application/json");
        // 设置响应内容使用utf-8转码
        response.setCharacterEncoding("utf-8");
        // 字符流回显结果
        OutputStream out = null;
        PrintWriter pw = null;

        try {
            // 获取字节流
            out = response.getOutputStream();
            // 构建字符流
            pw = new PrintWriter(out);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return pw;
    }

    // 关闭io流操作
    private void close(PrintWriter pw) {
        // 将缓冲区的数据强制输出,用于清空缓冲区
        pw.flush();
        // 关闭
        pw.close();
    }

}

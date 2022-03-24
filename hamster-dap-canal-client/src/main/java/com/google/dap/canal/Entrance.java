package com.google.dap.canal;

import com.google.dap.canal.client.CanalClient;

/**
 * canal客户端的入口类
 */
public class Entrance {
    public static void main(String[] args) {
        //实例化canal的客户端对象，调用start方法拉取canalserver端binlog日志
        CanalClient canalClient = new CanalClient();
        canalClient.start();
    }
}

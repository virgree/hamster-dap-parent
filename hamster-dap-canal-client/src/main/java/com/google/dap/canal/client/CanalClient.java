package com.google.dap.canal.client;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.dap.canal.bean.CanalRowData;
import com.google.dap.canal.util.ConfigUtil;
import com.google.dap.canal.util.KafkaSender;
import com.google.protobuf.InvalidProtocolBufferException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * canal客户端程序
 * 与canal服务端建立连接，然后获取cancalServer端的binlog日志
 */
public class CanalClient {

    //一次拉取binlog数据的条数
    private static  final  int  BATCH_SIZE = 5*1024;
    //canal的客户端连接器
    private CanalConnector canalConnector;
    //定义kafka的生产者工具类
    private KafkaSender kafkaSender;

    /**
     * 构造方法
     */
    public CanalClient(){
        //初始化连接
        canalConnector = CanalConnectors.newClusterConnector(ConfigUtil.zookeeperServerIp(),
                ConfigUtil.canalServerDestination(),
                ConfigUtil.canalServerUsername(),
                ConfigUtil.canalServerPassword()
                );

        //实例化kafka的生产者工具类
        kafkaSender = new KafkaSender();
    }

    /**
     * 开始执行
     */
    public void start(){
        try {
            //建立连接
            canalConnector.connect();
            //回滚上次的get请求，重新获取数据
            canalConnector.rollback();
            //订阅匹配的数据库
            canalConnector.subscribe(ConfigUtil.canalSubscribeFilter());
            //不停的循环拉取数据
            while (true){
                //拉取binlog日志，每次拉取5*1024条数据
                Message message = canalConnector.getWithoutAck(BATCH_SIZE);
                //获取batchid
                long batchId = message.getId();
                //获取binlog数据的条数
                int size = message.getEntries().size();
                if(size == 0 || size == -1){
                    //没有拉取到数据
                }else{
                    //将binlog日志进行解析，解析后的数据就是Map对象
                    Map binlogMessageToMap = binlogMessageToMap(message);
                    //需要将map对象序列化成protobuf格式写入到kafka中
                    CanalRowData rowData = new CanalRowData(binlogMessageToMap);
                    System.out.println(rowData);
                    if(binlogMessageToMap.size()>0){
                        //有数据，将数据发送到kafka集群
                        kafkaSender.send(rowData);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            //断开连接
            canalConnector.disconnect();
        }
    }
    /**
     * 将binlog日志转换为Map结构
     * @param message
     * @return
     */
    private Map binlogMessageToMap(Message message) throws InvalidProtocolBufferException {
        Map rowDataMap = new HashMap();

        // 1. 遍历message中的所有binlog实体
        for (CanalEntry.Entry entry : message.getEntries()) {
            // 只处理事务型binlog
            if(entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN ||
                    entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND) {
                continue;
            }

            // 获取binlog文件名
            String logfileName = entry.getHeader().getLogfileName();
            // 获取logfile的偏移量
            long logfileOffset = entry.getHeader().getLogfileOffset();
            // 获取sql语句执行时间戳
            long executeTime = entry.getHeader().getExecuteTime();
            // 获取数据库名
            String schemaName = entry.getHeader().getSchemaName();
            // 获取表名
            String tableName = entry.getHeader().getTableName();
            // 获取事件类型 insert/update/delete
            String eventType = entry.getHeader().getEventType().toString().toLowerCase();

            rowDataMap.put("logfileName", logfileName);
            rowDataMap.put("logfileOffset", logfileOffset);
            rowDataMap.put("executeTime", executeTime);
            rowDataMap.put("schemaName", schemaName);
            rowDataMap.put("tableName", tableName);
            rowDataMap.put("eventType", eventType);

            // 获取所有行上的变更
            Map<String, String> columnDataMap = new HashMap<>();
            CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
            List<CanalEntry.RowData> columnDataList = rowChange.getRowDatasList();
            for (CanalEntry.RowData rowData : columnDataList) {
                if(eventType.equals("insert") || eventType.equals("update")) {
                    for (CanalEntry.Column column : rowData.getAfterColumnsList()) {
                        columnDataMap.put(column.getName(), column.getValue().toString());
                    }
                }
                else if(eventType.equals("delete")) {
                    for (CanalEntry.Column column : rowData.getBeforeColumnsList()) {
                        columnDataMap.put(column.getName(), column.getValue().toString());
                    }
                }
            }

            rowDataMap.put("columns", columnDataMap);
        }

        return rowDataMap;
    }
}
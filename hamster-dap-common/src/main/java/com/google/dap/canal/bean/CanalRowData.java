package com.google.dap.canal.bean;

import com.alibaba.fastjson.JSON;
import com.google.canal.protobuf.CanalModel;
import com.google.dap.canal.protobuf.ProtoBufable;
import com.google.protobuf.InvalidProtocolBufferException;

import java.util.HashMap;
import java.util.Map;

/**
 * 这个类是canal数据的protobuf的实现类
 * 能够使用protobuf序列化成bean对象
 * 目的：将binlog解析后的map对象，转换成portobuf序列化后的字节码数据，最终写入到kafka集群
 */
public class CanalRowData implements ProtoBufable {

    private String logfileName;
    private Long logfileOffset;
    private Long executeTime;
    private String schemaName;
    private String tableName;
    private String eventType;
    private Map<String, String> columns;

    public String getLogfileName() {
        return logfileName;
    }

    public void setLogfileName(String logfileName) {
        this.logfileName = logfileName;
    }

    public Long getLogfileOffset() {
        return logfileOffset;
    }

    public void setLogfileOffset(Long logfileOffset) {
        this.logfileOffset = logfileOffset;
    }

    public Long getExecuteTime() {
        return executeTime;
    }

    public void setExecuteTime(Long executeTime) {
        this.executeTime = executeTime;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public Map<String, String> getColumns() {
        return columns;
    }

    public void setColumns(Map<String, String> columns) {
        this.columns = columns;
    }

    /**
     * 定义构造方法，解析map对象的binlog日志
     */
    public CanalRowData(Map map){
        //解析map对象中所有的参数
        if(map.size()>0){
            this.logfileName = map.get("logfileName").toString();
            this.logfileOffset = Long.parseLong(map.get("logfileOffset").toString());
            this.executeTime = Long.parseLong(map.get("executeTime").toString());
            this.schemaName = map.get("schemaName").toString();
            this.tableName = map.get("tableName").toString();
            this.eventType = map.get("eventType").toString();
            this.columns = (Map<String, String>)map.get("columns");
        }
    }

    /**
     * 传递一个字节码数据，将字节码数据反序列化成对象
     * @param bytes
     */
    public CanalRowData(byte[] bytes){
        try {
            //将字节码数据反序列成对象
            CanalModel.RowData rowData = CanalModel.RowData.parseFrom(bytes);
            this.logfileName = rowData.getLogfileName();
            this.logfileOffset = rowData.getLogfileOffset();
            this.executeTime = rowData.getExecuteTime();
            this.schemaName = rowData.getSchemaName();
            this.tableName = rowData.getTableName();
            this.eventType = rowData.getEventType();

            //将所有的列的集合添加到map中
            this.columns = new HashMap<>();
            this.columns.putAll(rowData.getColumnsMap());
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }
    }

    /**
     * 需要将map对象解析出来的参数，赋值给protobuf对象，然后序列化后字节码返回
     * @return
     */
    @Override
    public byte[] toBytes() {
        CanalModel.RowData.Builder builder = CanalModel.RowData.newBuilder();
        builder.setLogfileName(this.getLogfileName());
        builder.setLogfileOffset(this.getLogfileOffset());
        builder.setExecuteTime(this.getExecuteTime());
        builder.setSchemaName(this.getSchemaName());
        builder.setTableName(this.getTableName());
        builder.setEventType(this.getEventType());

        for (String key : this.getColumns().keySet()) {
            builder.putColumns(key, this.getColumns().get(key));
        }

        //将传递的binlog数据解析后序列化成字节码数据返回
        return builder.build().toByteArray();
    }

    @Override
    public String toString() {
        return JSON.toJSONString(this);
    }
}

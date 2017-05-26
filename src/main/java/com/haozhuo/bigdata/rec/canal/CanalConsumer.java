package com.haozhuo.bigdata.rec.canal;

import java.net.InetSocketAddress;
import java.util.List;


import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.protocol.CanalEntry.Column;
import com.alibaba.otter.canal.protocol.CanalEntry.Entry;
import com.alibaba.otter.canal.protocol.CanalEntry.EntryType;
import com.alibaba.otter.canal.protocol.CanalEntry.EventType;
import com.alibaba.otter.canal.protocol.CanalEntry.RowChange;
import com.alibaba.otter.canal.protocol.CanalEntry.RowData;
import com.haozhuo.bigdata.rec.Props;
import com.haozhuo.bigdata.rec.kafka.MyKafkaProducer;


public class CanalConsumer {
    MyKafkaProducer producer = new MyKafkaProducer();
    public void run() {
        // 创建链接
        CanalConnector connector = CanalConnectors.newSingleConnector(new InetSocketAddress(Props.get("canal.ip"), Integer.parseInt(Props.get("canal.port"))), Props.get("canal.destinations"), "", "");
        String tables = Props.get("canal.tables");
        int batchSize = 1000;
        try {
            connector.connect();

            //订阅所有表 connector.subscribe(".*\\..*");
            //订阅多张表 connector.subscribe("db.table1,db.table2");
            connector.subscribe(tables);
            connector.rollback();
            while (true) {
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                Message message = connector.getWithoutAck(batchSize); // 获取指定数量的数据
                long batchId = message.getId();
                int size = message.getEntries().size();
                if (batchId == -1 || size == 0) {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                    }
                } else {
                    // System.out.printf("message[batchId=%s,size=%s] \n", batchId, size);
                    printEntry(message.getEntries());
                }
                connector.ack(batchId); // 提交确认
                // connector.rollback(batchId); // 处理失败, 回滚数据
            }
        } finally {
            connector.disconnect();
        }
    }


    private  void printEntry(List<Entry> entries) {
        for (Entry entry : entries) {
            if (entry.getEntryType() == EntryType.TRANSACTIONBEGIN || entry.getEntryType() == EntryType.TRANSACTIONEND) {
                continue;
            }

            RowChange rowChange = null;
            try {
                rowChange = RowChange.parseFrom(entry.getStoreValue());
            } catch (Exception e) {
                throw new RuntimeException("ERROR ## parser of eromanga-event has an error , data:" + entry.toString(), e);
            }

            EventType eventType = rowChange.getEventType();
            System.out.println(String.format("binlog[%s:%s] , name[%s,%s] , eventType : %s",
                    entry.getHeader().getLogfileName(), entry.getHeader().getLogfileOffset(),
                    entry.getHeader().getSchemaName(), entry.getHeader().getTableName(),
                    eventType));

            for (RowData rowData : rowChange.getRowDatasList()) {
                if (eventType == EventType.DELETE) {
                    //printColumn(rowData.getBeforeColumnsList());
                } else if (eventType == EventType.INSERT) {
                    List<Column> columns = rowData.getAfterColumnsList();
                    String msg = columns.get(0).getValue()+"," + columns.get(1).getValue();
                    System.out.println(msg);
                } else {
                    /*System.out.println("-------&gt; before");
                    printColumn(rowData.getBeforeColumnsList());
                    System.out.println("-------&gt; after");
                    printColumn(rowData.getAfterColumnsList());*/
                }
            }
        }
    }

    private  void printColumn(List<Column> columns) {
       for (Column column : columns) {
            System.out.println(column.getName() + " : " + column.getValue() + "    update=" + column.getUpdated());
        }
    }

    public static void main(String args[]) {
        CanalConsumer a = new CanalConsumer();
        a.run();
    }
}

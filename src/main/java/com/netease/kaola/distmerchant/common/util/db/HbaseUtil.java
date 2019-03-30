package com.netease.kaola.distmerchant.common.util.db;

import com.alibaba.fastjson.JSON;
import com.netease.kaola.distmerchant.common.util.DateUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author kai.zhang
 * @since 2019/3/4
 */
public class HbaseUtil {
    private static final ConcurrentMap<String, Connection> hbaseConnectionMap = new ConcurrentHashMap<>();

    public static String buildCacheKey(String hbaseQuorum,
                                       String hbasePort,
                                       String hbaseParent) {
        String cacheKey = String.format("%s-%s-%s", hbaseQuorum, hbasePort, hbaseParent);
        return cacheKey;
    }

    public static Connection getConnection(String hbaseQuorum,
                                           String hbasePort,
                                           String hbaseParent) {
        String cacheKey = buildCacheKey(hbaseQuorum, hbasePort, hbaseParent);
        Connection connection = hbaseConnectionMap.computeIfAbsent(cacheKey, key -> {
            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", hbaseQuorum);
            conf.set("hbase.zookeeper.property.clientPort", hbasePort);
            conf.set("zookeeper.znode.parent", hbaseParent);
            try {
                return ConnectionFactory.createConnection(conf);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        return connection;
    }

    public static void close(String hbaseQuorum,
                             String hbasePort,
                             String hbaseParent) {
        String cacheKey = buildCacheKey(hbaseQuorum, hbasePort, hbaseParent);
        Connection connection = hbaseConnectionMap.get(cacheKey);
        if (null != connection) {
            try {
                hbaseConnectionMap.remove(cacheKey);
                connection.close();
            } catch (IOException e) {
            }
        }
    }

    public static void addRow(Connection connection, String tableName, String rowKey, String colFamily, Map<String, String> colValues) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(rowKey));
        if (colValues != null && !colValues.isEmpty()) {
            for (Map.Entry<String, String> col : colValues.entrySet()) {
                put.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(col.getKey()), Bytes.toBytes(col.getValue()));
            }
        }
        table.put(put);
    }

    public static void deleteRow(Connection connection, String tableName, String rowKey) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        table.delete(delete);
    }

    public static <T>T getData(Connection connection, String tableName, String rowKey, HbaseResultCallback<T> callback) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rowKey));
        Result result = table.get(get);
        return callback.parseResult(result);
    }

    //批量查找数据
    public static <T> List<T> scanData(Connection connection, String tableName, String startRow, String stopRow, HbaseResultCallback<T> callback)throws IOException{
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        scan.setStartRow(Bytes.toBytes(startRow));
        scan.setStopRow(Bytes.toBytes(stopRow));
        ResultScanner resultScanner = table.getScanner(scan);
        List<T> resultList = new ArrayList<>();
        for (Result result : resultScanner) {
            resultList.add(callback.parseResult(result));
        }
        return resultList;
    }
    //格式化输出
    public static void showCell(Result result){
        Cell[] cells = result.rawCells();
        for(Cell cell:cells){
            System.out.println("RowName:"+new String(CellUtil.cloneRow(cell))+" ");
            System.out.println("Timetamp:"+cell.getTimestamp()+" ");
            System.out.println("column Family:"+new String(CellUtil.cloneFamily(cell))+" ");
            System.out.println("row Name:"+new String(CellUtil.cloneQualifier(cell))+" ");
            System.out.println("value:"+new String(CellUtil.cloneValue(cell))+" ");
        }
    }
    public static void main(String... args) {
        String tableName = "kaola_dist:dist_gorder_incr";
        String colFamilyName = "incrinfo";
        try {

            Date now = new Date();
            Date before2Day = DateUtil.dateAddDays(now, -2);
            long before2DayKey = DateUtil.parseDate(DateUtil.safeDateFormat(before2Day, DateUtil.DAY_TIME_FORMAT), DateUtil.DAY_TIME_FORMAT).getTime();
            Date yestoday = DateUtil.dateAddDays(now, -1);
            long yestodayKey = DateUtil.parseDate(DateUtil.safeDateFormat(yestoday, DateUtil.DAY_TIME_FORMAT), DateUtil.DAY_TIME_FORMAT).getTime();

            Date tomorrow = DateUtil.dateAddDays(now, 1);
            long tomorrowKey = DateUtil.parseDate(DateUtil.safeDateFormat(tomorrow, DateUtil.DAY_TIME_FORMAT), DateUtil.DAY_TIME_FORMAT).getTime();

            String before2DaysKey1 = String.format("%s%s%s", (before2DayKey / 100000) % 10, before2DayKey, 1);
            String yesDaysKey1 = String.format("%s%s%s", yestodayKey /100000 % 10, yestodayKey, 1);
            String tomorrowDaysKey1 = String.format("%s%s%s",  tomorrowKey/100000 % 10, tomorrowKey, 3);

            Connection connection = getConnection("xxxxx",
                    "2181", "/hbase-kaolajd-test");
            List<GorderIncr> gorderIncrs = scanData(connection, tableName, before2DaysKey1, tomorrowDaysKey1, new HbaseResultCallback<GorderIncr>() {
                @Override
                public GorderIncr parseResult(Result result) {
                    GorderIncr gorderIncr = new GorderIncr();
                    Cell[] cells = result.rawCells();

                    for(Cell cell : cells){
                        if ("gorder_id".equals(new String(CellUtil.cloneQualifier(cell)))) {
                            gorderIncr.gorderId = new String(CellUtil.cloneValue(cell));
                        } else if ("id".equals(new String(CellUtil.cloneQualifier(cell)))) {
                            gorderIncr.id = new String(CellUtil.cloneValue(cell));
                        }
                    }
                    return gorderIncr;
                }
            });
            System.out.println(gorderIncrs);
            close("xxxx",
                    "2181", "/hbase-kaolajd-test");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    static class GorderIncr{
        public String id;
        public String gorderId;

        @Override
        public String toString() {
            return "id = " + id + ", gorder_id = " + gorderId;
        }
    }
}

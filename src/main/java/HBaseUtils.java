import java.io.IOException;
import java.util.*;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;


public class HBaseUtils {

    private static Configuration conf;
    private static Connection connection;
    private static Admin admin;

    public static void init(){
        conf = HBaseConfiguration.create();
        try {
            connection = ConnectionFactory.createConnection(conf);
            admin=connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void close(){
        try{
            if(admin != null){
                admin.close(); }
            if(null != connection){
                connection.close();
            }
        }catch (IOException e){
            e.printStackTrace(); }
    }

    /**
     * 创建表
     * @param tableName
     * @param fields
     * @return
     */
    public static boolean createTable(String tableName, String[] fields) {
        try {
            init();
            if (admin.tableExists(TableName.valueOf(tableName))) {
                return false;
            }
            TableDescriptorBuilder tableDescriptor = TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName));
            List<ColumnFamilyDescriptor> familyDescriptors = new ArrayList<>();
            for (String field : fields) {
                ColumnFamilyDescriptor familyDescriptor= ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(field)).build();
                familyDescriptors.add(familyDescriptor);
            }
            tableDescriptor.setColumnFamilies(familyDescriptors);
            admin.createTable(tableDescriptor.build());
            System.out.println(tableName+" created Successfully...");
            close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return true;
    }

    public static void deleteRow(String tableName,String row){
        try {
            init();
            Table table = connection.getTable(TableName.valueOf(tableName));
            Get get = new Get(Bytes.toBytes(row));
            Result result = table.get(get);
            if (!result.isEmpty()){
                Delete delete = new Delete(Bytes.toBytes(row));
                table.delete(delete);
                System.out.println("----删除成功----");
            }else {
                System.out.printf("----删除失败----");
            }
            table.close();
            close();
        } catch (IOException e){
            e.printStackTrace();
        }

    }


    /**
     * 删除 hBase 表
     *
     * @param tableName 表名
     */
    public static boolean deleteTable(String tableName) {
        try {
            init();
            // 删除表前需要先禁用表
            admin.disableTable(TableName.valueOf(tableName));
            admin.deleteTable(TableName.valueOf(tableName));
            close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return true;
    }


    public static boolean addRecord(String tableName, String row, String[] fields, String[] values) {
        try {
            init();
            Table table = connection.getTable(TableName.valueOf(tableName));
            Put put = new Put(Bytes.toBytes(row));
            for (int i=0;i<fields.length;i++) {
                String[] s=fields[i].split(":");
                String columnFamilyName=s[0];
                String column=s[1];
                put.addColumn(Bytes.toBytes(columnFamilyName), Bytes.toBytes(column), Bytes.toBytes(values[i]));
                table.put(put);
            }
            table.close();
            close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return true;
    }

    /**
     * 检索全表
     *
     * @param tableName 表名
     */
    public static ResultScanner scanColumn(String tableName,String column) {
        int isColFam=column.indexOf(":");
        try {
            init();
            Table table = connection.getTable(TableName.valueOf(tableName));
            Scan scan = new Scan();
            if (isColFam==-1){
                scan.addFamily(Bytes.toBytes(column));
                return table.getScanner(scan);
            }
            else {
                String[] s=column.split(":");
                String columnFamilyName=s[0];
                String columnName=s[1];
                scan.addColumn(Bytes.toBytes(columnFamilyName),Bytes.toBytes(columnName));
                return table.getScanner(scan);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static void printColumn(ResultScanner scanner){
        for (Result result : scanner) {
            byte[] row=result.getRow();
            NavigableMap<byte[],NavigableMap<byte[],NavigableMap<Long,byte[]>>> map = result.getMap();
            Set<byte[]> cfs=map.keySet();
            for (byte[] cf : cfs) {
                NavigableMap<byte[],NavigableMap<Long,byte[]>> map1= map.get(cf);
                Set<byte[]> cols=map1.keySet();
                for (byte[] col : cols) {
                    NavigableMap<Long,byte[]> map2=map1.get(col);
                    Set<Long> tss = map2.keySet();
                    for (Long aLong : tss) {
                        byte[] val=map2.get(aLong);
                        System.out.println("行键："+new String(row)+"\t列族："+new String(cf)+
                                "\t列名："+new String(col)+"\t时间戳:"+aLong+"\t值："+new String(val));
                    }
                }
            }
        }
        scanner.close();
        close();
    }


    public static void modifyData(String tableName,String row,String column,String value){
        try {
            init();
            Table table = connection.getTable(TableName.valueOf(tableName));
            Scan scan = new Scan();
            String[] s=column.split(":");
            String columnFamily=s[0];
            String columnName=s[1];
            scan.addColumn(Bytes.toBytes(columnFamily),Bytes.toBytes(columnName));
            ResultScanner scanner = table.getScanner(scan);
            Iterator<Result> iterator = scanner.iterator();
            if (iterator.hasNext()){
                    String[] cols={column};
                    String[] vals={value};

                    Put put = new Put(Bytes.toBytes(row));
                    for (int i=0;i<cols.length;i++) {
                        String[] s1=cols[i].split(":");
                        put.addColumn(Bytes.toBytes(s1[0]), Bytes.toBytes(s1[1]), Bytes.toBytes(vals[i]));
                        table.put(put);
                    }
            }
            table.close();
            close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        String tableName= "StudentCourse";
//        String[] familyNames={"StudentInfo","Math","Computer Science","English"};
//        HBaseUtils.createTable(tableName,familyNames);
//        System.out.printf("创建数据表成功");
        String[] fields = {"StudentInfo:S_No","StudentInfo:S_Sex","StudentInfo:S_Age",
                           "Math:C_No","Math:C_Credit","Math:SC_Score",
                           "Computer Science:C_No","Computer Science:C_Credit","Computer Science:SC_Score",
                           "English:C_No","English:C_Credit","English:SC_Score"
                            };
        String[] values={"2018211702","male","21",
                         "000001","4","95",
                         "000231","2","89",
                         "000126","3","88"};
        String[] values1={"2018211900","female","21",
                "000001","4","100",
                "000231","2","100",
                "000126","3","100"};
//        HBaseUtils.addRecord(tableName,"WangChong",fields,values);
//        HBaseUtils.addRecord(tableName,"JLM",fields,values1);
//        System.out.printf("插入成功");


//        System.out.println("查询数据");
//        printColumn(Objects.requireNonNull(HBaseUtils.scanColumn(tableName, "StudentInfo")));
//        printColumn(Objects.requireNonNull(HBaseUtils.scanColumn(tableName, "Math:SC_Score")));


//        System.out.println("----删除数据----");
//        HBaseUtils.deleteRow(tableName,"JLM");
//        System.out.println("----查询数据----");
//        HBaseUtils.printColumn(Objects.requireNonNull(HBaseUtils.scanColumn(tableName,"StudentInfo")));


        System.out.println("----修改之前的数据----");
        HBaseUtils.printColumn(Objects.requireNonNull(HBaseUtils.scanColumn(tableName,"Math")));
        System.out.println("----修改数据----");
        HBaseUtils.modifyData(tableName,"WangChong","Math:SC_Score","100");
        System.out.println("----修改之后的数据----");
        HBaseUtils.printColumn(Objects.requireNonNull(HBaseUtils.scanColumn(tableName,"Math")));

    }


}



import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


import org.apache.avro.generic.GenericData;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.TableName;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;

public class HBase {

    public static void main(String[] args) throws IOException {

        // Instantiating configuration class 初始化配置文件
        Configuration conf = HBaseConfiguration.create();


        // Instantiating HbaseAdmin class  初始化HbaseAdmin
        Connection conn = ConnectionFactory.createConnection(conf);
        Admin admin  = conn.getAdmin();


        TableDescriptorBuilder tableDescriptor = TableDescriptorBuilder.newBuilder(TableName.valueOf("Customer1"));
        ColumnFamilyDescriptor familyDescriptor1 =ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("columnFamily1")).build();
        ColumnFamilyDescriptor familyDescriptor2 =ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("columnFamily2")).build();
        ColumnFamilyDescriptor familyDescriptor3 =ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("columnFamily3")).build();
        List<ColumnFamilyDescriptor> ColumnFamilyDescriptors = new ArrayList<>();
        ColumnFamilyDescriptors.add(familyDescriptor1);
        ColumnFamilyDescriptors.add(familyDescriptor2);
        ColumnFamilyDescriptors.add(familyDescriptor3);

        tableDescriptor.setColumnFamilies(ColumnFamilyDescriptors);

        admin.createTable(tableDescriptor.build());


        System.out.println("Table1 created Successfully...");
    }
}

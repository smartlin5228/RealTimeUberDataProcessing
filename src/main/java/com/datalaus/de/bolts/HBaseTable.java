package com.datalaus.de.bolts;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by cloudera on 12/6/16.
 */
public class HBaseTable {
    private static Configuration configuration = null;
    //TODO: What kind of structure is static{}
    static {
        configuration = HBaseConfiguration.create();
    }

    public static void createTable(String tableName, String[] families) throws Exception {
        HBaseAdmin hBaseAdmin = new HBaseAdmin(configuration);
        if (hBaseAdmin.tableExists(tableName)) {
            System.out.println("Table already exists!");
        } else {
            HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
            for (int i = 0; i < families.length; i++) {
                tableDescriptor.addFamily(new HColumnDescriptor(families[i]));
            }
            hBaseAdmin.createTable(tableDescriptor);
            System.out.println("Created table " + tableName + ", done!");
        }
    }
    public static void deleteTable(String tableName) throws Exception {
        try {
            HBaseAdmin hBaseAdmin = new HBaseAdmin(configuration);
            hBaseAdmin.disableTable(tableName);
            hBaseAdmin.deleteTable(tableName);
            System.out.println("Deleted table " + tableName + ", done!");
        } catch (MasterNotRunningException e) {
            e.printStackTrace();
        } catch (ZooKeeperConnectionException e) {
            e.printStackTrace();
        }
    }
    public static void addRecord(String tableName, String rowKey, String family, String qualifier, String value) throws Exception {
        try {
            HTable table = new HTable(configuration, tableName);
            Put put = new Put(Bytes.toBytes(rowKey));
            //TODO: add or addColumn? Depreciation?
            put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));
            table.put(put);
            System.out.println("Inserted record " + rowKey + " to table "
                    + tableName + ", done!");

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public static void deleteRecord(String tableName, String rowKey) throws IOException{
        //TODO:When shall we throw IOException and Exception?
        HTable hTable = new HTable(configuration, tableName);
        List<Delete> list = new ArrayList<>();
        Delete delete = new Delete(rowKey.getBytes());
        list.add(delete);
        hTable.delete(list);
        System.out.println("Deleted " + tableName + ", done!");
    }
    public static void getOneRecord(String tableName, String rowKey) throws IOException {
        try {
            HTable hTable = new HTable(configuration, tableName);
            Get get = new Get(rowKey.getBytes());
            Result result = hTable.get(get);
            for (KeyValue keyValue : result.raw()) {
                System.out.print(new String(keyValue.getRow()) + " " );
                System.out.print(new String(keyValue.getFamily()) + ":" );
                System.out.print(new String(keyValue.getQualifier()) + " " );
                System.out.print(keyValue.getTimestamp() + " " );
                System.out.println(new String(keyValue.getValue()));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public static void main(String[] args) {
        try {
            String tablename = "scores";
            String[] families = { "grade", "course" };
            HBaseTable.createTable(tablename, families);
            // add record zkb
            HBaseTable.addRecord(tablename, "zkb", "grade", "", "5");
            HBaseTable.addRecord(tablename, "zkb", "course", "", "90");
            HBaseTable.addRecord(tablename, "zkb", "course", "math", "97");
            HBaseTable.addRecord(tablename, "zkb", "course", "art", "87");
            // add record baoniu
            HBaseTable.addRecord(tablename, "baoniu", "grade", "", "4");
            HBaseTable.addRecord(tablename, "baoniu", "course", "math", "89");

            System.out.println("===========get one record========");
            HBaseTable.getOneRecord(tablename, "zkb");

            System.out.println("===========show all record========");
            HBaseTable.getAllRecord(tablename);

            System.out.println("===========del one record========");
            HBaseTable.deleteRecord(tablename, "baoniu");
            HBaseTable.getAllRecord(tablename);

            System.out.println("===========show all record========");
            HBaseTable.getAllRecord(tablename);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void getAllRecord(String tablename) {
        try {
            HTable hTable = new HTable(configuration, tablename);
            Scan scan = new Scan();
            ResultScanner resultScanner = hTable.getScanner(scan);
            for (Result result : resultScanner) {
                for (KeyValue keyValue : result.raw()) {
                    System.out.print(new String(keyValue.getRow()) + " ");
                    System.out.print(new String(keyValue.getFamily()) + ":");
                    System.out.print(new String(keyValue.getQualifier()) + " ");
                    System.out.print(keyValue.getTimestamp() + " ");
                    System.out.println(new String(keyValue.getValue()));
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class HBaseHandler {

    private Connection conn;
    private Admin admin;
    private TableName tableName;

    public HBaseHandler(){
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "emr-worker-2");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        configuration.set("hbase.master", "127.0.0.1:60000");
        try{
            conn = ConnectionFactory.createConnection(configuration);
            admin = conn.getAdmin();
        } catch(IOException e) {
            System.out.println(e);
        }
    }

    public HBaseHandler(String namespace) throws IOException{
        this();
        admin.createNamespace(NamespaceDescriptor.create(namespace).build());
    }

    public void setTable(String name, String[] colFamilys) throws IOException{
        tableName = TableName.valueOf(name);
        // 建表
        if (admin.tableExists(tableName)) {
            System.out.println("Table already exists");
        } else {
            HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
            for(String colFamily : colFamilys){
                HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(colFamily);
                hTableDescriptor.addFamily(hColumnDescriptor);
            }
            admin.createTable(hTableDescriptor);
            System.out.println("Table " + name + " create successful");
        }
    }


    public void putValue(int rowKey, String colFamily, String colName, byte[] value)
            throws IOException{
        // 插入数据
        Put put = new Put(Bytes.toBytes(rowKey)); // row key
        put.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(colName), value); // col1
        conn.getTable(tableName).put(put);
        String str = new String(value, StandardCharsets.UTF_8);
        System.out.println("value of " + str + "  insert success");
    }

    public void putRow(int rowKey, String name, String student_id, int class_no, int understanding, int programming)
            throws IOException{
        putValue(rowKey, "name", "name", Bytes.toBytes(name));
        putValue(rowKey, "info", "student_id", Bytes.toBytes(student_id));
        putValue(rowKey, "info", "class", Bytes.toBytes(class_no));
        putValue(rowKey, "score", "understanding", Bytes.toBytes(understanding));
        putValue(rowKey, "score", "programming", Bytes.toBytes(programming));
        System.out.println("row of " + rowKey + "  insert success");
    }

    public void putRow(int rowKey, String name, String student_id)
            throws IOException{
        putValue(rowKey, "name", "name", Bytes.toBytes(name));
        putValue(rowKey, "info", "student_id", Bytes.toBytes(student_id));
    }
}
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class Main {
    public static void main(String[] args) throws IOException {
        HBaseHandler handler = new HBaseHandler("ma");
        String[] colFamilys = {"name", "info", "score"};
        handler.setTable("student", colFamilys);
        handler.putRow(1, "Tom", "20210000000001", 1, 75, 82);
        handler.putRow(2, "Jerry", "20210000000002", 1, 85, 67);
        handler.putRow(3, "Jack", "20210000000003", 2, 80, 80);
        handler.putRow(4, "Rose", "20210000000004", 2, 60, 61);
        handler.putRow(5, "ma", "G20220735020157");


//            // 查看数据
//            Get get = new Get(Bytes.toBytes(rowKey));
//            if (!get.isCheckExistenceOnly()) {
//                Result result = conn.getTable(tableName).get(get);
//                for (Cell cell : result.rawCells()) {
//                    String colName = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
//                    String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
//                    System.out.println("Data get success, colName: " + colName + ", value: " + value);
//                }
//            }

            // 删除数据
//        Delete delete = new Delete(Bytes.toBytes(rowKey));      // 指定rowKey
//        conn.getTable(tableName).delete(delete);
//        System.out.println("Delete Success");
//
//        // 删除表
//        if (admin.tableExists(tableName)) {
//            admin.disableTable(tableName);
//            admin.deleteTable(tableName);
//            System.out.println("Table Delete Successful");
//        } else {
//            System.out.println("Table does not exist!");
//        }

    }
}

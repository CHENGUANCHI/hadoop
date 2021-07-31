import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HBaseOper {
    private static String nameSpace = "guan";
    private static String tableName = "student";

    private static String columnInfo = "info";
    private static String columnScore = "score";

    public static void main(String[] args) throws IOException {
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "jikehadoop01,jikehadoop02,jikehadoop03");
        config.set("hbase.zookeeper.property.clientPort", "2181");
        Connection connection = ConnectionFactory.createConnection(config);
        Admin admin = connection.getAdmin();

        if(admin.tableExists(TableName.valueOf(nameSpace, tableName))){
            admin.deleteTable(TableName.valueOf(nameSpace, tableName));
            System.out.println(nameSpace + ":" + tableName + " delete");
        }else{
//            NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(nameSpace).build();
//            admin.createNamespace(namespaceDescriptor);
            ColumnFamilyDescriptor infoColumnDiscriptor = ColumnFamilyDescriptorBuilder.of(columnInfo);
            ColumnFamilyDescriptor scoreColumnDiscriptor = ColumnFamilyDescriptorBuilder.of(columnScore);
            List<ColumnFamilyDescriptor> columnFamilyDescriptorList = new ArrayList<>();
            columnFamilyDescriptorList.add(infoColumnDiscriptor);
            columnFamilyDescriptorList.add(scoreColumnDiscriptor);

            TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(TableName.valueOf(nameSpace,tableName));
            TableDescriptor tableDescriptor = tableDescriptorBuilder.setColumnFamilies(columnFamilyDescriptorList).build();

            admin.createTable(tableDescriptor);

            insertData(connection, nameSpace, tableName, "Tom", columnInfo, "student_id", "20210000000001");
            insertData(connection, nameSpace, tableName, "Tom", columnInfo, "class", "1");
            insertData(connection, nameSpace, tableName, "Tom", columnScore, "understanding", "75");
            insertData(connection, nameSpace, tableName, "Tom", columnScore, "programming", "82");

            insertData(connection, nameSpace, tableName, "Jerry", columnInfo, "student_id", "20210000000002");
            insertData(connection, nameSpace, tableName, "Jerry", columnInfo, "class", "1");
            insertData(connection, nameSpace, tableName, "Jerry", columnScore, "understanding", "85");
            insertData(connection, nameSpace, tableName, "Jerry", columnScore, "programming", "67");

            insertData(connection, nameSpace, tableName, "Jack", columnInfo, "student_id", "20210000000003");
            insertData(connection, nameSpace, tableName, "Jack", columnInfo, "class", "2");
            insertData(connection, nameSpace, tableName, "Jack", columnScore, "understanding", "80");
            insertData(connection, nameSpace, tableName, "Jack", columnScore, "programming", "80");

            insertData(connection, nameSpace, tableName, "Rose", columnInfo, "student_id", "20210000000004");
            insertData(connection, nameSpace, tableName, "Rose", columnInfo, "class", "2");
            insertData(connection, nameSpace, tableName, "Rose", columnScore, "understanding", "60");
            insertData(connection, nameSpace, tableName, "Rose", columnScore, "programming", "61");

            insertData(connection, nameSpace, tableName, "guan", columnInfo, "student_id", "G20200388030212");
            insertData(connection, nameSpace, tableName, "guan", columnInfo, "class", "4");
            insertData(connection, nameSpace, tableName, "guan", columnScore, "understanding", "100");
            insertData(connection, nameSpace, tableName, "guan", columnScore, "programming", "100");

        }

    }

    public static void insertData(Connection connection,String namespace, String tableName, String rowKey, String colFamily, String col, String val) throws IOException {
        Table table = connection.getTable(TableName.valueOf(namespace, tableName));
        Put put = new Put(rowKey.getBytes());
        put.addColumn(colFamily.getBytes(),col.getBytes(),val.getBytes());
        table.put(put);
        table.close();
    }
}

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

import java.io.IOException;
import java.lang.String;

import static org.apache.hadoop.hbase.util.Bytes.toBytes;

public class SuperTable {

    public static void main(String[] args) throws IOException {

        // Instantiating configuration class
        Configuration con = HBaseConfiguration.create();

        // Instantiating HbaseAdmin class
        HBaseAdmin admin = new HBaseAdmin(con);

        // Instantiating table descriptor class
        String tableName = "powers";
        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));

        // Adding column families to table descriptor
        tableDescriptor.addFamily(new HColumnDescriptor("personal"));
        tableDescriptor.addFamily(new HColumnDescriptor("professional"));

        // Execute the table through admin
        admin.createTable(tableDescriptor);
        System.out.println(" Table created ");

        HTable hTable = new HTable(con, tableName);

        // Instantiating Put class
        // accepts a row name.

        // adding values using add() method
        // accepts column family name, qualifier/row name ,value
        Put p1 = createPut("row1", "superman", "strength", "clark", "100");
        hTable.put(p1);
        Put p2 = createPut("row2", "batman", "money", "bruce", "50");
        hTable.put(p2);
        Put p3 = createPut("row3", "wolverine", "healing", "logan", "75");
        hTable.put(p3);

        // Saving the put Instance to the HTable.
        System.out.println("data inserted");

        hTable.flushCommits();
        hTable.close();

        // Instantiate the Scan class


        // Instantiating HTable class
        HTable table = new HTable(con, tableName);

        // Instantiating the Scan class
        Scan scan = new Scan();

        // Scanning the required columns
        scan.addColumn(toBytes("personal"), toBytes("hero"));

        // Getting the scan result
        ResultScanner scanner = table.getScanner(scan);

        // Reading values from scan result
        for (Result result = scanner.next(); result != null; result = scanner.next()) {
            System.out.println(result);
        }
        //closing the scanner
        scanner.close();
    }

    private static Put createPut(String rowName, String heroType, String powerName, String name, String xp) {
        Put result = new Put(toBytes(rowName));
        result.addColumn(toBytes("personal"), toBytes("hero"), toBytes(heroType));
        result.addColumn(toBytes("personal"), toBytes("power"), toBytes(powerName));
        result.addColumn(toBytes("professional"), toBytes("name"), toBytes(name));
        result.addColumn(toBytes("professional"), toBytes("xp"), toBytes(xp));
        return result;
    }
}


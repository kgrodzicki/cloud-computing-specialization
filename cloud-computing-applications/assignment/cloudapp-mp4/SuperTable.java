import java.io.IOException;

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

import org.apache.hadoop.hbase.util.Bytes;

public class SuperTable{

   public static void main(String[] args) throws IOException {

      // Instantiate Configuration class

      // Instaniate HBaseAdmin class
      
      // Instantiate table descriptor class

      // Add column families to table descriptor

      // Execute the table through admin

      // Instantiating HTable class
     
      // Repeat these steps as many times as necessary

	      // Instantiating Put class
              // Hint: Accepts a row name

      	      // Add values using add() method
              // Hints: Accepts column family name, qualifier/row name ,value

      // Save the table
	
      // Close table

      // Instantiate the Scan class
     
      // Scan the required columns

      // Get the scan result

      // Read values from scan result
      // Print scan result
 
      // Close the scanner
   
      // Htable closer
   }
}


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;

/**
 * Created by nichunen on 16/10/16.
 */

public class hbase_api {

    public static void main(String[] args) {


        Configuration conf = new Configuration();
        java.net.URL xml1 = hbase_api.class.getResource("hbase-site.xml");
        java.net.URL xml2 = hbase_api.class.getResource("core-site.xml");
        conf.addResource(xml1);
        conf.addResource(xml2);

        HBaseConfiguration hBaseConf = new HBaseConfiguration(conf);

        /*
        try {
            HBaseAdmin admin = new HBaseAdmin(hBaseConf);
            Boolean yesma = admin.tableExists("test");
            HTableDescriptor tableDesc = new HTableDescriptor("test");
            tableDesc.addFamily(new HColumnDescriptor("info"));
            admin.createTable(tableDesc);
        } catch (MasterNotRunningException e) {
            e.printStackTrace();
        } catch (ZooKeeperConnectionException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        */


        /*
        try {
            HTable table = new HTable(hBaseConf, "test");
            Put put = new Put("rowkey1".getBytes());
            put.addImmutable("info".getBytes(), "A".getBytes(), "1".getBytes());
            table.put(put);
            table.flushCommits();
            table.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        */

        try {
            HTable table = new HTable(hBaseConf, "test");
            Scan scan = new Scan();
            ResultScanner res = table.getScanner(scan);
            for(Result r: res) {
                for(KeyValue kv: r.raw()){
                    System.out.println("Rowkey => " + new String(r.getRow()) + " family => " + new String(kv.getFamily())
                    + " qualifier => " + new String(kv.getQualifier()) + " timestamp => " + kv.getTimestamp());
                }

            }

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

}

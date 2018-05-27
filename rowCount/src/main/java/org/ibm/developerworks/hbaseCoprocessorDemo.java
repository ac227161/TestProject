package org.ibm.developerworks;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.hadoop.hbase.util.Bytes;
import org.ibm.developerworks.getRowCount.getRowCountRequest;
import org.ibm.developerworks.getRowCount.getRowCountResponse;
import org.ibm.developerworks.getRowCount.ibmDeveloperWorksService;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * demo program to show how to program with HBase coprocessor
 */
public class hbaseCoprocessorDemo {
    void createTable(String tableName) {
        try {
            Configuration config = new Configuration();
            HBaseAdmin admin = new HBaseAdmin(config);
            HTableDescriptor tableDesc = new HTableDescriptor(tableName);
            if (admin.tableExists(tableName) == true) {
                admin.disableTable(tableName);
                admin.deleteTable(tableName);
            }
            tableDesc.addFamily(new HColumnDescriptor("c1")); //add column family
            tableDesc.addCoprocessor("org.ibm.developerworks.coprocessor.RowCountObserver");
            tableDesc.addCoprocessor("org.ibm.developerworks.coprocessor.RowCountEndpoint");
            admin.createTable(tableDesc);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    void populateTenRows(String tableName, int rowCount) {
        try {
            Configuration config = new Configuration();
            HConnection conn = HConnectionManager.createConnection(config);
            HTableInterface tbl = conn.getTable(tableName);
            //insert 1000
            for (int i = 0; i < 1000; i++) {
                String rowkey = "r" + Integer.toString(i);
                Put put = new Put(rowkey.getBytes());
                put.add("c1".getBytes(), "col1".getBytes(), "v".getBytes());
                tbl.put(put);
            }
            for (int i = 0; i < 1000 - rowCount; i++) {
                String rowkey = "r" + Integer.toString(i);
                Delete d = new Delete(rowkey.getBytes());
                tbl.delete(d);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    long getTableRowCountBatch(String tableName) {
        long totalRowCount = 0;
        Map<byte[], getRowCountResponse> results = null;
        try {
            Configuration config = new Configuration();
            HConnection connection = HConnectionManager.createConnection(config);
            HTableInterface table = connection.getTable(tableName);

            org.ibm.developerworks.getRowCount.getRowCountRequest.Builder builder = getRowCountRequest.newBuilder();
            builder.setReCount(false);

            byte[] s = Bytes.toBytes("r1");
            byte[] e = Bytes.toBytes("t1");
            results = table.batchCoprocessorService(ibmDeveloperWorksService.getDescriptor().findMethodByName("getRowCount"), builder.build(), s, e, getRowCountResponse.getDefaultInstance());
        } catch (Exception e) {
            e.printStackTrace();
        } catch (Throwable t) {
        }
        Collection<getRowCountResponse> resultsc = results.values();
        for (getRowCountResponse r : resultsc) {
            totalRowCount += r.getRowCount();
        }

        return totalRowCount;
    }

    long getTableRowCountFast(String tableName) {
        System.out.println("getTableRowCountFast invoked for " + tableName);
        final AtomicLong totalRowCount = new AtomicLong();
        try {
            Configuration config = new Configuration();
            HConnection connection = HConnectionManager.createConnection(config);
            HTableInterface table = connection.getTable(tableName);


            Batch.Call<ibmDeveloperWorksService, getRowCountResponse> callable = new Batch.Call<ibmDeveloperWorksService, getRowCountResponse>() {
                ServerRpcController controller = new ServerRpcController();
                BlockingRpcCallback<getRowCountResponse> rpcCallback = new BlockingRpcCallback<getRowCountResponse>();

                @Override
                public getRowCountResponse call(ibmDeveloperWorksService instance) throws IOException {
                    org.ibm.developerworks.getRowCount.getRowCountRequest.Builder builder = getRowCountRequest.newBuilder();
                    builder.setReCount(false);
                    instance.getRowCount(controller, builder.build(), rpcCallback);
                    return rpcCallback.get();
                }
            };
            Batch.Callback<getRowCountResponse> callback = new Batch.Callback<getRowCountResponse>() {
                @Override
                public void update(byte[] region, byte[] row, getRowCountResponse result) {
                    totalRowCount.getAndAdd(result.getRowCount());
                }
            };

            table.coprocessorService(ibmDeveloperWorksService.class, null, null, callable, callback);
        } catch (Exception e) {
            e.printStackTrace();
        } catch (Throwable t) {
        }
        return totalRowCount.get();
    }

    long getTableRowCountSlow(String tableName) {
        System.out.println("getTableRowCountSlow invoked for " + tableName);
        Map<byte[], getRowCountResponse> results = null;
        try {
            Configuration config = new Configuration();
            HConnection connection = HConnectionManager.createConnection(config);
            HTableInterface table = connection.getTable(tableName);

            Batch.Call<ibmDeveloperWorksService, getRowCountResponse> callable = new Batch.Call<ibmDeveloperWorksService, getRowCountResponse>() {
                ServerRpcController controller = new ServerRpcController();
                BlockingRpcCallback<getRowCountResponse> rpcCallback = new BlockingRpcCallback<getRowCountResponse>();

                @Override
                public getRowCountResponse call(ibmDeveloperWorksService instance) throws IOException {
                    org.ibm.developerworks.getRowCount.getRowCountRequest.Builder builder = getRowCountRequest.newBuilder();
                    builder.setReCount(true);
                    instance.getRowCount(controller, builder.build(), rpcCallback);
                    return rpcCallback.get();
                }
            };

            results = table.coprocessorService(ibmDeveloperWorksService.class, null, null, callable);
        } catch (Exception e) {
            e.printStackTrace();
        } catch (Throwable t) {
        }

        long totalRowCount = 0;
        Collection<getRowCountResponse> resultsc = results.values();
        for (getRowCountResponse r : resultsc) {
            totalRowCount += r.getRowCount();
        }

        return totalRowCount;
    }

    long wholeTableRegionCount(String tableName, boolean fastMethod) {
        if (fastMethod == false) {
            return getTableRowCountSlow(tableName);
            //p.s. 没有使用batch方法，读者可以自己修改这行代码，进行测试。注释掉上面一行，使用下面这行代码即可。
            //return getTableRowCountBatch(tableName);
        } else
            return getTableRowCountFast(tableName);
    }

    long singleRegionCount(String tableName, String rowkey, boolean reCount) {
        long rowcount = 0;
        try {
            Configuration config = new Configuration();
            HConnection conn = HConnectionManager.createConnection(config);
            HTableInterface tbl = conn.getTable(tableName);
            CoprocessorRpcChannel channel = tbl.coprocessorService(rowkey.getBytes());
            org.ibm.developerworks.getRowCount.ibmDeveloperWorksService.BlockingInterface service = org.ibm.developerworks.getRowCount.ibmDeveloperWorksService.newBlockingStub(channel);
            org.ibm.developerworks.getRowCount.getRowCountRequest.Builder request = org.ibm.developerworks.getRowCount.getRowCountRequest.newBuilder();
            request.setReCount(reCount);
            org.ibm.developerworks.getRowCount.getRowCountResponse ret = service.getRowCount(null, request.build());
            rowcount = ret.getRowCount();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return rowcount;
    }

    public static void main(String[] args) {
        System.out.println("Hello HBase Coprocessor!");
        int argsize = args.length;
        System.out.println("input " + argsize + " arguments");
        if (argsize < 3) return;
        String tblName = args[0];
        boolean fastMethod = (Integer.parseInt(args[1]) == 1) ? true : false;
        int num = Integer.parseInt(args[2]);
        String userRowKey = "r900"; //default one
        boolean wholeTableCount = true;
        if (argsize == 4) {
            userRowKey = args[3];
            wholeTableCount = false;
        }
        hbaseCoprocessorDemo demo = new hbaseCoprocessorDemo();
        demo.createTable(tblName);
        demo.populateTenRows(tblName, num);
        if (wholeTableCount) {
            long wholeCount = demo.wholeTableRegionCount(tblName, fastMethod);
            System.out.println("Get whole table count : " + wholeCount);
        } else {
            long singleCount = demo.singleRegionCount(tblName, userRowKey, fastMethod);
            System.out.println("Get single count: " + singleCount);
        }
        System.out.println("bye!");
    }
}

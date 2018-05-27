package org.ibm.developerworks.coprocessor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class RowCountObserver extends BaseRegionObserver {
    RegionCoprocessorEnvironment env;
    private static final Log LOG = LogFactory.getLog(RowCountObserver.class);
    private String zNodePath = "/hbase/ibmdeveloperworks/demo";
    private ZooKeeperWatcher zkw = null;
    private long myrowcount = 0;
    private boolean initcount = false;
    private HRegion m_region;

    @Override
    public void start(CoprocessorEnvironment e) throws IOException {
        env = (RegionCoprocessorEnvironment) e;
        RegionCoprocessorEnvironment re = (RegionCoprocessorEnvironment) e;
        RegionServerServices rss = re.getRegionServerServices();
        m_region = re.getRegion();
        zNodePath = zNodePath + m_region.getRegionNameAsString();
        zkw = rss.getZooKeeper();
        myrowcount = 0; //count;
        initcount = false;

        try {
            if (ZKUtil.checkExists(zkw, zNodePath) == -1) {
                LOG.error("LIULIUMI: cannot find the znode");
                ZKUtil.createWithParents(zkw, zNodePath);
                LOG.info("znode path is : " + zNodePath);
            }
        } catch (Exception ee) {
            LOG.error("LIULIUMI: create znode fail");
        }
    }

    @Override
    public void stop(CoprocessorEnvironment e) throws IOException {
        // nothing to do here
    }

    @Override
    public void preDelete(ObserverContext<RegionCoprocessorEnvironment> e,
                          Delete delete,
                          WALEdit edit,
                          Durability durability) throws IOException {
        myrowcount--;
        try {
            ZKUtil.setData(zkw, zNodePath, Bytes.toBytes(myrowcount));
        } catch (Exception ee) {
            LOG.info("setData exception");
        }
    }

    @Override
    public void postOpen(ObserverContext<RegionCoprocessorEnvironment> e) {
        LOG.info("LIULIUMI post open invoked");
        long count = 0;
        try {
            if (initcount == false) {
                Scan scan = new Scan();
                InternalScanner scanner = null;
                scanner = m_region.getScanner(scan);
                List<Cell> results = new ArrayList<Cell>();
                boolean hasMore = false;
                do {
                    hasMore = scanner.next(results);
                    if (results.size() > 0)
                        count++;
                } while (hasMore);
                initcount = true;
            }
            ZKUtil.setData(zkw, zNodePath, Bytes.toBytes(count));
        } catch (Exception ee) {
            LOG.info("setData exception");
        }
    }

    @Override
    public void prePut(ObserverContext<RegionCoprocessorEnvironment> e,
                       Put put,
                       WALEdit edit,
                       Durability durability) throws IOException {
        myrowcount++;
        try {
            ZKUtil.setData(zkw, zNodePath, Bytes.toBytes(myrowcount));
        } catch (Exception ee) {
            LOG.info("setData exception");
        }
    }

    @Override
    public void postPut(ObserverContext<RegionCoprocessorEnvironment> e,
                        Put put,
                        WALEdit edit,
                        Durability durability) throws IOException {
        HTableInterface tbl = e.getEnvironment().getTable(m_region.getTableDesc().getTableName());
    }
}

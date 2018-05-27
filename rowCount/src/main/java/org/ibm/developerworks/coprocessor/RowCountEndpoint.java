/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.ibm.developerworks.coprocessor;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.CoprocessorException;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.ibm.developerworks.getRowCount;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class RowCountEndpoint extends getRowCount.ibmDeveloperWorksService implements Coprocessor, CoprocessorService {
    private RegionCoprocessorEnvironment env;
    private static final Log LOG = LogFactory.getLog(RowCountEndpoint.class);

    private String zNodePath = "/hbase/ibmdeveloperworks/demo";
    private ZooKeeperWatcher zkw = null;

    public RowCountEndpoint() {
    }

    /**
     * Just returns a reference to this object, which implements the RowCounterService interface.
     */
    @Override
    public Service getService() {
        return this;
    }

    /**
     * Stores a reference to the coprocessor environment provided by the
     * {@link org.apache.hadoop.hbase.regionserver.RegionCoprocessorHost} from the region where this
     * coprocessor is loaded.  Since this is a coprocessor endpoint, it always expects to be loaded
     * on a table region, so always expects this to be an instance of
     * {@link RegionCoprocessorEnvironment}.
     *
     * @param env the environment provided by the coprocessor host
     * @throws IOException if the provided environment is not an instance of
     *                     {@code RegionCoprocessorEnvironment}
     */
    @Override
    public void start(CoprocessorEnvironment envi) throws IOException {
        if (envi instanceof RegionCoprocessorEnvironment) {
            this.env = (RegionCoprocessorEnvironment) envi;
            RegionCoprocessorEnvironment re = (RegionCoprocessorEnvironment) envi;
            RegionServerServices rss = re.getRegionServerServices();
            zkw = rss.getZooKeeper();
            zNodePath = zNodePath + re.getRegion().getRegionNameAsString();
            try {
                if (ZKUtil.checkExists(zkw, zNodePath) == -1) {
                    LOG.info("LIULIUMI: create znode :" + zNodePath);
                    ZKUtil.createWithParents(zkw, zNodePath);
                } else {
                    LOG.info("LIULIUMI: znode exist");
                }
            } catch (Exception ee) {
                ee.printStackTrace();
            }

        } else {
            throw new CoprocessorException("Must be loaded on a table region!");
        }
    }

    @Override
    public void stop(CoprocessorEnvironment env) throws IOException {
        // nothing to do
    }

    @Override
    public void getRowCount(RpcController controller, getRowCount.getRowCountRequest request,
                            RpcCallback<getRowCount.getRowCountResponse> done) {
        boolean reCount = request.getReCount();
        long rowcount = 0;
        if (reCount == true) {
            InternalScanner scanner = null;
            try {
                Scan scan = new Scan();
                scanner = env.getRegion().getScanner(scan);
                List<Cell> results = new ArrayList<Cell>();
                boolean hasMore = false;

                do {
                    hasMore = scanner.next(results);
                    rowcount++;
                } while (hasMore);
            } catch (IOException ioe) {
            } finally {
                if (scanner != null) {
                    try {
                        scanner.close();
                    } catch (IOException ignored) {
                    }
                }
            }
        } else {
            try {
                byte[] data = ZKUtil.getData(zkw, zNodePath);
                rowcount = Bytes.toLong(data);
                LOG.info("LIULIUMI: get rowcount " + rowcount);
            } catch (Exception e) {
                LOG.info("Exception during getData");
            }
        }
        getRowCount.getRowCountResponse.Builder responseBuilder = getRowCount.getRowCountResponse.newBuilder();
        responseBuilder.setRowCount(rowcount);
        done.run(responseBuilder.build());
    }

}

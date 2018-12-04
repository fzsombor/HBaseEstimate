import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class HBaseEst {

    // to estimate the row count
    private static final double DELTA_FROM_AVERAGE = 0.15;


    // Copied from Hive's HBaseStorageHandler.java.
    public static final String DEFAULT_PREFIX = "default.";

    // Number of rows fetched during the row count estimation per region
    public static final int ROW_COUNT_ESTIMATE_BATCH_SIZE = 10;

    // Minimum number of regions that are checked to estimate the row count
    private static final int MIN_NUM_REGIONS_TO_CHECK = 5;

    // Name of table in HBase.
    // 'this.name' is the alias of the HBase table in Hive.
    protected String hbaseTableName_ = "hbasetest";

    // Input format class for HBase tables read by Hive.
    private static final String HBASE_INPUT_FORMAT =
            "org.apache.hadoop.hive.hbase.HiveHBaseTableInputFormat";

    // Serialization class for HBase tables set in the corresponding Metastore table.
    private static final String HBASE_SERIALIZATION_LIB =
            "org.apache.hadoop.hive.hbase.HBaseSerDe";

    // Storage handler class for HBase tables read by Hive.
    private static final String HBASE_STORAGE_HANDLER =
            "org.apache.hadoop.hive.hbase.HBaseStorageHandler";

    // Column family of HBase row key
    private static final String ROW_KEY_COLUMN_FAMILY = ":key";

    // Keep the conf around
    private final static Configuration hbaseConf_ = HBaseConfiguration.create();

    // Cached column families. Used primarily for speeding up row stats estimation
    // (see IMPALA-4211).
    private HColumnDescriptor[] columnFamilies_ = null;


    HBaseEst(String tablename) {
        System.out.println("creating instance");
        hbaseTableName_= tablename;

        hbaseConf_.set("hbase.zookeeper.quorum", "krumpli-1.vpc.cloudera.com");
        hbaseConf_.setInt("hbase.zookeeper.property.clientPort", 2181);

    }

    public ClusterStatus getClusterStatus() throws IOException {
        Admin admin = null;
        ClusterStatus clusterStatus = null;
        try {
            Connection connection = HBaseEst.ConnectionHolder.getConnection(hbaseConf_);
            admin = connection.getAdmin();
            clusterStatus = admin.getClusterStatus();
        } finally {
            if (admin != null) admin.close();
        }
        return clusterStatus;
    }


    private Pair<Long, Long> getEstimatedRowStatsForRegion(HRegionLocation location,
                                                           boolean isCompressed, ClusterStatus clusterStatus) throws IOException {
        HRegionInfo info = location.getRegionInfo();

        Scan s = new Scan(info.getStartKey());
        // Get a small sample of rows
        s.setBatch(ROW_COUNT_ESTIMATE_BATCH_SIZE);
        // Try and get every version so the row's size can be used to estimate.
        s.setMaxVersions(Short.MAX_VALUE);
        // Don't cache the blocks as we don't think these are
        // necessarily important blocks.
        s.setCacheBlocks(false);
        // Try and get deletes too so their size can be counted.
        s.setRaw(false);

        org.apache.hadoop.hbase.client.Table table = getHBaseTable();
        ResultScanner rs = table.getScanner(s);

        long currentRowSize = 0;
        long currentRowCount = 0;

        try {
            // Get the the ROW_COUNT_ESTIMATE_BATCH_SIZE fetched rows
            // for a representative sample
            for (int i = 0; i < ROW_COUNT_ESTIMATE_BATCH_SIZE; ++i) {
                Result r = rs.next();
                if (r == null)
                    break;
                // Check for empty rows, see IMPALA-1451
                if (r.isEmpty())
                    continue;
                System.out.println("currentRowCount: " + ++currentRowCount);
                // To estimate the number of rows we simply use the amount of bytes
                // returned from the underlying buffer. Since HBase internally works
                // with these structures as well this gives us ok estimates.
                Cell[] cells = r.rawCells();
                for (Cell c : cells) {
                    if (c instanceof KeyValue) {
                        currentRowSize += KeyValue.getKeyValueDataStructureSize(c.getRowLength(),
                                c.getFamilyLength(), c.getQualifierLength(), c.getValueLength(),
                                c.getTagsLength());
                        System.out.println("currentRowSize: " + currentRowSize);
                    } else {
                        throw new IllegalStateException("Celltype " + c.getClass().getName() +
                                " not supported.");
                    }
                }
            }
        } finally {
            rs.close();
            closeHBaseTable(table);
        }

        // If there are no rows then no need to estimate.
        if (currentRowCount == 0) return new Pair<Long, Long>(0L, 0L);
        // Get the size.
        long currentSize = getRegionSize(location, clusterStatus);
        // estimate the number of rows.
        double bytesPerRow = currentRowSize / (double) currentRowCount;
        if (currentSize == 0) {
            return new Pair<Long, Long>(currentRowCount, (long) bytesPerRow);
        }

        // Compression factor two is only a best effort guess
        long estimatedRowCount =
                (long) ((isCompressed ? 2 : 1) * (currentSize / bytesPerRow));

        return new Pair<Long, Long>(estimatedRowCount, (long) bytesPerRow);
    }

    /**
     * Get an estimate of the number of rows and bytes per row in regions between
     * startRowKey and endRowKey.
     * <p>
     * This number is calculated by incrementally checking as many region servers as
     * necessary until we observe a relatively constant row size per region on average.
     * Depending on the skew of data in the regions this can either mean that we need
     * to check only a minimal number of regions or that we will scan all regions.
     * <p>
     * The HBase region servers periodically update the master with their metrics,
     * including storefile size. We get the size of the storefiles for all regions in
     * the cluster with a single call to getClusterStatus from the master.
     * <p>
     * The accuracy of this number is determined by the number of rows that are written
     * and kept in the memstore and have not been flushed until now. A large number
     * of key-value pairs in the memstore will lead to bad estimates as this number
     * is not reflected in the storefile size that is used to estimate this number.
     * <p>
     * Currently, the algorithm does not consider the case that the key range used as a
     * parameter might be generally of different size than the rest of the region.
     * <p>
     * The values computed here should be cached so that in high qps workloads
     * the nn is not overwhelmed. Could be done in load(); Synchronized to make
     * sure that only one thread at a time is using the htable.
     *
     * @param startRowKey First row key in the range
     * @param endRowKey   Last row key in the range
     * @return The estimated number of rows in the regions between the row keys (first) and
     * the estimated row size in bytes (second).
     */
    public  Pair<Long, Long> getEstimatedRowStats(byte[] startRowKey,
                                                              byte[] endRowKey) {
        Preconditions.checkNotNull(startRowKey);
        Preconditions.checkNotNull(endRowKey);

        boolean isCompressed = false;
        long rowCount = 0;
        long rowSize = 0;
        org.apache.hadoop.hbase.client.Table table = null;
        try {
            table = getHBaseTable();
            ClusterStatus clusterStatus = getClusterStatus();

            // Check to see if things are compressed.
            // If they are we'll estimate a compression factor.
            if (columnFamilies_ == null) {
                columnFamilies_ = table.getTableDescriptor().getColumnFamilies();
            }
            Preconditions.checkNotNull(columnFamilies_);
            for (HColumnDescriptor desc : columnFamilies_) {
                isCompressed |= desc.getCompression() != Compression.Algorithm.NONE;
            }

            // Fetch all regions for the key range
            System.out.println("Getregions");
            List<HRegionLocation> locations = getRegionsInRange(table, startRowKey, endRowKey);
            Collections.shuffle(locations);
            // The following variables track the number and size of 'rows' in
            // HBase and allow incremental calculation of the average and standard
            // deviation.
            StatsHelper<Long> statsSize = new StatsHelper<Long>();
            long totalEstimatedRows = 0;

            // Collects stats samples from at least MIN_NUM_REGIONS_TO_CHECK
            // and at most all regions until the delta is small enough.
            while ((statsSize.count() < MIN_NUM_REGIONS_TO_CHECK ||
                    statsSize.stddev() > statsSize.mean() * DELTA_FROM_AVERAGE) &&
                    statsSize.count() < locations.size()) {
                HRegionLocation currentLocation = locations.get((int) statsSize.count());
                Pair<Long, Long> tmp = getEstimatedRowStatsForRegion(currentLocation,
                        isCompressed, clusterStatus);
                totalEstimatedRows += tmp.first;
                statsSize.addSample(tmp.second);
            }

            // Sum up the total size for all regions in range.
            long totalSize = 0;
            for (final HRegionLocation location : locations) {
                totalSize += getRegionSize(location, clusterStatus);
            }
            if (totalSize == 0) {
                rowCount = totalEstimatedRows;
            } else {
                rowCount = (long) (totalSize / statsSize.mean());
            }
            rowSize = (long) statsSize.mean();
        } catch (IOException ioe) {
            // Print the stack trace, but we'll ignore it
            // as this is just an estimate.
            // TODO: Put this into the per query log.
            System.out.println("Error computing HBase row count estimate");
            return new Pair<Long, Long>(-1l, -1l);
        } finally {
            if (table != null) closeHBaseTable(table);
        }
        return new Pair<Long, Long>(rowCount, rowSize);
    }

    public long getRegionSize(HRegionLocation location, ClusterStatus clusterStatus) {
        System.out.println("getting region size");
        HRegionInfo info = location.getRegionInfo();
        ServerLoad serverLoad = clusterStatus.getLoad(location.getServerName());

        // If the serverLoad is null, the master doesn't have information for this region's
        // server. This shouldn't normally happen.
        if (serverLoad == null) {
            System.out.println("Unable to find load for server: " + location.getServerName() +
                    " for location " + info.getRegionNameAsString());
            return 0;
        }
        RegionLoad regionLoad = serverLoad.getRegionsLoad().get(info.getRegionName());

        final long megaByte = 1024L * 1024L;
        System.out.println("regionsize: "+ regionLoad.getStorefileSizeMB() * megaByte);
        return regionLoad.getStorefileSizeMB() * megaByte;
    }

    /**
     * Connection instances are expensive to create. The HBase documentation recommends
     * one and then sharing it among threads. All operations on a connection are
     * thread-safe.
     */
    private static class ConnectionHolder {
        private static Connection connection_ = null;

        public static synchronized Connection getConnection(Configuration conf)
                throws IOException {
            if (connection_ == null || connection_.isClosed()) {
                connection_ = ConnectionFactory.createConnection(conf);
            }
            return connection_;
        }
    }

    /**
     * Table client objects are thread-unsafe and cheap to create. The HBase docs recommend
     * creating a new one for each task and then closing when done.
     */
    public org.apache.hadoop.hbase.client.Table getHBaseTable() throws IOException {
        return HBaseEst.ConnectionHolder.getConnection(hbaseConf_)
                .getTable(TableName.valueOf(hbaseTableName_));
    }

    private void closeHBaseTable(org.apache.hadoop.hbase.client.Table table) {
        try {
            table.close();
        } catch (IOException e) {
            System.out.println("Error closing HBase table: " + hbaseTableName_);
        }
    }

    public static List<HRegionLocation> getRegionsInRange(
            org.apache.hadoop.hbase.client.Table hbaseTbl,
            final byte[] startKey, final byte[] endKey) throws IOException {
        final boolean endKeyIsEndOfTable = Bytes.equals(endKey, HConstants.EMPTY_END_ROW);
        if ((Bytes.compareTo(startKey, endKey) > 0) && !endKeyIsEndOfTable) {
            throw new IllegalArgumentException("Invalid range: " +
                    Bytes.toStringBinary(startKey) + " > " + Bytes.toStringBinary(endKey));
        }
        final List<HRegionLocation> regionList = new ArrayList<HRegionLocation>();
        byte[] currentKey = startKey;
        Connection connection = HBaseEst.ConnectionHolder.getConnection(hbaseConf_);
        // Make sure only one thread is accessing the hbaseTbl.
        synchronized (hbaseTbl) {
            RegionLocator locator = connection.getRegionLocator(hbaseTbl.getName());
            do {
                // always reload region location info.
                HRegionLocation regionLocation = locator.getRegionLocation(currentKey, true);
                regionList.add(regionLocation);
                currentKey = regionLocation.getRegionInfo().getEndKey();
            } while (!Bytes.equals(currentKey, HConstants.EMPTY_END_ROW) &&
                    (endKeyIsEndOfTable || Bytes.compareTo(currentKey, endKey) < 0));
        }
        return regionList;
    }
}

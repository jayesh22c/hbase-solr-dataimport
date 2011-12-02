package org.apache.solr.handler.dataimport;

import static org.apache.solr.handler.dataimport.DataImportHandlerException.SEVERE;
import static org.apache.solr.handler.dataimport.DataImportHandlerException.wrapAndThrow;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.solr.handler.dataimport.HbaseQuery.HbaseColumn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HbaseDataSource<T> extends DataSource<T> {
    private static final Logger LOG = LoggerFactory.getLogger(HbaseDataSource.class);

    public final static String HBASE_ROW_ID = "ROW_KEY";

    private HTablePool tablePool;
    private Map<String, HTableInterface> htableList = new HashMap<String, HTableInterface>();
    
    private int poolMaxSize = 10;

    @Override
    public void close() {
        for (String htableName : htableList.keySet()) {
            tablePool.closeTablePool(htableName);
        }
    }

    @Override
    public T getData(String arg0) {
        throw new Error("Do not use this method");
    }

    public Iterator<Map<String, Object>> getHBaseData(HbaseQuery query) {
        ResultSetIterator r = new ResultSetIterator(query);
        return r.getIterator();
    }

    @Override
    public void init(Context arg0, Properties arg1) {
        String host = arg1.getProperty("host");
        Configuration config = HBaseConfiguration.create();
        config.set(HConstants.ZOOKEEPER_QUORUM, host);
        tablePool = new HTablePool(config, poolMaxSize);
    }

    private void logError(String msg, Exception e) {
        LOG.warn(msg, e);
    }
    
    private HTableInterface getHTable(String tableName) {
        HTableInterface htable = null;
        synchronized (htableList) {
            htable = htableList.get(tableName);
            if (htable == null){
                htable = tablePool.getTable(tableName);
                htableList.put(tableName, htable);
            }
        }
        return htable;
    }
    
    private class ResultSetIterator {

        ResultScanner resultScanner = null;
        Iterator<Result> resultIterator;
        List<HbaseColumn> columns;
        Iterator<Map<String, Object>> rSetIterator;
        
        public ResultSetIterator(HbaseQuery query) {

            try {
                String startRow = query.getStartRow();
                String stopRow = query.getStopRow();
                String tableName = query.getTableName();
                
                columns = query.getColumns();
                HTableInterface htable = getHTable(tableName);
                
                if (query.isSingleRow()) {
                    Get get = new Get(Bytes.toBytes(startRow));

                    if (columns != null) {
                        for (HbaseColumn column : columns) {
                            get.addColumn(column.getFamily().getBytes(), column.getColumnName().getBytes());
                        }
                    }
                    
                    Result result = htable.get(get);
                    List<Result> resultList = new ArrayList<Result>(1);
                    resultList.add(result);
                    resultIterator = resultList.iterator();
                    
                } else {
                    Scan scan = new Scan();

                    if (startRow != null) {
                        scan.setStartRow(startRow.getBytes());
                    }

                    if (stopRow != null) {
                        scan.setStopRow(stopRow.getBytes());
                    }

                    if (columns != null) {
                        for (HbaseColumn column : columns) {
                            scan.addColumn(column.getFamily().getBytes(), column.getColumnName().getBytes());
                        }
                    }

                    LOG.debug("Executing scanner: " + query);

                    long start = System.currentTimeMillis();

                    resultScanner = htable.getScanner(scan);

                    LOG.trace("Time taken for scanner: " + (System.currentTimeMillis() - start));

                    resultIterator = resultScanner.iterator();
                }

            } catch (Exception e) {
                wrapAndThrow(SEVERE, e, "Unable to execute SCANNER: " + query);
            }

            if (!resultIterator.hasNext()) {
                rSetIterator = new ArrayList<Map<String, Object>>().iterator();
                return;
            }

            rSetIterator = new Iterator<Map<String, Object>>() {
                public boolean hasNext() {
                    return hasnext();
                }

                public Map<String, Object> next() {
                    return getARow();
                }

                public void remove() {
                }
            };
        }

        private Iterator<Map<String, Object>> getIterator() {
            return rSetIterator;
        }

        private Map<String, Object> getARow() {
            if (resultIterator == null)
                return null;
            Result res = resultIterator.next();
            Map<String, Object> result = new HashMap<String, Object>();
            if (!res.isEmpty()) {
                if (columns != null) {
                    for (HbaseColumn column : columns) {
                        byte[] value = res.getValue(column.getFamily().getBytes(), column.getColumnName().getBytes());
                        if (value != null) {
                            result.put(column.getColumnName(), Bytes.toStringBinary(value));
                        }
                    }
                }
                String rowKey = Bytes.toStringBinary(res.getRow());
                result.put(HBASE_ROW_ID, rowKey);
            }

            return result;
        }

        private boolean hasnext() {
            if (resultIterator == null)
                return false;
            try {
                if (resultIterator.hasNext()) {
                    return true;
                } else {
                    close();
                    return false;
                }

            } catch (Exception e) {
                close();
                wrapAndThrow(SEVERE, e);
                return false;
            }
        }

        private void close() {
            try {
                if (resultScanner != null) {
                    resultScanner.close();
                }
            } catch (Exception e) {
                logError("Exception while closing result scanner", e);
            } finally {
            }
        }
    }
}

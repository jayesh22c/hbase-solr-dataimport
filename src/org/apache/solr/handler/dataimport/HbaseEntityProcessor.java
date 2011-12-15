package org.apache.solr.handler.dataimport;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HbaseEntityProcessor extends EntityProcessorBase {
    private static final Logger LOG = LoggerFactory.getLogger(HbaseEntityProcessor.class);

    protected HbaseDataSource<Iterator<Map<String, Object>>> dataSource;

    @Override
    @SuppressWarnings("unchecked")
    public void init(Context context) {
        super.init(context);
        DataSource<Iterator<Map<String, Object>>> ds = context.getDataSource();
        if (ds instanceof HbaseDataSource) {
            dataSource = (HbaseDataSource<Iterator<Map<String, Object>>>) ds;
        }
    }

    protected void initQuery(HbaseQuery q) {
        try {
            DataImporter.QUERY_COUNT.get().incrementAndGet();
            rowIterator = dataSource.getHBaseData(q);
            this.query = q.toString();
        } catch (DataImportHandlerException e) {
            throw e;
        } catch (Exception e) {
            LOG.error("The query failed '" + q + "'", e);
            throw new DataImportHandlerException(DataImportHandlerException.SEVERE, e);
        }
    }

    @Override
    public Map<String, Object> nextRow() {
        if (rowIterator == null) {
            HbaseQuery q = getHQuery();
            initQuery(q);
        }
        return getNext();
    }

    @Override
    public Map<String, Object> nextModifiedRowKey() {
        if (rowIterator == null) {
            HbaseQuery q = new HbaseQuery();
            String tableName = context.getEntityAttribute(TABLE_NAME);
            q.setTableName(tableName);
            // ${dataimporter.last_index_time}
            String startRow = context.getEntityAttribute(START_ROW);
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            try {
                startRow = context.replaceTokens(startRow);
                Date startRowDate = sdf.parse(startRow);
                q.setStartRow(String.valueOf(startRowDate.getTime()));
                q.setStopRow(null);
                initQuery(q);
            } catch (ParseException e) {
                LOG.error("The query failed '" + q + "'", e);
                throw new DataImportHandlerException(DataImportHandlerException.SEVERE, e);
            }
        }
        return getNext();
    }

    @Override
    public Map<String, Object> nextDeletedRowKey() {
        return null;
    }

    @Override
    public Map<String, Object> nextModifiedParentRowKey() {
        return null;
    }

    public HbaseQuery getHQuery() {
        HbaseQuery result = new HbaseQuery();
        result.setTableName(context.getEntityAttribute(TABLE_NAME));
        result.setColumns(context.getEntityAttribute(COLUMNS));
        if (Context.DELTA_DUMP.equals(context.currentProcess())) {
            String startRow = context.replaceTokens("${dataimporter.delta." + HbaseDataSource.HBASE_ROW_ID + "}");
            String stopRow = context.replaceTokens("${dataimporter.delta." + HbaseDataSource.HBASE_ROW_ID + "}");
            result.setStartRow(startRow);
            result.setStopRow(stopRow);
        } else {
            // it is a FULL import, nothing to do here
        }

        return result;

    }

    private static final String START_ROW = "startRow";

    private static final String STOP_ROW = "stopRow";

    public static final String TABLE_NAME = "tableName";

    public static final String COLUMNS = "columns";
}

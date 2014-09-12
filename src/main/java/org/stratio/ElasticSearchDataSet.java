package org.stratio;

import org.apache.metamodel.data.AbstractDataSet;
import org.apache.metamodel.data.DefaultRow;
import org.apache.metamodel.data.Row;
import org.apache.metamodel.query.SelectItem;
import org.apache.metamodel.schema.Column;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

final class ElasticSearchDataSet extends AbstractDataSet {

    private static final Logger logger = LoggerFactory
            .getLogger(ElasticSearchDataSet.class);

    private static int readCount = 0;

    private final SearchHit[] _cursor;
    private final boolean _queryPostProcessed;

    private boolean _closed;
    private volatile SearchHit _dbObject;

    public ElasticSearchDataSet(SearchResponse cursor, Column[] columns,
                          boolean queryPostProcessed) {
        super(columns);
        _cursor = cursor.getHits().hits();
        _queryPostProcessed = queryPostProcessed;
        _closed = false;
    }

    public boolean isQueryPostProcessed() {
        return _queryPostProcessed;
    }

    @Override
    public void close() {
        super.close();
        //_cursor.close();
        _closed = true;
    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        if (!_closed) {
            logger.warn(
                    "finalize() invoked, but DataSet is not closed. Invoking close() on {}",
                    this);
            close();
        }
    }

    @Override
    public boolean next() {
        if (readCount<_cursor.length) {
            _dbObject = _cursor[readCount];
            readCount++;
            return true;
        } else {
            _dbObject = null;
            return false;
        }
    }

    @Override
    public Row getRow() {
        if (_dbObject == null) {
            return null;
        }

        final int size = getHeader().size();
        final Object[] values = new Object[size];
        for (int i = 0; i < values.length; i++) {
            final SelectItem selectItem = getHeader().getSelectItem(i);
            final Map<String, Object> element = _dbObject.getSource();
            final String key = selectItem.getColumn().getName();
            values[i] = element.get(key);
        }
        return new DefaultRow(getHeader(), values);
    }
}

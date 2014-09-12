package org.stratio;

import junit.framework.TestCase;
import org.apache.metamodel.schema.ColumnType;

public class ElasticSearchMetaDataParserTest extends TestCase {

    public void testParseMetadataInfo() throws Exception {
        String metaDataInfo = "{message={type=long}, postDate={type=date, format=dateOptionalTime}, anotherDate={type=date, format=dateOptionalTime}, user={type=string}}";

        ElasticSearchMetaData metaData = ElasticSearchMetaDataParser.parse(metaDataInfo);
        String[] columnNames = metaData.getColumnNames();
        ColumnType[] columnTypes = metaData.getColumnTypes();

        assertTrue(columnNames.length==4);
        assertEquals(columnNames[0], "message");
        assertEquals(columnNames[1], "postDate");
        assertEquals(columnNames[2], "anotherDate");
        assertEquals(columnNames[3], "user");
        assertTrue(columnTypes.length == 4);
        assertEquals(columnTypes[0], ColumnType.BIGINT);
        assertEquals(columnTypes[1], ColumnType.DATE);
        assertEquals(columnTypes[2], ColumnType.DATE);
        assertEquals(columnTypes[3], ColumnType.VARCHAR);
    }
}

package org.stratio;

import junit.framework.TestCase;
import org.apache.metamodel.schema.ColumnType;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.compress.CompressedString;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.apache.metamodel.DataContext;

import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class ElasticSearchDataContextTest extends TestCase {

    private EmbeddedElasticsearchServer embeddedElasticsearchServer;
    private String indexName = "twitter";
    private String indexType = "tweet";

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        embeddedElasticsearchServer = new EmbeddedElasticsearchServer();
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
        embeddedElasticsearchServer.shutdown();
    }

    protected Client getClient() {
        return embeddedElasticsearchServer.getClient();
    }

    public void testParseMetadataInfo() throws Exception {
        String metaDataInfo = "{message={type=long}, postDate={type=date, format=dateOptionalTime}, anotherDate={type=date, format=dateOptionalTime}, user={type=string}}";

        ElasticSearchMetaData metaData = ElasticSearchMetaDataParser.parse(metaDataInfo);
        String[] columnNames = metaData.getColumnNames();
        ColumnType[] columnTypes = metaData.getColumnTypes();

        assertTrue(columnNames.length==4);
        assertEquals(columnNames[0],"message");
        assertEquals(columnNames[1],"postDate");
        assertEquals(columnNames[2],"anotherDate");
        assertEquals(columnNames[3],"user");
        assertTrue(columnTypes.length==4);
        assertEquals(columnTypes[0], ColumnType.BIGINT);
        assertEquals(columnTypes[1], ColumnType.DATE);
        assertEquals(columnTypes[2], ColumnType.DATE);
        assertEquals(columnTypes[3], ColumnType.VARCHAR);
    }

    public void testRead() throws Exception {
        indexOneThousandDocuments(getClient());

        final DataContext dataContext = new ElasticSearchDataContext(getClient());

        assertEquals("[twitter]", Arrays.toString(dataContext.getDefaultSchema().getTableNames()));
/*
        // delete if already exists
        {
            col.drop();
            col = db.createCollection("my_collection", null);
        }


        // Instantiate the actual data context
        final DataContext dataContext = new MongoDbDataContext(db);

        assertEquals("[my_collection, system.indexes]", Arrays.toString(dataContext.getDefaultSchema().getTableNames()));
        Table table = dataContext.getDefaultSchema().getTableByName("my_collection");
        assertEquals("[_id, baz, foo, id, list, name]", Arrays.toString(table.getColumnNames()));

        assertEquals(ColumnType.MAP, table.getColumnByName("baz").getType());
        assertEquals(ColumnType.VARCHAR, table.getColumnByName("foo").getType());
        assertEquals(ColumnType.LIST, table.getColumnByName("list").getType());
        assertEquals(ColumnType.INTEGER, table.getColumnByName("id").getType());
        assertEquals(ColumnType.ROWID, table.getColumnByName("_id").getType());

        DataSet ds = dataContext.query().from("my_collection").select("name").and("foo").and("baz").and("list").where("id")
                .greaterThan(800).or("foo").isEquals("bar").execute();
        assertEquals(MongoDbDataSet.class, ds.getClass());
        assertFalse(((MongoDbDataSet) ds).isQueryPostProcessed());
        try {
            assertTrue(ds.next());
            assertEquals("Row[values=[record no. 0, bar, {count=0, constant=foobarbaz}, [ \"l1\" , \"l2\" , \"l3\" , 0]]]",
                    ds.getRow().toString());

            assertTrue(ds.next());
            assertEquals("Row[values=[record no. 5, bar, {count=5, constant=foobarbaz}, [ \"l1\" , \"l2\" , \"l3\" , 5]]]",
                    ds.getRow().toString());

            assertTrue(ds.next());
            assertEquals(
                    "Row[values=[record no. 10, bar, {count=10, constant=foobarbaz}, [ \"l1\" , \"l2\" , \"l3\" , 10]]]", ds
                    .getRow().toString());

            for (int j = 15; j < 801; j++) {
                if (j % 5 == 0) {
                    assertTrue(ds.next());
                    assertEquals("Row[values=[record no. " + j + ", bar, {count=" + j
                            + ", constant=foobarbaz}, [ \"l1\" , \"l2\" , \"l3\" , " + j + "]]]", ds.getRow().toString());
                }
            }

            assertTrue(ds.next());
            assertTrue(ds.getRow().getValue(2) instanceof Map);
            assertEquals(LinkedHashMap.class, ds.getRow().getValue(2).getClass());

            assertTrue("unexpected type: " + ds.getRow().getValue(3).getClass(), ds.getRow().getValue(3) instanceof List);
            assertEquals(BasicDBList.class, ds.getRow().getValue(3).getClass());

            assertEquals(
                    "Row[values=[record no. 801, baz, {count=801, constant=foobarbaz}, [ \"l1\" , \"l2\" , \"l3\" , 801]]]",
                    ds.getRow().toString());
            assertTrue(ds.next());
            assertEquals(
                    "Row[values=[record no. 802, baz, {count=802, constant=foobarbaz}, [ \"l1\" , \"l2\" , \"l3\" , 802]]]",
                    ds.getRow().toString());
            assertTrue(ds.next());
            assertEquals(
                    "Row[values=[record no. 803, baz, {count=803, constant=foobarbaz}, [ \"l1\" , \"l2\" , \"l3\" , 803]]]",
                    ds.getRow().toString());
            assertTrue(ds.next());
            assertEquals(
                    "Row[values=[record no. 804, baz, {count=804, constant=foobarbaz}, [ \"l1\" , \"l2\" , \"l3\" , 804]]]",
                    ds.getRow().toString());
            assertTrue(ds.next());
            assertEquals(
                    "Row[values=[record no. 805, bar, {count=805, constant=foobarbaz}, [ \"l1\" , \"l2\" , \"l3\" , 805]]]",
                    ds.getRow().toString());

            for (int i = 0; i < 194; i++) {
                assertTrue(ds.next());
            }
            assertEquals(
                    "Row[values=[record no. 999, baz, {count=999, constant=foobarbaz}, [ \"l1\" , \"l2\" , \"l3\" , 999]]]",
                    ds.getRow().toString());
            assertFalse(ds.next());
        } finally {
            ds.close();
        }

        ds = dataContext.query().from("my_collection").select("id").and("name").where("id").in(2, 6, 8, 9).execute();
        assertTrue(ds.next());
        assertEquals("Row[values=[2, record no. 2]]", ds.getRow().toString());
        assertTrue(ds.next());
        assertEquals("Row[values=[6, record no. 6]]", ds.getRow().toString());
        assertTrue(ds.next());
        assertEquals("Row[values=[8, record no. 8]]", ds.getRow().toString());
        assertTrue(ds.next());
        assertEquals("Row[values=[9, record no. 9]]", ds.getRow().toString());
        assertFalse(ds.next());
        ds.close();

        ds = dataContext.query().from("my_collection").select("id").and("name").where("foo").isEquals("bar").execute();
        assertEquals(MongoDbDataSet.class, ds.getClass());
        assertFalse(((MongoDbDataSet) ds).isQueryPostProcessed());

        try {
            List<Object[]> objectArrays = ds.toObjectArrays();
            assertEquals(200, objectArrays.size());
            assertEquals("[0, record no. 0]", Arrays.toString(objectArrays.get(0)));
        } finally {
            ds.close();
        }

        // do a query that we cannot push to mongo
        ds = dataContext.query().from("my_collection")
                .select(FunctionType.SUM, dataContext.getDefaultSchema().getTables()[0].getColumnByName("id")).where("foo")
                .isEquals("bar").execute();
        assertEquals(InMemoryDataSet.class, ds.getClass());

        ds.close();*/
    }

    private void indexOneThousandDocuments(Client client) {
        BulkRequestBuilder bulkRequest = client.prepareBulk();

        try {
        for (int i = 0; i < 1000; i++) {
            bulkRequest.add(client.prepareIndex(indexName, indexType)
                    .setSource(buildJsonObject(i)));
        }
        bulkRequest.execute().actionGet();
        } catch (Exception ex) {
           System.out.println("Exception indexing documents!!!!!");
        }

    }

    private XContentBuilder buildJsonObject(int elementId) throws Exception {
        return jsonBuilder().startObject().field("user", "user" + elementId)
                .field("postDate", new Date())
                .field("message", elementId)
                .endObject();
    }
}
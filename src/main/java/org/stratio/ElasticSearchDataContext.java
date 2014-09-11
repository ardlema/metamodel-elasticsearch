package org.stratio;

import com.sun.corba.se.spi.ior.ObjectId;
import org.apache.metamodel.DataContext;
import org.apache.metamodel.MetaModelException;
import org.apache.metamodel.QueryPostprocessDataContext;
import org.apache.metamodel.data.DataSet;
import org.apache.metamodel.schema.*;
import org.apache.metamodel.util.SimpleTableDef;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.hppc.cursors.ObjectCursor;
import org.elasticsearch.common.hppc.cursors.ObjectObjectCursor;

import java.util.*;

public class ElasticSearchDataContext extends QueryPostprocessDataContext
        implements DataContext {

    private final Client elasticSearchClient;
    private final SimpleTableDef[] tableDefs;
    private Schema schema;

    public ElasticSearchDataContext(Client client, SimpleTableDef... tableDefs) {
        this.elasticSearchClient = client;
        this.schema = null;
        this.tableDefs = tableDefs;
    }

    public ElasticSearchDataContext(Client client) {
        this(client, detectSchema(client));
    }

    public static SimpleTableDef[] detectSchema(Client client) {
        List<String> indexNames = new ArrayList<String>();
        ClusterStateResponse clusterStateResponse = client.admin().cluster().prepareState().execute().actionGet();
        ImmutableOpenMap<String,IndexMetaData> indexes = clusterStateResponse.getState().getMetaData().getIndices();
        //TODO: FOR EACH INDEX GET THE MAPPING
        for (ObjectCursor<String> typeCursor : indexes.keys())
            indexNames.add(typeCursor.value);
        SimpleTableDef[] result = new SimpleTableDef[indexNames.size()];
        int i = 0;
        for (String indexName : indexNames) {
            ClusterState cs = client.admin().cluster().prepareState().setIndices(indexName).execute().actionGet().getState();
            IndexMetaData imd = cs.getMetaData().index(indexName);
            ImmutableOpenMap<String, MappingMetaData> mappings = imd.getMappings();

                try {
                    SimpleTableDef table = detectTable(client, indexName);
                    result[i] = table;
                } catch(Exception e) {}
                i++;

        }
        return result;
    }

    public static SimpleTableDef detectTable(Client client, String indexName) throws Exception {
        ClusterState cs = client.admin().cluster().prepareState().setIndices(indexName).execute().actionGet().getState();
        IndexMetaData imd = cs.getMetaData().index(indexName);
        MappingMetaData mappingMetaData = imd.mapping("tweet");
        Map<String, Object> mp = mappingMetaData.getSourceAsMap();
        Iterator it = mp.entrySet().iterator();
        Map.Entry pair = (Map.Entry)it.next();
        ElasticSearchMetaData metaData = ElasticSearchMetaDataParser.parse(pair.getValue().toString());
        return new SimpleTableDef(indexName, metaData.getColumnNames(), metaData.getColumnTypes());
    }


    @Override
    protected Schema getMainSchema() throws MetaModelException {
        if (schema == null) {
            MutableSchema theSchema = new MutableSchema(getMainSchemaName());
            for (SimpleTableDef tableDef: tableDefs) {
                MutableTable table = tableDef.toTable().setSchema(theSchema);

                theSchema.addTable(table);
            }

            schema = theSchema;
        }
        return schema;
    }

    @Override
    protected String getMainSchemaName() throws MetaModelException {
        return "ElasticSearchSchema";
    }

    @Override
    protected DataSet materializeMainSchemaTable(Table table, Column[] columns, int i) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }
}

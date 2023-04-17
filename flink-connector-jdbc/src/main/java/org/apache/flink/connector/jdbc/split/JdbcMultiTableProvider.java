package org.apache.flink.connector.jdbc.split;

import org.apache.flink.annotation.Experimental;

import java.io.Serializable;
import java.util.List;

/**
 * In the scenario of sub-database sub-table, based on the results of the original data fragmentation, Serializable[][] is split twice
 */
@Experimental
public class JdbcMultiTableProvider implements JdbcParameterValuesProvider  {

    /**
     * Correspondence between matching database connection and table
     */
    private List<TableItem> tables;


    /**
     * schema name
     */
    private String dbName;

    /**
     * The split result after the original jdbc data fragmentation configuration
     */
    private Serializable[][] partition;

    public JdbcMultiTableProvider(String dbName, List<TableItem> tables) {
        this.tables = tables;
        this.dbName = dbName;
    }

    /**
     * @return Returns the corresponding relationship between split fragments and data blocks, Serializable[partition][parameterValues]
     * The startup partition is the shard index, and parameterValues is the data parameter corresponding to each shard.
     */
    @Override
    public Serializable[][] getParameterValues() {
        int tableNum = tables.stream().mapToInt(item -> item.getTable().size()).sum();
        int splitCount = partition == null ? tableNum : tableNum * partition.length;
        int paramLength = partition == null ? 2 : 4;
        Serializable[][] parameters = new Serializable[splitCount][paramLength];
        int splitIndex = 0;

        for (TableItem tableItem : tables) {
            for (String table : tableItem.getTable()) {
                if (partition != null) {
                    for (Serializable[] serializables : partition) {
                        parameters[splitIndex][0] = tableItem.getUrl();
                        parameters[splitIndex][1] = table;
                        //数据分片配置
                        parameters[splitIndex][2] = serializables[0];
                        parameters[splitIndex][3] = serializables[1];
                        splitIndex++;
                    }
                } else {
                    parameters[splitIndex][0] = tableItem.getUrl();
                    parameters[splitIndex][1] = table;
                    splitIndex++;
                }
                parameters[splitIndex][2] = dbName;
            }
        }
        return parameters;
    }

    public JdbcParameterValuesProvider withPartition(JdbcNumericBetweenParametersProvider jdbcNumericBetweenParametersProvider) {
        if (null == jdbcNumericBetweenParametersProvider) {
            return this;
        }
        this.partition = jdbcNumericBetweenParametersProvider.getParameterValues();
        return this;
    }

    public static class TableItem {
        private String url;
        private List<String> table;

        public String getUrl() {
            return url;
        }

        public void setUrl(String url) {
            this.url = url;
        }

        public List<String> getTable() {
            return table;
        }

        public void setTable(List<String> table) {
            this.table = table;
        }
    }
}

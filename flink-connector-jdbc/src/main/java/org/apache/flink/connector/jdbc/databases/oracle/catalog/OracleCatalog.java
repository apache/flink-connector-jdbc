package org.apache.flink.connector.jdbc.databases.oracle.catalog;

import org.apache.commons.compress.utils.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.connector.jdbc.catalog.AbstractJdbcCatalog;
import org.apache.flink.connector.jdbc.databases.postgres.catalog.PostgresTablePath;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.*;
import org.apache.flink.table.catalog.exceptions.*;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;


/**
 * OracleCatalog 用于查表和查数据库,便于重建
 */
public class OracleCatalog extends AbstractJdbcCatalog {

    private static final Logger LOG = LoggerFactory.getLogger(OracleCatalog.class);

    public static final String DEFAULT_DATABASE = "TEST_SCHEMA";


    public static final String IDENTIFIER = "jdbc";
    private static final String ORACLE_DRIVER = "oracle.driver.OracleDriver";
    private OracleTypeMapper dialectTypeMapper;
    private static final Set<String> builtinDatabases = new HashSet<String>() {
        {
            add("SCOTT");
            add("ANONYMOUS");
            add("XS$NULL");
            add("DIP");
            add("SPATIAL_WFS_ADMIN_USR");
            add("SPATIAL_CSW_ADMIN_USR");
            add("APEX_PUBLIC_USER");
            add("ORACLE_OCM");
            add("MDDATA");
        }
    };

    public OracleCatalog(ClassLoader userClassLoader, String catalogName, String defaultDatabase, String username, String pwd, String baseUrl) {
        super(userClassLoader, catalogName, defaultDatabase, username, pwd, baseUrl);
        String driverVersion = Preconditions.checkNotNull(getDriverVersion(), "Driver version must not be null.");
        String databaseVersion = Preconditions.checkNotNull(getDatabaseVersion(), "Database version must not be null.");
        LOG.info("Driver version: {}, database version: {}", driverVersion, databaseVersion);
        this.dialectTypeMapper = new OracleTypeMapper(databaseVersion, driverVersion);
    }

    private String getDatabaseVersion() {
        try (Connection conn = DriverManager.getConnection(defaultUrl, username, pwd)) {
            return conn.getMetaData().getDatabaseProductVersion();
        } catch (Exception e) {
            throw new CatalogException( String.format("Failed in getting Oracle version by %s.", defaultUrl), e);
        }
    }

    private String getDriverVersion() {
        try (Connection conn = DriverManager.getConnection(defaultUrl, username, pwd)) {
            String driverVersion = conn.getMetaData().getDriverVersion();
            Pattern regexp = Pattern.compile("\\d+?\\.\\d+?\\.\\d+");
            Matcher matcher = regexp.matcher(driverVersion);
            return matcher.find() ? matcher.group(0) : null;
        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed in getting Oracle driver version by %s.", defaultUrl), e);

        }
    }

    @Override
    public List<String> listDatabases() throws CatalogException {
        return extractColumnValuesBySQL(this.defaultUrl,
                "select username from sys.dba_users " +
                        "where DEFAULT_TABLESPACE <> 'SYSTEM' and DEFAULT_TABLESPACE <> 'SYSAUX' " +
                        " order by username",
                1,
                dbName -> !builtinDatabases.contains(dbName));
    }

    @Override
    public CatalogDatabase getDatabase(String databaseName) throws DatabaseNotExistException, CatalogException {
        Preconditions.checkState(!StringUtils.isBlank(databaseName), "Database name must not be blank");
        if (listDatabases().contains(databaseName)) {
            return new CatalogDatabaseImpl(Collections.emptyMap(), null);
        } else {
            throw new DatabaseNotExistException(getName(), databaseName);
        }
    }


    @Override
    public List<String> listTables(String databaseName) throws DatabaseNotExistException, CatalogException {
        Preconditions.checkState(StringUtils.isNotBlank(databaseName), "Database name must not be blank.");
        if (!databaseExists(databaseName)){// 注意这个值是 oracle 实例名称
            throw new DatabaseNotExistException(getName(), databaseName);
        }

        List<String> listDatabases = listDatabases().stream().map(username -> "'" + username + "'")
                .collect(Collectors.toList());
        return extractColumnValuesBySQL(this.defaultUrl,
                "SELECT OWNER||'.'||TABLE_NAME AS schemaTableName FROM sys.all_tables WHERE OWNER IN (" + String.join(",", listDatabases) + ")"+
                        "ORDER BY OWNER,TABLE_NAME",1, null, null);
    }


    @Override
    public CatalogBaseTable getTable(ObjectPath tablePath) throws TableNotExistException, CatalogException {
        if (!tableExists(tablePath)) {
            throw new TableNotExistException(getName(), tablePath);
        }

        String databaseName = tablePath.getDatabaseName();
        String dbUrl = baseUrl;
        try(Connection conn = DriverManager.getConnection(dbUrl, username, pwd)) {
            DatabaseMetaData metaData = conn.getMetaData();
            Optional<UniqueConstraint> primaryKey = getPrimaryKey(metaData, databaseName, getSchemaName(tablePath), getTableName(tablePath));
            String statement = String.format("SELECT * FROM %s ", getSchemaTableName(tablePath)) ;
            PreparedStatement ps = conn.prepareStatement(statement);
            ResultSetMetaData resultSetMetaData = ps.getMetaData();

            String[] columnNames = new String[resultSetMetaData.getColumnCount()];
            DataType[] types = new DataType[resultSetMetaData.getColumnCount()];

            for (int i = 1; i<=resultSetMetaData.getColumnCount(); i++) {
                columnNames[i - 1] = resultSetMetaData.getColumnName(i);
                types[i - 1] = fromJDBCType(tablePath, resultSetMetaData,i);
                if (resultSetMetaData.isNullable(i) == ResultSetMetaData.columnNoNulls) {
                    types[i-1] = types[i-1].notNull();
                }
            }

            Schema.Builder schemaBuilder = Schema.newBuilder().fromFields(columnNames, types);
            primaryKey.ifPresent( pk -> schemaBuilder.primaryKeyNamed(pk.getName(), pk.getColumns()));
            Schema tableSchema = schemaBuilder.build();
            Map<String, String> props = new HashMap<>();
            props.put(FactoryUtil.CONNECTOR.key(), IDENTIFIER);
            props.put("username" , username);
            props.put("password", pwd);
            props.put("table_name", getSchemaTableName(tablePath));
            props.put("driverName", ORACLE_DRIVER);
            return CatalogTable.of(tableSchema, null, Lists.newArrayList(), props);

        } catch (Exception ex) {
            throw new CatalogException(String.format("Failed getting Table %s", tablePath.getFullName()), ex);
        }
    }

    @Override
    public boolean tableExists(ObjectPath tablePath) throws CatalogException {
        String[] schemaTableNames = getSchemaTableName(tablePath).split("\\.");
        return !extractColumnValuesBySQL(
                defaultUrl, "SELECT table_name FROM sys.all_tables where OWNER = ? and table_name = ?",
                1, null, schemaTableNames[0], schemaTableNames[1])
                .isEmpty();

    }

    protected List<String> extractColumnValuesBySQL(String connUrl, String sql, int columnIndex, Predicate<String> filterFunc, Object... params){
        List<String> columnValues = Lists.newArrayList();
        String sql1 = "SELECT table_name,OWNER FROM sys.all_tables";
        try (Connection conn = DriverManager.getConnection(connUrl, username, pwd);
             PreparedStatement ps = conn.prepareStatement(sql)){
            if (Objects.nonNull(params) && params.length >0){
                for (int i=0; i<params.length; i++) {
                    ps.setObject(i + 1, params[i]);
                }
            }
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {

//                String username = rs.getString("table_name");
//                String default_tablespace = rs.getString("OWNER");
////                String temporary_tablespace = rs.getString("temporary_tablespace");
//                System.out.println("username:"+username+"OWNER:"+default_tablespace);
                String columnValue = rs.getString(columnIndex);
                if (Objects.isNull(filterFunc) || filterFunc.test(columnValue)) {
                    columnValues.add(columnValue);
                }
//                return columnValues;
            }
        } catch (Exception ex){
            throw new CatalogException(String.format("The following SQL query could not be executed (%s): %s", connUrl, sql ), ex);
        }
        return columnValues;
    }

    protected String getSchemaTableName(ObjectPath tablePath) {
        return OracleTablePath.fromFlinkTableName(tablePath.getObjectName()).getFullPath();
    }

    protected String getSchemaName(ObjectPath tablePath) {
        return OracleTablePath.fromFlinkTableName(tablePath.getObjectName()).getPgSchemaName();
    }

    protected String getTableName(ObjectPath tablePath) {
        return OracleTablePath.fromFlinkTableName(tablePath.getObjectName()).getPgTableName();
    }


    protected DataType fromJDBCType(ObjectPath tablePath, ResultSetMetaData metadata, int colIndex)

            throws SQLException {

        return dialectTypeMapper.mapping(tablePath, metadata, colIndex);

    }
}

package org.apache.flink.connector.jdbc.databases.mysql.table;

import org.apache.flink.connector.jdbc.databases.mysql.MySqlTestBase;
import org.apache.flink.connector.jdbc.table.JdbcDynamicTableSinkITCase;
import org.apache.flink.types.Row;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

/** The Table Sink ITCase for MySql. */
public class MySqlDynamicTableSinkITCase extends JdbcDynamicTableSinkITCase
        implements MySqlTestBase {

    @Override
    protected List<Row> testData() {
        List<Row> data = new ArrayList<>();
        data.add(Row.of(1, 1L, "Hi", Timestamp.valueOf("1970-01-01 00:00:01")));
        data.add(Row.of(2, 2L, "Hello", Timestamp.valueOf("1970-01-01 00:00:02")));
        data.add(Row.of(3, 2L, "Hello world", Timestamp.valueOf("1970-01-01 00:00:03")));
        data.add(
                Row.of(
                        4,
                        3L,
                        "Hello world, how are you?",
                        Timestamp.valueOf("1970-01-01 00:00:00.004")));
        data.add(Row.of(5, 3L, "I am fine.", Timestamp.valueOf("1970-01-01 00:00:05")));
        data.add(Row.of(6, 3L, "Luke Skywalker", Timestamp.valueOf("1970-01-01 00:00:06")));
        data.add(Row.of(7, 4L, "Comment#1", Timestamp.valueOf("1970-01-01 00:00:07")));
        data.add(Row.of(8, 4L, "Comment#2", Timestamp.valueOf("1970-01-01 00:00:08")));
        data.add(Row.of(9, 4L, "Comment#3", Timestamp.valueOf("1970-01-01 00:00:09")));
        data.add(Row.of(10, 4L, "Comment#4", Timestamp.valueOf("1970-01-01 00:00:10")));
        data.add(Row.of(11, 5L, "Comment#5", Timestamp.valueOf("1970-01-01 00:00:11")));
        data.add(Row.of(12, 5L, "Comment#6", Timestamp.valueOf("1970-01-01 00:00:12")));
        data.add(Row.of(13, 5L, "Comment#7", Timestamp.valueOf("1970-01-01 00:00:13")));
        data.add(Row.of(14, 5L, "Comment#8", Timestamp.valueOf("1970-01-01 00:00:14")));
        data.add(Row.of(15, 5L, "Comment#9", Timestamp.valueOf("1970-01-01 00:00:15")));
        data.add(Row.of(16, 6L, "Comment#10", Timestamp.valueOf("1970-01-01 00:00:16")));
        data.add(Row.of(17, 6L, "Comment#11", Timestamp.valueOf("1970-01-01 00:00:17")));
        data.add(Row.of(18, 6L, "Comment#12", Timestamp.valueOf("1970-01-01 00:00:18")));
        data.add(Row.of(19, 6L, "Comment#13", Timestamp.valueOf("1970-01-01 00:00:19")));
        data.add(Row.of(20, 6L, "Comment#14", Timestamp.valueOf("1970-01-01 00:00:20")));
        data.add(Row.of(21, 6L, "Comment#15", Timestamp.valueOf("1970-01-01 00:00:21")));
        return data;
    }
}

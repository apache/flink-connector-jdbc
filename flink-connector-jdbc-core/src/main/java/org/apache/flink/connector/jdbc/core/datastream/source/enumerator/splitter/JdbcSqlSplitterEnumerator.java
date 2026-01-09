package org.apache.flink.connector.jdbc.core.datastream.source.enumerator.splitter;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.jdbc.core.datastream.source.enumerator.JdbcSqlSplitEnumeratorBase;
import org.apache.flink.connector.jdbc.core.datastream.source.enumerator.SqlTemplateSplitEnumerator;
import org.apache.flink.connector.jdbc.core.datastream.source.split.JdbcSourceSplit;
import org.apache.flink.connector.jdbc.datasource.connections.JdbcConnectionProvider;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * A splitter enumerator for JdbcSqlSplitEnumeratorBase.
 *
 * <p>This class is used to allow the retrofit of existing implementations of
 * JdbcSqlSplitEnumeratorBase to the new SplitterEnumerator interface. It delegates the split
 * enumeration to the underlying JdbcSqlSplitEnumeratorBase instance.
 */
@Deprecated
@Internal
public class JdbcSqlSplitterEnumerator implements SplitterEnumerator {
    private final JdbcSqlSplitEnumeratorBase.Provider<?> provider;
    private JdbcSqlSplitEnumeratorBase<?> base;
    private boolean finished;

    public JdbcSqlSplitterEnumerator(JdbcSqlSplitEnumeratorBase.Provider<?> provider) {
        this.provider = provider;
        this.base = provider.create();
        this.finished = false;
    }

    @Override
    public void start(JdbcConnectionProvider connectionProvider) {
        base.open();
    }

    @Override
    public void close() {
        base.close();
    }

    @Override
    public boolean isAllSplitsFinished() {
        return this.finished;
    }

    @Override
    public List<JdbcSourceSplit> enumerateSplits() {
        try {
            this.finished = true;
            return base.enumerateSplits(() -> true);
        } catch (Exception e) {
            throw new RuntimeException("Error enumerating splits", e);
        }
    }

    @Override
    public List<String> lineageQueries() {
        List<String> queries = new ArrayList<>();
        if (base instanceof SqlTemplateSplitEnumerator) {
            queries.add(((SqlTemplateSplitEnumerator) base).getSqlTemplate());
        }
        return queries;
    }

    @Override
    public Serializable serializableState() {
        return null;
    }

    @Override
    public SplitterEnumerator restoreState(Serializable state) {
        this.base = provider.restore(state);
        return this;
    }
}

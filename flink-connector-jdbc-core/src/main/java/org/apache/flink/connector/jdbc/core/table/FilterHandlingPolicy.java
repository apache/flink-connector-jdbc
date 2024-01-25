package org.apache.flink.connector.jdbc.core.table;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.DescribedEnum;
import org.apache.flink.configuration.description.InlineElement;

import static org.apache.flink.configuration.description.TextElement.text;

/** Fine-grained configuration to control filter push down for jdbc Table/SQL source. */
@PublicEvolving
public enum FilterHandlingPolicy implements DescribedEnum {
    ALWAYS("always", text("Always push the supported filters to database.")),

    NEVER("never", text("Never push any filters to database."));

    private final String name;
    private final InlineElement description;

    FilterHandlingPolicy(String name, InlineElement description) {
        this.name = name;
        this.description = description;
    }

    @Override
    public InlineElement getDescription() {
        return description;
    }

    @Override
    public String toString() {
        return name;
    }
}

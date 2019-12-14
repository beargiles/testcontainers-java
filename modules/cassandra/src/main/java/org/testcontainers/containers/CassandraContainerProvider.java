package org.testcontainers.containers;

import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.JdbcDatabaseContainerProvider;

/**
 * Implementation of JdbcDatabaseContainerProvider for Cassandra.
 */
public class CassandraContainerProvider extends JdbcDatabaseContainerProvider {
    @Override
    public boolean supports(String databaseType) {
        return databaseType.equals(CassandraContainer.NAME);
    }

    @Override
    public JdbcDatabaseContainer newInstance() {
        return newInstance(CassandraContainer.DEFAULT_TAG);
    }

    @Override
    public JdbcDatabaseContainer newInstance(String tag) {
        return new CassandraContainer(CassandraContainer.IMAGE + ":" + tag);
    }
}

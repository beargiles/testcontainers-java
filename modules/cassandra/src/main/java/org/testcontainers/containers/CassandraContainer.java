package org.testcontainers.containers;

import com.datastax.driver.core.Cluster;
import com.github.dockerjava.api.command.InspectContainerResponse;
import org.apache.commons.io.IOUtils;
import org.testcontainers.containers.delegate.CassandraDatabaseDelegate;
import org.testcontainers.delegate.DatabaseDelegate;
import org.testcontainers.ext.ScriptUtils;
import org.testcontainers.ext.ScriptUtils.ScriptLoadException;
import org.testcontainers.utility.MountableFile;

import javax.script.ScriptException;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

/**
 * Cassandra container
 *
 * Supports 2.x and 3.x Cassandra versions
 *
 * @author Eugeny Karpov
 */
public class CassandraContainer<SELF extends CassandraContainer<SELF>> extends JdbcDatabaseContainer<SELF> {

    public static final String NAME = "cassandra";
    public static final Integer CQL_PORT = 9042;
    public static final Integer THRIFT_PORT = 9160;
    public static final Integer JMX_PORT = 7199;
    private static final String CONTAINER_CONFIG_LOCATION = "/etc/cassandra";
    private static final String USERNAME = "cassandra";
    private static final String PASSWORD = "cassandra";
    private static final String DEFAULT_DRIVER_CLASSNAME = "com.github.adejanovski.cassandra.jdbc.CassandraDriver";

    public static final String IMAGE = "cassandra";
    public static final String DEFAULT_TAG = "3.11.2";

    private String configLocation;
    private String initScriptPath;
    private boolean enableNativeAPI;
    private boolean enableJmxReporting;
    private String driverClassname = DEFAULT_DRIVER_CLASSNAME;

    public CassandraContainer() {
        this(IMAGE + ":" + DEFAULT_TAG);
    }

    public CassandraContainer(String dockerImageName) {
        super(dockerImageName);
        addExposedPort(CQL_PORT);
        setStartupAttempts(3);
        this.enableNativeAPI = false;
        this.enableJmxReporting = false;
    }

    @Override
    protected Integer getLivenessCheckPort() {
        return getMappedPort(CQL_PORT);
    }

    @Override
    protected void configure() {
        optionallyMapResourceParameterAsVolume(CONTAINER_CONFIG_LOCATION, configLocation);
        if (enableNativeAPI) {
            addExposedPort(THRIFT_PORT);
            addEnv("CASSANDRA_START_RPC", "true");
        }
        if (enableJmxReporting) {
            addExposedPort(JMX_PORT);
        }
    }

    @Override
    protected void containerIsStarted(InspectContainerResponse containerInfo) {
        runInitScriptIfRequired();
    }

    /**
     * Load init script content and apply it to the database if initScriptPath is set
     */
    protected void runInitScriptIfRequired() {
        if (initScriptPath != null) {
            try {
                URL resource = Thread.currentThread().getContextClassLoader().getResource(initScriptPath);
                if (resource == null) {
                    logger().warn("Could not load classpath init script: {}", initScriptPath);
                    throw new ScriptLoadException("Could not load classpath init script: " + initScriptPath + ". Resource not found.");
                }
                String cql = IOUtils.toString(resource, StandardCharsets.UTF_8);
                DatabaseDelegate databaseDelegate = getDatabaseDelegate();
                ScriptUtils.executeDatabaseScript(databaseDelegate, initScriptPath, cql);
            } catch (IOException e) {
                logger().warn("Could not load classpath init script: {}", initScriptPath);
                throw new ScriptLoadException("Could not load classpath init script: " + initScriptPath, e);
            } catch (ScriptException e) {
                logger().error("Error while executing init script: {}", initScriptPath, e);
                throw new ScriptUtils.UncategorizedScriptException("Error while executing init script: " + initScriptPath, e);
            }
        }
    }

    /**
     * Map (effectively replace) directory in Docker with the content of resourceLocation if resource location is not null
     *
     * Protected to allow for changing implementation by extending the class
     *
     * @param pathNameInContainer path in docker
     * @param resourceLocation    relative classpath to resource
     */
    protected void optionallyMapResourceParameterAsVolume(String pathNameInContainer, String resourceLocation) {
        Optional.ofNullable(resourceLocation)
                .map(MountableFile::forClasspathResource)
                .ifPresent(mountableFile -> withCopyFileToContainer(mountableFile, pathNameInContainer));
    }

    /**
     * Initialize Cassandra with the custom overridden Cassandra configuration
     * <p>
     * Be aware, that Docker effectively replaces all /etc/cassandra content with the content of config location, so if
     * Cassandra.yaml in configLocation is absent or corrupted, then Cassandra just won't launch
     *
     * @param configLocation relative classpath with the directory that contains cassandra.yaml and other configuration files
     */
    public SELF withConfigurationOverride(String configLocation) {
        this.configLocation = configLocation;
        return self();
    }

    /**
     * Initialize Cassandra with init CQL script
     * <p>
     * CQL script will be applied after container is started (see using WaitStrategy)
     *
     * @param initScriptPath relative classpath resource
     */
    public SELF withInitScript(String initScriptPath) {
        this.initScriptPath = initScriptPath;
        return self();
    }

    /**
     * Initialize Cassandra with Native support (via thrift over RPC) with default driver classname.
     * This is usually required for JDBC access.
     */
    public SELF withNativeAPI(boolean enableNativeAPI) {
        return withNativeAPI(enableNativeAPI, DEFAULT_DRIVER_CLASSNAME);
    }

    /**
     * Initialize Cassandra with Native support (via thrift over RPC) using specified driver classname.
     * This is usually required for JDBC access.
     */
    public SELF withNativeAPI(boolean enableNativeAPI, String driverClassname) {
        this.enableNativeAPI = enableNativeAPI;
        this.driverClassname = driverClassname;
        return self();
    }

    /**
     * Initialize Cassandra client with JMX reporting enabled or disabled
     */
    public SELF withJmxReporting(boolean enableJmxReporting) {
        this.enableJmxReporting = enableJmxReporting;
        return self();
    }

    /**
     * Get username
     *
     * By default Cassandra has authenticator: AllowAllAuthenticator in cassandra.yaml
     * If username and password need to be used, then authenticator should be set as PasswordAuthenticator
     * (through custom Cassandra configuration) and through CQL with default cassandra-cassandra credentials
     * user management should be modified
     */
    public String getUsername() {
        return USERNAME;
    }

    /**
     * Get password
     *
     * By default Cassandra has authenticator: AllowAllAuthenticator in cassandra.yaml
     * If username and password need to be used, then authenticator should be set as PasswordAuthenticator
     * (through custom Cassandra configuration) and through CQL with default cassandra-cassandra credentials
     * user management should be modified
     */
    public String getPassword() {
        return PASSWORD;
    }

    /**
     * Get recommended driver classname.
     */
    @Override
    public String getDriverClassName() {
        return driverClassname;
    }

    /**
     * Get JDBC URL
     *
     * Returns appropriate JDBC URL if JDBC support is enabled. Otherwise throws UnsupportedOperationException.
     * If a keyspace is used it should be appended to the URL.
     */
    @Override
    public String getJdbcUrl() {
        if (enableNativeAPI) {
            return "jdbc:cassandra://" + getContainerIpAddress() + ":" + getMappedPort(THRIFT_PORT);
        }
        throw new UnsupportedOperationException();
    }

    /**
     * Gets SQL query that can be used to verify the server is up. This query returns
     * the server release version.
     */
    @Override
    protected String getTestQueryString() {
        return "SELECT release_version FROM system.local";
    }

    /**
     * Get mapped JQL port.
     *
     * @return
     */
    public int getCqlPort() {
        return getMappedPort(CQL_PORT);
    }

    /**
     * Get mapped Native (thrift-over-RPC) port. This is used by most JDBC driver implementations.
     *
     * @return thrift port, or -1 if NativeAPI is not enabled
     */
    public int getNativePort() {
        return enableNativeAPI ? getMappedPort(THRIFT_PORT) : -1;
    }

    /**
     * Get mapped JMX port.
     *
     * @return jmx port, or -1 if JMX is not enabled
     */
    public int getJmxPort() {
        return enableJmxReporting ? getMappedPort(JMX_PORT) : -1;
    }

    /**
     * Get configured Cluster
     *
     * Can be used to obtain connections to Cassandra in the container
     */
    public Cluster getCluster() {
        return getCluster(this, enableJmxReporting);
    }

    public static Cluster getCluster(ContainerState containerState, boolean enableJmxReporting) {
        final Cluster.Builder builder = Cluster.builder()
            .addContactPoint(containerState.getContainerIpAddress())
            .withPort(containerState.getMappedPort(CQL_PORT));
        if (!enableJmxReporting) {
            builder.withoutJMXReporting();
        }
        return builder.build();
    }

    public static Cluster getCluster(ContainerState containerState) {
        return getCluster(containerState, false);
    }

    /**
     * Close down server. This has no effect.
     */
    @Override
    public void close() {
        // no-op
    }

    protected DatabaseDelegate getDatabaseDelegate() {
        return new CassandraDatabaseDelegate(this);
    }
}

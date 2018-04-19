package com.bebe.zookeeper.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.URL;
import java.net.UnknownHostException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

public class Configuration {
    private static final Logger LOG = LoggerFactory.getLogger(Configuration.class);
    private static final Properties SERVER_PROPS = System.getProperties();
    private static final String CONF_PATH = "./conf/";
    private static final String KEY_RESOURCE_FILE = "resource";
    private static final String DEFAULT_PROPERTIES = "default.properties";
    private static Properties RESOURCE_PROPS;
    private static Properties DEFAULT_RESOURCE_PROPS;

    static {
        DEFAULT_RESOURCE_PROPS = getDefaultProperties();
        RESOURCE_PROPS = getSpecificProperties();
    }

    private static Properties getDefaultProperties() {
        ClassLoader classLoader = ClassLoader.getSystemClassLoader();

        InputStream is = classLoader.getResourceAsStream(DEFAULT_PROPERTIES);

        DEFAULT_RESOURCE_PROPS = new Properties();

        try {
            DEFAULT_RESOURCE_PROPS.load(is);

            LOG.info("Load default configuration ok");

            return DEFAULT_RESOURCE_PROPS;
        } catch (Exception e) {
            LOG.info("Load default configuration fail");
            LOG.error("{}", e);

            return null;
        } finally {
            try {
                if (is != null) {
                    is.close();
                }
            } catch (Exception e) {
                LOG.error("{}", e);
            }
        }
    }


    private static Properties getSpecificProperties() {
        String resourceFile = SERVER_PROPS.getProperty(KEY_RESOURCE_FILE);

        if (resourceFile != null) {
            Path path = Paths.get(resourceFile);
            if (path == null) {
                LOG.info("Use default configuration");

                return DEFAULT_RESOURCE_PROPS;
            }

            String fileName = path.getFileName().toString();
            Properties props = getProperties(fileName);

            if (props != null) {
                LOG.info("Load {} configuration ok", fileName);

                return props;
            } else {
                LOG.info("Not found {}, use default configuration", fileName);

                return DEFAULT_RESOURCE_PROPS;
            }
        } else {
            LOG.info("No specific resource, use default configuration");

            return DEFAULT_RESOURCE_PROPS;
        }
    }

    private static Properties getProperties(String resource) {
        LOG.debug("getProperties:{}", resource);
        FileInputStream fis = null;

        try {
            URL url = ClassLoader.getSystemClassLoader().getResource(".");

            // Use for Maven/Use for IntelliJ IDEA & Eclipse
            String path = url == null? CONF_PATH + resource
                    : url.getPath().replace("target/classes/", "") + CONF_PATH + resource;
            LOG.debug("getProperties:{}", path);

            fis = new FileInputStream(path);
            Properties props = new Properties();

            props.load(fis);

            return props;
        } catch (Exception e) {
            LOG.error("{}", e);
        } finally {
            if (fis != null) {
                try {
                    fis.close();
                } catch (IOException e) {
                    LOG.error("{}", e);
                }
            }
        }

        return null;
    }

    private static String getProperty(String key) {
        // SERVER_PROPS > RESOURCE_PROPS > DEFAULT_RESOURCE_PROPS

        return getProperty(key, "");
    }

    private static String getProperty(String key, String defaultValue) {
        // SERVER_PROPS > RESOURCE_PROPS > DEFAULT_RESOURCE_PROPS

        if (SERVER_PROPS != null) {
            String cliProperty = SERVER_PROPS.getProperty(key);

            if (cliProperty!=null && !cliProperty.trim().isEmpty()) {
                LOG.debug("SERVER_PROPS[{}]:{}", key, cliProperty);
                return cliProperty.trim();
            }
        }

        if (RESOURCE_PROPS != null) {
            String fileProperty = RESOURCE_PROPS.getProperty(key);

            if (fileProperty!=null && !fileProperty.trim().isEmpty()) {
                LOG.debug("RESOURCE_PROPS[{}]:{}", key, fileProperty);
                return fileProperty.trim();
            }
        }

        if (DEFAULT_RESOURCE_PROPS != null) {
            String defaultFileProperty = DEFAULT_RESOURCE_PROPS.getProperty(key);

            if (defaultFileProperty != null && !defaultFileProperty.trim().isEmpty()) {
                LOG.debug("DEFAULT_RESOURCE_PROPS[{}]:{}", key, defaultFileProperty);
                return defaultFileProperty.trim();
            }
        }

        return defaultValue;
    }

    /**
     * Retrieve zk sessionTimeout
     *
     * @return
     */
    public static int getSessionTimeout() {
        return Integer.valueOf(getProperty("timeout", "15000"));
    }

    /**
     * Retrieve zk host
     *
     * @return
     */
    public static String getZKHost() {
        return getProperty("zkHost");
    }


    /**
     * Retrieve com.bebe.zookeeper.cluster name
     *
     * @return
     */
    public static String getClusterName() {
        return getProperty("cluster.name");
    }

    /**
     * Retrieve processor command
     *
     * @return
     */
    public static String getCommand() {
        return getProperty("process.command");
    }

    /**
     * Retrieve limit node size
     *
     * @return
     */
    public static int getMaximunProcessors() {
        return Integer.valueOf(getProperty("processors.maximum", "1"));
    }

    /**
     * Retrieve node name
     *
     * @return
     */
    public static String getAgentName() {

        try {
            return getProperty("agent.name", InetAddress.getLocalHost().getHostName());
        }catch (UnknownHostException e){
            LOG.error("\t=== fail to retrieve hostname:{} ===", e);
            return null;
        }
    }

    /**
     * Retrieve buffer time for stop com.bebe.zookeeper.process
     *
     * @return
     */
    public static long getStopBufferTime() {
        return Long.valueOf(getProperty("buffer", "3000"));
    }

    /**
     * Retrieve buffer time for stop com.bebe.zookeeper.process
     *
     * @return
     */
    public static int getRetries() {
        return Integer.valueOf(getProperty("agent.retries", "3"));
    }

}

/**
 * 
 */
package mil.nga.giat.geowave.cli.geoserver.constants;

/**
 * GeoServer connection constants
 *
 */
public interface GeoServerConstants
{
	public static final String GEOSERVER_NAMESPACE_PREFIX = "geoserver";
	public static final String GEOSERVER_URL = GEOSERVER_NAMESPACE_PREFIX + ".url";
	public static final String GEOSERVER_USER = GEOSERVER_NAMESPACE_PREFIX + ".user";
	public static final String GEOSERVER_PASS = GEOSERVER_NAMESPACE_PREFIX + ".pass";
	public static final String GEOSERVER_WORKSPACE = GEOSERVER_NAMESPACE_PREFIX + ".workspace";
	public static final String GEOSERVER_CS = GEOSERVER_NAMESPACE_PREFIX + ".coverageStore";
	public static final String GEOSERVER_DS = GEOSERVER_NAMESPACE_PREFIX + ".dataStore";

	public static final String GEOSERVER_SSL_SECURITY_PROTOCOL = GEOSERVER_NAMESPACE_PREFIX + ".ssl.security.protocol";

	public static final String GEOSERVER_SSL_TRUSTSTORE_FILE = GEOSERVER_NAMESPACE_PREFIX + ".ssl.trustStore";
	public static final String GEOSERVER_SSL_TRUSTSTORE_PASS = GEOSERVER_NAMESPACE_PREFIX + ".ssl.trustStorePassword";
	public static final String GEOSERVER_SSL_TRUSTSTORE_TYPE = GEOSERVER_NAMESPACE_PREFIX + ".ssl.trustStoreType";
	public static final String GEOSERVER_SSL_TRUSTSTORE_PROVIDER = GEOSERVER_NAMESPACE_PREFIX
			+ ".ssl.trustStoreProvider";
	public static final String GEOSERVER_SSL_TRUSTMGR_ALG = GEOSERVER_NAMESPACE_PREFIX
			+ ".ssl.trustStoreMgrFactoryAlgorithm";
	public static final String GEOSERVER_SSL_TRUSTMGR_PROVIDER = GEOSERVER_NAMESPACE_PREFIX
			+ ".ssl.trustStoreMgrFactoryProvider";

	public static final String GEOSERVER_SSL_KEYSTORE_FILE = GEOSERVER_NAMESPACE_PREFIX + ".ssl.keyStore";
	public static final String GEOSERVER_SSL_KEYSTORE_PASS = GEOSERVER_NAMESPACE_PREFIX + ".ssl.keyStorePassword";
	public static final String GEOSERVER_SSL_KEYSTORE_PROVIDER = GEOSERVER_NAMESPACE_PREFIX + ".ssl.keyStoreProvider";
	public static final String GEOSERVER_SSL_KEY_PASS = GEOSERVER_NAMESPACE_PREFIX + ".ssl.keyPassword";
	public static final String GEOSERVER_SSL_KEYSTORE_TYPE = GEOSERVER_NAMESPACE_PREFIX + ".ssl.keyStoreType";
	public static final String GEOSERVER_SSL_KEYMGR_ALG = GEOSERVER_NAMESPACE_PREFIX + ".ssl.keyMgrFactoryAlgorithm";
	public static final String GEOSERVER_SSL_KEYMGR_PROVIDER = GEOSERVER_NAMESPACE_PREFIX
			+ ".ssl.keyMgrFactoryProvider";
}

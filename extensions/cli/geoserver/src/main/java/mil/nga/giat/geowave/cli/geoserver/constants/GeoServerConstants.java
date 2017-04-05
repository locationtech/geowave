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

	public static final String GEOSERVER_URL = "geoserver.url";
	public static final String GEOSERVER_USER = "geoserver.user";
	public static final String GEOSERVER_PASS = "geoserver.pass";
	public static final String GEOSERVER_WORKSPACE = "geoserver.workspace";
	public static final String GEOSERVER_CS = "geoserver.coverageStore";
	public static final String GEOSERVER_DS = "geoserver.dataStore";

	public static final String GEOSERVER_SSL_SECURITY_PROTOCOL = "geoserver.ssl.security.protocol";

	public static final String GEOSERVER_SSL_TRUSTSTORE_FILE = "geoserver.ssl.trustStore";
	public static final String GEOSERVER_SSL_TRUSTSTORE_PASS = "geoserver.ssl.trustStorePassword";
	public static final String GEOSERVER_SSL_TRUSTSTORE_TYPE = "geoserver.ssl.trustStoreType";
	public static final String GEOSERVER_SSL_TRUSTSTORE_PROVIDER = "geoserver.ssl.trustStoreProvider";
	public static final String GEOSERVER_SSL_TRUSTMGR_ALG = "geoserver.ssl.trustStoreMgrFactoryAlgorithm";
	public static final String GEOSERVER_SSL_TRUSTMGR_PROVIDER = "geoserver.ssl.trustStoreMgrFactoryProvider";

	public static final String GEOSERVER_SSL_KEYSTORE_FILE = "geoserver.ssl.keyStore";
	public static final String GEOSERVER_SSL_KEYSTORE_PASS = "geoserver.ssl.keyStorePassword";
	public static final String GEOSERVER_SSL_KEYSTORE_PROVIDER = "geoserver.ssl.keyStoreProvider";
	public static final String GEOSERVER_SSL_KEY_PASS = "geoserver.ssl.keyPassword";
	public static final String GEOSERVER_SSL_KEYSTORE_TYPE = "geoserver.ssl.keyStoreType";
	public static final String GEOSERVER_SSL_KEYMGR_ALG = "geoserver.ssl.keyMgrFactoryAlgorithm";
	public static final String GEOSERVER_SSL_KEYMGR_PROVIDER = "geoserver.ssl.keyMgrFactoryProvider";
}

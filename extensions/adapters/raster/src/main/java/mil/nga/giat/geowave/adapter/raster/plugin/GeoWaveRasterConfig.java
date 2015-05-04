package mil.nga.giat.geowave.adapter.raster.plugin;

import java.io.InputStream;
import java.net.URL;
import java.util.Hashtable;
import java.util.Map;

import javax.media.jai.Interpolation;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

public class GeoWaveRasterConfig
{

	static private final Map<String, GeoWaveRasterConfig> CONFIG_CACHE = new Hashtable<String, GeoWaveRasterConfig>();

	protected static enum ConfigParameter {
		ZOOKEEPER_URLS(
				"zookeeperUrls"),
		INSTANCE_ID(
				"instanceId"),
		USERNAME(
				"username"),
		PASSWORD(
				"password"),
		NAMESPACE(
				"namespace"),
		// the following two are optional parameters that will override the
		// behavior of tile mosaicing that is already set within each adapter
		INTERPOLATION(
				"interpolationOverride"),
		EQUALIZE_HISTOGRAM(
				"equalizeHistogramOverride");
		private String configName;

		private ConfigParameter(
				final String configName ) {
			this.configName = configName;
		}

		public String getConfigName() {
			return configName;
		}
	}

	private String xmlUrl;

	private String zookeeperUrls;

	private String accumuloInstanceId;

	private String accumuloUsername;

	private String accumuloPassword;

	private String geowaveNamespace;

	private Boolean equalizeHistogramOverride = null;

	private Integer interpolationOverride = null;

	protected GeoWaveRasterConfig() {}

	public static GeoWaveRasterConfig createConfig(
			final String zookeeperUrl,
			final String accumuloInstanceId,
			final String accumuloUsername,
			final String accumuloPassword,
			final String geowaveNamespace ) {
		return createConfig(
				zookeeperUrl,
				accumuloInstanceId,
				accumuloUsername,
				accumuloPassword,
				geowaveNamespace,
				null,
				null);
	}

	public static GeoWaveRasterConfig createConfig(
			final String zookeeperUrl,
			final String accumuloInstanceId,
			final String accumuloUsername,
			final String accumuloPassword,
			final String geowaveNamespace,
			final Boolean equalizeHistogramOverride,
			final Integer interpolationOverride ) {
		final GeoWaveRasterConfig result = new GeoWaveRasterConfig();
		result.zookeeperUrls = zookeeperUrl;
		result.accumuloInstanceId = accumuloInstanceId;
		result.accumuloUsername = accumuloUsername;
		result.accumuloPassword = accumuloPassword;
		result.geowaveNamespace = geowaveNamespace;
		result.equalizeHistogramOverride = equalizeHistogramOverride;
		result.interpolationOverride = interpolationOverride;
		return result;
	}

	public static GeoWaveRasterConfig readFrom(
			final URL xmlURL )
			throws Exception {
		GeoWaveRasterConfig result = CONFIG_CACHE.get(xmlURL.toString());

		if (result != null) {
			return result;
		}

		final InputStream in = xmlURL.openStream();
		final InputSource input = new InputSource(
				xmlURL.toString());

		final DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
		dbf.setIgnoringElementContentWhitespace(true);
		dbf.setIgnoringComments(true);

		final DocumentBuilder db = dbf.newDocumentBuilder();

		// db.setEntityResolver(new ConfigEntityResolver(xmlURL));
		final Document dom = db.parse(input);
		in.close();

		result = new GeoWaveRasterConfig();

		result.xmlUrl = xmlURL.toString();

		result.zookeeperUrls = readValueString(
				dom,
				ConfigParameter.ZOOKEEPER_URLS.getConfigName());
		result.accumuloInstanceId = readValueString(
				dom,
				ConfigParameter.INSTANCE_ID.getConfigName());
		result.accumuloUsername = readValueString(
				dom,
				ConfigParameter.USERNAME.getConfigName());
		result.accumuloPassword = readValueString(
				dom,
				ConfigParameter.PASSWORD.getConfigName());
		result.geowaveNamespace = readValueString(
				dom,
				ConfigParameter.NAMESPACE.getConfigName());
		final String equalizeHistogram = readValueString(
				dom,
				ConfigParameter.EQUALIZE_HISTOGRAM.getConfigName());
		if (equalizeHistogram != null) {
			if (equalizeHistogram.trim().toLowerCase().equals(
					"true")) {
				result.equalizeHistogramOverride = true;
			}
			else {
				result.equalizeHistogramOverride = false;
			}
		}
		result.interpolationOverride = readValueInteger(
				dom,
				ConfigParameter.INTERPOLATION.getConfigName());
		CONFIG_CACHE.put(
				xmlURL.toString(),
				result);

		return result;
	}

	public String getXmlUrl() {
		return xmlUrl;
	}

	public String getZookeeperUrls() {
		return zookeeperUrls;
	}

	public String getAccumuloInstanceId() {
		return accumuloInstanceId;
	}

	public String getAccumuloUsername() {
		return accumuloUsername;
	}

	public String getAccumuloPassword() {
		return accumuloPassword;
	}

	public String getGeowaveNamespace() {
		return geowaveNamespace;
	}

	public boolean isInterpolationOverrideSet() {
		return (interpolationOverride != null);
	}

	public Interpolation getInterpolationOverride() {
		if (!isInterpolationOverrideSet()) {
			throw new IllegalStateException(
					"Interpolation Override is not set for this config");
		}

		return Interpolation.getInstance(interpolationOverride);
	}

	public boolean isEqualizeHistogramOverrideSet() {
		return (equalizeHistogramOverride != null);
	}

	public boolean isEqualizeHistogramOverride() {
		if (!isEqualizeHistogramOverrideSet()) {
			throw new IllegalStateException(
					"Equalize Histogram is not set for this config");
		}
		return equalizeHistogramOverride;
	}

	static private String readValueString(
			final Document dom,
			final String elemName ) {
		final Node n = getNodeByName(
				dom,
				elemName);

		if (n == null) {
			return null;
		}

		return n.getTextContent();
	}

	static private Integer readValueInteger(
			final Document dom,
			final String elemName ) {
		final Node n = getNodeByName(
				dom,
				elemName);

		if (n == null) {
			return null;
		}

		return Integer.valueOf(n.getTextContent());
	}

	static private Node getNodeByName(
			final Document dom,
			final String elemName ) {
		final NodeList list = dom.getElementsByTagName(elemName);
		final Node n = list.item(0);

		return n;
	}
}

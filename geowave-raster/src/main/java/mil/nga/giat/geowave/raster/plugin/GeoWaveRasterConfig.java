package mil.nga.giat.geowave.raster.plugin;

import java.io.InputStream;
import java.net.URL;
import java.util.Hashtable;
import java.util.Map;

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
		INTERPOLATION(
				"interpolation"),
		EQUALIZE_HISTOGRAM(
				"equalizeHistogram");
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

	private boolean equalizeHistogram;

	private Integer interpolation;

	protected GeoWaveRasterConfig() {}

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
			result.equalizeHistogram = equalizeHistogram.trim().toLowerCase().equals(
					"true");
		}
		else {
			result.equalizeHistogram = false;
		}
		result.interpolation = readValueInteger(
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

	public Integer getInterpolation() {
		return interpolation;
	}

	public boolean isEqualizeHistogram() {
		return equalizeHistogram;
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

		return new Integer(
				n.getTextContent());
	}

	static private Node getNodeByName(
			final Document dom,
			final String elemName ) {
		final NodeList list = dom.getElementsByTagName(elemName);
		final Node n = list.item(0);

		return n;
	}
}

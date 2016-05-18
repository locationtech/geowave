package mil.nga.giat.geowave.core.store;

import com.beust.jcommander.Parameter;

/**
 * This interface doesn't actually do anything, is just used for tracking during
 * development.
 */
public class StoreFactoryOptions
{

	public final static String GEOWAVE_NAMESPACE_OPTION = "gwNamespace";

	@Parameter(names = "--" + GEOWAVE_NAMESPACE_OPTION, description = "The geowave namespace (optional; default is no namespace)")
	private String geowaveNamespace;

	public String getGeowaveNamespace() {
		return geowaveNamespace;
	}

	public void setGeowaveNamespace(
			String geowaveNamespace ) {
		this.geowaveNamespace = geowaveNamespace;
	}

}

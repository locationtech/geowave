package mil.nga.giat.geowave.vector.plugin;

import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;

import org.apache.log4j.Logger;
import org.geotools.data.ResourceInfo;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

/**
 * An informational container to self-describe a GeoWave source.
 * 
 */
public class GeoWaveResourceInfo implements
		ResourceInfo
{
	/** Package logger */
	private final static Logger LOGGER = Logger.getLogger(GeoWaveResourceInfo.class);
	private final GeoWaveFeatureSource myFS;
	private final URI myURI;

	public GeoWaveResourceInfo(
			final GeoWaveFeatureSource fs ) {
		myFS = fs;
		final String namespaceURI = myFS.getFeatureType().getName().getNamespaceURI();
		if (namespaceURI != null) {
			myURI = URI.create(myFS.getFeatureType().getName().getNamespaceURI());
		}
		else {
			// this is the old way...but ContentDataStore, provided by geotools
			// sets to null.
			// which is correct?
			myURI = URI.create(myFS.getFeatureType().getName().getURI());
		}
	}

	@Override
	public CoordinateReferenceSystem getCRS() {
		return myFS.getCRS();
	}

	@Override
	public ReferencedEnvelope getBounds() {
		try {
			return myFS.getBounds();
		}
		catch (final IOException e) {
			LOGGER.warn(
					"Error getting bounds",
					e);
		}
		return null;
	}

	@Override
	public URI getSchema() {
		return myURI;
	}

	@Override
	public String getName() {
		return myFS.getName().getLocalPart();
	}

	@Override
	public String getDescription() {
		return "GeoWave Resource";
	}

	@Override
	public Set<String> getKeywords() {
		final Set<String> keywords = new HashSet<String>();
		keywords.add(getName());
		return keywords;
	}

	@Override
	public String getTitle() {
		return getName();
	}
}

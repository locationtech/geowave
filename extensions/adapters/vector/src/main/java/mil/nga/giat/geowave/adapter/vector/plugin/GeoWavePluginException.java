package mil.nga.giat.geowave.adapter.vector.plugin;

/**
 * A basic, general exception thrown within the GeoWave plugin to GeoTools.
 * 
 */
public class GeoWavePluginException extends
		Exception
{

	private static final long serialVersionUID = -8043877412333078281L;

	public GeoWavePluginException(
			final String msg ) {
		super(
				msg);
	}

}

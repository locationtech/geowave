package mil.nga.giat.geowave.cli.geoserver;

public abstract class GeoServerRemoveCommand<T> extends
		GeoServerCommand<T>
{

	/**
	 * Return "200 OK" for all remove commands.
	 */
	@Override
	public Boolean successStatusIs200() {
		return true;
	}
}

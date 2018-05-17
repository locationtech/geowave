package mil.nga.giat.geowave.cli.geoserver;

import org.restlet.data.Status;

import mil.nga.giat.geowave.core.cli.api.OperationParams;

public abstract class GeoServerRemoveCommand<T> extends
		GeoServerCommand<T>
{

	/**
	 * Return "200 OK" for all remove commands.
	 */
	@Override
	public Status getSuccessStatus() {
		return Status.SUCCESS_OK;
	}
}

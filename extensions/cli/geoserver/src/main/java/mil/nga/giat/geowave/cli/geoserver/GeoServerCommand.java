package mil.nga.giat.geowave.cli.geoserver;

import javax.ws.rs.NotAuthorizedException;
import javax.ws.rs.core.Response;

import org.apache.spark.status.api.v1.ForbiddenException;

import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.cli.api.ServiceEnabledCommand;
import mil.nga.giat.geowave.core.cli.exceptions.DuplicateEntryException;
import mil.nga.giat.geowave.core.cli.exceptions.TargetNotFoundException;

abstract public class GeoServerCommand<T> extends
		ServiceEnabledCommand<T>
{

	protected GeoServerRestClient geoserverClient = null;

	@Override
	public boolean prepare(
			final OperationParams params ) {
		if (geoserverClient == null) {
			// Create the rest client
			geoserverClient = GeoServerRestClient.getInstance(new GeoServerConfig(
					getGeoWaveConfigFile(params)));
		}

		// Successfully prepared
		return true;
	}

	public boolean isDuplicate(
			Response response,
			String errorMessage )
			throws TargetNotFoundException {
		if (errorMessage.toLowerCase().contains(
				"already exists")) {
			return true;
		}
		return false;
	}

	public T handleError(
			Response response,
			String errorMessage )
			throws Exception {
		if (isDuplicate(
				response,
				errorMessage)) {
			throw new DuplicateEntryException(
					errorMessage);
		}
		switch (response.getStatus()) {
			case 401:
				throw new NotAuthorizedException(
						errorMessage);
			case 403:
				throw new ForbiddenException(
						errorMessage);
			case 404:
				throw new TargetNotFoundException(
						errorMessage);
				// GeoServer responses for 500 codes are poorly formatted so
				// don't return that response
			case 500:
				throw new Exception(
						"Internal Server Error\n GeoServer Response Code = 500");
			default:
				throw new Exception(
						errorMessage);
		}
	}

}

package mil.nga.giat.geowave.service.rest;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;

import org.restlet.ext.jackson.JacksonRepresentation;
import org.restlet.representation.Representation;
import org.restlet.resource.Get;
import org.restlet.resource.ServerResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.service.rest.operations.RestOperationStatusMessage;

/**
 * ServerResource that returns the status of async REST operations submitted to
 * the server
 */
public class AsyncOperationStatusResource extends
		ServerResource
{
	private static final Logger LOGGER = LoggerFactory.getLogger(AsyncOperationStatusResource.class);

	@Get("json")
	public Representation getStatus(
			final Representation request ) {

		RestOperationStatusMessage status = new RestOperationStatusMessage();
		ConcurrentHashMap<String, Future<?>> opStatuses = null;
		String id = getQueryValue("id");
		try {
			// look up the operation status
			opStatuses = (ConcurrentHashMap<String, Future<?>>) this.getApplication().getContext().getAttributes().get(
					"asyncOperationStatuses");
			if (opStatuses.get(id) != null) {
				Future<?> future = opStatuses.get(id);

				if (future.isDone()) {
					status.status = RestOperationStatusMessage.StatusType.COMPLETE;
					status.message = "operation success";
					status.data = future.get();
					opStatuses.remove(id);
				}
				else {
					status.status = RestOperationStatusMessage.StatusType.RUNNING;
				}
				return new JacksonRepresentation<RestOperationStatusMessage>(
						status);
			}
		}
		catch (final Exception e) {
			LOGGER.error(
					"Error exception: ",
					e.getMessage());
			status.status = RestOperationStatusMessage.StatusType.ERROR;
			status.message = "exception occurred";
			status.data = e;
			if (opStatuses != null) opStatuses.remove(id);
			return new JacksonRepresentation<RestOperationStatusMessage>(
					status);
		}
		status.status = RestOperationStatusMessage.StatusType.ERROR;
		status.message = "no operation found for ID: " + id;
		return new JacksonRepresentation<RestOperationStatusMessage>(
				status);
	}
}

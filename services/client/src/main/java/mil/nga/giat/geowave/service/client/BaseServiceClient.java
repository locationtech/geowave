package mil.nga.giat.geowave.service.client;

import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.Response;

import org.glassfish.jersey.client.proxy.WebResourceFactory;
import org.glassfish.jersey.media.multipart.MultiPartFeature;

import mil.nga.giat.geowave.service.BaseService;

public class BaseServiceClient
{

	private final BaseService baseService;

	public BaseServiceClient(
			final String baseUrl ) {
		this(
				baseUrl,
				null,
				null);
	}

	public BaseServiceClient(
			final String baseUrl,
			String user,
			String password ) {

		baseService = WebResourceFactory.newResource(
				BaseService.class,
				ClientBuilder.newClient().register(
						MultiPartFeature.class).target(
						baseUrl));
	}

	public Response operation_status(
			final String id ) {

		final Response resp = baseService.operation_status(id);
		return resp;

	}
}

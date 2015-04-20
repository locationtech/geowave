package mil.nga.giat.geowave.service.client;

import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.Response;

import mil.nga.giat.geowave.service.InfoService;
import net.sf.json.JSONObject;

import org.glassfish.jersey.client.proxy.WebResourceFactory;

public class InfoServiceClient
{

	private final InfoService infoService;

	public InfoServiceClient(
			final String baseUrl ) {
		infoService = WebResourceFactory.newResource(
				InfoService.class,
				ClientBuilder.newClient().target(
						baseUrl));
	}

	public JSONObject getNamespaces() {
		final Response resp = infoService.getNamespaces();
		resp.bufferEntity();
		return JSONObject.fromObject(resp.readEntity(String.class));
	}

	public JSONObject getIndices(
			final String namespace ) {
		final Response resp = infoService.getIndices(namespace);
		resp.bufferEntity();
		return JSONObject.fromObject(resp.readEntity(String.class));
	}

	public JSONObject getAdapters(
			final String namespace ) {
		final Response resp = infoService.getAdapters(namespace);
		resp.bufferEntity();
		return JSONObject.fromObject(resp.readEntity(String.class));
	}
}

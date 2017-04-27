package mil.nga.giat.geowave.cli.geoserver;

import mil.nga.giat.geowave.cli.geoserver.GeoServerConfig;
import mil.nga.giat.geowave.cli.geoserver.GeoServerRestClient;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;

public class GeoServerRestClientTest
{
	WebTarget webTarget;
	GeoServerConfig config;
	GeoServerRestClient client;

	private WebTarget mockedWebTarget() {
		WebTarget webTarget = Mockito.mock(WebTarget.class);
		Invocation.Builder invBuilder = Mockito.mock(Invocation.Builder.class);
		Response response = Mockito.mock(Response.class);

		Mockito.when(
				webTarget.path(Mockito.anyString())).thenReturn(
				webTarget);
		Mockito.when(
				webTarget.request()).thenReturn(
				invBuilder);

		Mockito.when(
				invBuilder.get()).thenReturn(
				response);
		Mockito.when(
				invBuilder.delete()).thenReturn(
				response);
		Mockito.when(
				invBuilder.post(Mockito.any(Entity.class))).thenReturn(
				response);

		return webTarget;

	}

	@Before
	public void prepare() {
		webTarget = mockedWebTarget();
		config = new GeoServerConfig();
		client = new GeoServerRestClient(
				config,
				webTarget);
	}

	@Test
	public void testGetFeatureLayer() {
		client.getFeatureLayer("some_layer");
		Mockito.verify(
				webTarget).path(
				"rest/layers/some_layer.json");
	}

	@Test
	public void testGetConfig() {
		GeoServerConfig returnedConfig = client.getConfig();
		Assert.assertEquals(
				config,
				returnedConfig);
	}

	@Test
	public void testGetCoverage() {
		client.getCoverage(
				"some_workspace",
				"some_cvgStore",
				"some_coverage");
		Mockito.verify(
				webTarget).path(
				"rest/workspaces/some_workspace/coveragestores/some_cvgStore/coverages/some_coverage.json");
	}

	@Test
	public void testGetCoverageStores() {
		client.getCoverageStores("some_workspace");
		Mockito.verify(
				webTarget).path(
				"rest/workspaces/some_workspace/coveragestores.json");
	}

	@Test
	public void testGetCoverages() {
		client.getCoverages(
				"some_workspace",
				"some_cvgStore");
		Mockito.verify(
				webTarget).path(
				"rest/workspaces/some_workspace/coveragestores/some_cvgStore/coverages.json");
	}

	@Test
	public void testGetDatastore() {
		client.getDatastore(
				"some_workspace",
				"some_datastore");
		Mockito.verify(
				webTarget).path(
				"rest/workspaces/some_workspace/datastores/some_datastore.json");
	}

	@Test
	public void testGetStyle() {
		client.getStyle("some_style");
		Mockito.verify(
				webTarget).path(
				"rest/styles/some_style.sld");
	}

	@Test
	public void testGetStyles() {
		client.getStyles();
		Mockito.verify(
				webTarget).path(
				"rest/styles.json");
	}

	@Test
	public void testGetWorkspaces() {
		client.getWorkspaces();
		Mockito.verify(
				webTarget).path(
				"rest/workspaces.json");
	}

	@Test
	public void addFeatureLayer() {
		client.addFeatureLayer(
				"some_workspace",
				"some_datastore",
				"some_layer",
				"some_style");
		Mockito.verify(
				webTarget).path(
				"rest/layers/some_layer.json");
	}

	@Test
	public void addCoverage() {
		client.addCoverage(
				"some_workspace",
				"some_cvgStore",
				"some_coverage");
		Mockito.verify(
				webTarget).path(
				"rest/workspaces/some_workspace/coveragestores/some_cvgStore/coverages");
	}

	@Test
	public void addWorkspace() {
		client.addWorkspace("some_workspace");
		Mockito.verify(
				webTarget).path(
				"rest/workspaces");
	}
}

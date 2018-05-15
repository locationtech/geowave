package mil.nga.giat.geowave.service.rest;

import static org.junit.Assert.*;

import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.math3.util.Pair;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.restlet.Response;
import org.restlet.data.Status;
import org.restlet.representation.Representation;

import mil.nga.giat.geowave.core.cli.api.ServiceEnabledCommand;
import mil.nga.giat.geowave.core.cli.api.ServiceEnabledCommand.HttpMethod;
import mil.nga.giat.geowave.core.cli.api.ServiceStatus;

public class GeoWaveOperationServiceWrapperTest
{

	private ServiceEnabledCommand mockedOperation(
			HttpMethod method )
			throws Exception {
		return mockedOperation(
				method,
				false);
	}

	private ServiceEnabledCommand mockedOperation(
			HttpMethod method,
			boolean isAsync )
			throws Exception {
		ServiceEnabledCommand operation = Mockito.mock(ServiceEnabledCommand.class);

		Mockito.when(
				operation.getMethod()).thenReturn(
				method);
		Mockito.when(
				operation.executeService(Mockito.any())).thenReturn(
				ImmutablePair.of(
						ServiceStatus.OK,
						null));
		Mockito.when(
				operation.runAsync()).thenReturn(
				isAsync);

		return operation;
	}

	@Before
	public void setUp()
			throws Exception {}

	@After
	public void tearDown()
			throws Exception {}

	@Test
	public void getReturns200()
			throws Exception {

		ServiceEnabledCommand operation = mockedOperation(HttpMethod.GET);

		GeoWaveOperationServiceWrapper gwOpServWrap = new GeoWaveOperationServiceWrapper(
				operation,
				null);
		gwOpServWrap.setResponse(new Response(
				null));
		gwOpServWrap.restGet();
		Assert.assertEquals(
				gwOpServWrap.getResponse().getStatus(),
				Status.SUCCESS_OK);
	}

	@Test
	public void postReturns201()
			throws Exception {

		ServiceEnabledCommand operation = mockedOperation(HttpMethod.POST);

		GeoWaveOperationServiceWrapper gwOpServWrap = new GeoWaveOperationServiceWrapper(
				operation,
				null);
		gwOpServWrap.setResponse(new Response(
				null));
		gwOpServWrap.restPost(null);
		Assert.assertEquals(
				gwOpServWrap.getResponse().getStatus(),
				Status.SUCCESS_CREATED);
	}

	@Test
	public void asyncGetReturns200()
			throws Exception {

		ServiceEnabledCommand operation = mockedOperation(
				HttpMethod.GET,
				true);

		GeoWaveOperationServiceWrapper gwOpServWrap = new GeoWaveOperationServiceWrapper(
				operation,
				null);
		gwOpServWrap.setResponse(new Response(
				null));
		gwOpServWrap.restGet();
		Assert.assertEquals(
				gwOpServWrap.getResponse().getStatus(),
				Status.SUCCESS_OK);
	}

	@Test
	public void asyncPostReturns201()
			throws Exception {

		ServiceEnabledCommand operation = mockedOperation(
				HttpMethod.POST,
				false);

		GeoWaveOperationServiceWrapper gwOpServWrap = new GeoWaveOperationServiceWrapper(
				operation,
				null);
		gwOpServWrap.setResponse(new Response(
				null));
		gwOpServWrap.restPost(null);
		Assert.assertEquals(
				gwOpServWrap.getResponse().getStatus(),
				Status.SUCCESS_CREATED);
	}

}

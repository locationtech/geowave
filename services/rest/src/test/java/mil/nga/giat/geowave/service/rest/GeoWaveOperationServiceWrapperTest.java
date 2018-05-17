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
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;
import org.restlet.Application;
import org.restlet.Context;
import org.restlet.Response;
import org.restlet.data.Status;
import org.restlet.representation.Representation;

import mil.nga.giat.geowave.core.cli.api.ServiceEnabledCommand;
import mil.nga.giat.geowave.core.cli.api.ServiceEnabledCommand.HttpMethod;
import mil.nga.giat.geowave.core.cli.api.ServiceStatus;

public class GeoWaveOperationServiceWrapperTest
{

	private ServiceEnabledCommand mockedOperation(
			HttpMethod method,
			Status successStatus )
			throws Exception {
		return mockedOperation(
				method,
				successStatus,
				false);
	}

	private ServiceEnabledCommand mockedOperation(
			HttpMethod method,
			Status successStatus,
			boolean isAsync )
			throws Exception {
		ServiceEnabledCommand operation = Mockito.mock(ServiceEnabledCommand.class);

		Mockito.when(
				operation.getMethod()).thenReturn(
				method);
		Mockito.when(
				operation.runAsync()).thenReturn(
				isAsync);
		Mockito.when(
				operation.getSuccessStatus()).thenReturn(
				successStatus);

		return operation;
	}

	@Before
	public void setUp()
			throws Exception {}

	@After
	public void tearDown()
			throws Exception {}

	@Test
	public void getMethodReturnsSuccessStatus()
			throws Exception {

		// Rarely used Teapot Code to check.
		Status expectedSuccessStatus = new Status(
				418);

		ServiceEnabledCommand operation = mockedOperation(
				HttpMethod.GET,
				expectedSuccessStatus);

		GeoWaveOperationServiceWrapper gwOpServWrap = new GeoWaveOperationServiceWrapper(
				operation,
				null);
		gwOpServWrap.setResponse(new Response(
				null));
		gwOpServWrap.restGet();
		Assert.assertEquals(
				expectedSuccessStatus,
				gwOpServWrap.getResponse().getStatus());
	}

	@Test
	public void postMethodReturnsSuccessStatus()
			throws Exception {

		// Rarely used Teapot Code to check.
		Status expectedSuccessStatus = new Status(
				418);

		ServiceEnabledCommand operation = mockedOperation(
				HttpMethod.POST,
				expectedSuccessStatus);

		GeoWaveOperationServiceWrapper gwOpServWrap = new GeoWaveOperationServiceWrapper(
				operation,
				null);
		gwOpServWrap.setResponse(new Response(
				null));
		gwOpServWrap.restPost(null);
		Assert.assertEquals(
				expectedSuccessStatus,
				gwOpServWrap.getResponse().getStatus());
	}

	@Test
	@Ignore
	public void asyncMethodReturnsSuccessStatus()
			throws Exception {

		// Rarely used Teapot Code to check.
		Status expectedSuccessStatus = new Status(
				418);

		ServiceEnabledCommand operation = mockedOperation(
				HttpMethod.POST,
				expectedSuccessStatus,
				true);

		GeoWaveOperationServiceWrapper gwOpServWrap = new GeoWaveOperationServiceWrapper(
				operation,
				null);
		gwOpServWrap.setResponse(new Response(
				null));
		gwOpServWrap.restPost(null);

		// TODO: Returns 500. Error Caught at
		// "final Context appContext = Application.getCurrent().getContext();"
		Assert.assertEquals(
				expectedSuccessStatus,
				gwOpServWrap.getResponse().getStatus());
	}

}

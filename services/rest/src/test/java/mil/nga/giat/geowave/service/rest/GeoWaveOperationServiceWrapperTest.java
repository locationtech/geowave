package mil.nga.giat.geowave.service.rest;

import java.io.IOException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;
import org.restlet.Response;
import org.restlet.data.MediaType;
import org.restlet.data.Status;
import org.restlet.representation.Representation;

import mil.nga.giat.geowave.core.cli.api.ServiceEnabledCommand;
import mil.nga.giat.geowave.core.cli.api.ServiceEnabledCommand.HttpMethod;

public class GeoWaveOperationServiceWrapperTest
{

	private GeoWaveOperationServiceWrapper classUnderTest;

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
		Mockito.when(
				operation.computeResults(Mockito.any())).thenReturn(
				null);

		return operation;
	}

	private Representation mockedRequest(
			MediaType mediaType )
			throws IOException {

		Representation request = Mockito.mock(Representation.class);

		Mockito.when(
				request.getMediaType()).thenReturn(
				mediaType);
		Mockito.when(
				request.getText()).thenReturn(
				"{}");

		return request;
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

		classUnderTest = new GeoWaveOperationServiceWrapper(
				operation,
				null);
		classUnderTest.setResponse(new Response(
				null));
		classUnderTest.restGet();
		Assert.assertEquals(
				expectedSuccessStatus,
				classUnderTest.getResponse().getStatus());
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

		classUnderTest = new GeoWaveOperationServiceWrapper(
				operation,
				null);
		classUnderTest.setResponse(new Response(
				null));
		classUnderTest.restPost(mockedRequest(MediaType.APPLICATION_JSON));
		Assert.assertEquals(
				expectedSuccessStatus,
				classUnderTest.getResponse().getStatus());
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

		classUnderTest = new GeoWaveOperationServiceWrapper(
				operation,
				null);
		classUnderTest.setResponse(new Response(
				null));
		classUnderTest.restPost(null);

		// TODO: Returns 500. Error Caught at
		// "final Context appContext = Application.getCurrent().getContext();"
		Assert.assertEquals(
				expectedSuccessStatus,
				classUnderTest.getResponse().getStatus());
	}

}

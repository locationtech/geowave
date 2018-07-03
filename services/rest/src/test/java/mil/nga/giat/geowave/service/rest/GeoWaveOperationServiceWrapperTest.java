package mil.nga.giat.geowave.service.rest;

import java.io.IOException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;
import org.restlet.Request;
import org.restlet.Response;
import org.restlet.data.MediaType;
import org.restlet.data.Method;
import org.restlet.data.Reference;
import org.restlet.data.Status;
import org.restlet.representation.Representation;

import mil.nga.giat.geowave.core.cli.api.ServiceEnabledCommand;
import mil.nga.giat.geowave.core.cli.api.ServiceEnabledCommand.HttpMethod;

public class GeoWaveOperationServiceWrapperTest
{

	private GeoWaveOperationServiceWrapper classUnderTest;

	private ServiceEnabledCommand mockedOperation(
			HttpMethod method,
			Boolean successStatusIs200 )
			throws Exception {
		return mockedOperation(
				method,
				successStatusIs200,
				false);
	}

	private ServiceEnabledCommand mockedOperation(
			HttpMethod method,
			Boolean successStatusIs200,
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
				operation.successStatusIs200()).thenReturn(
				successStatusIs200);
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
		Boolean successStatusIs200 = true;

		ServiceEnabledCommand operation = mockedOperation(
				HttpMethod.GET,
				successStatusIs200);

		classUnderTest = new GeoWaveOperationServiceWrapper(
				operation,
				null);
		classUnderTest.setResponse(new Response(
				null));
		classUnderTest.setRequest(new Request(
				Method.GET,
				"foo.bar"));
		classUnderTest.restGet();
		Assert.assertEquals(
				successStatusIs200,
				classUnderTest.getResponse().getStatus().equals(
						Status.SUCCESS_OK));
	}

	@Test
	public void postMethodReturnsSuccessStatus()
			throws Exception {

		// Rarely used Teapot Code to check.
		Boolean successStatusIs200 = false;

		ServiceEnabledCommand operation = mockedOperation(
				HttpMethod.POST,
				successStatusIs200);

		classUnderTest = new GeoWaveOperationServiceWrapper(
				operation,
				null);
		classUnderTest.setResponse(new Response(
				null));
		classUnderTest.restPost(mockedRequest(MediaType.APPLICATION_JSON));
		Assert.assertEquals(
				successStatusIs200,
				classUnderTest.getResponse().getStatus().equals(
						Status.SUCCESS_OK));
	}

	@Test
	@Ignore
	public void asyncMethodReturnsSuccessStatus()
			throws Exception {

		// Rarely used Teapot Code to check.
		Boolean successStatusIs200 = true;

		ServiceEnabledCommand operation = mockedOperation(
				HttpMethod.POST,
				successStatusIs200,
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
				successStatusIs200,
				classUnderTest.getResponse().getStatus().equals(
						Status.SUCCESS_OK));
	}

}

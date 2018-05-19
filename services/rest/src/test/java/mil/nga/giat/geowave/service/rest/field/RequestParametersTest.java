package mil.nga.giat.geowave.service.rest.field;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.restlet.data.MediaType;
import org.restlet.representation.Representation;

import mil.nga.giat.geowave.service.rest.exceptions.UnsupportedMediaTypeException;

public class RequestParametersTest
{

	private RequestParameters classUnderTest;

	private JSONObject testJSON;

	private Representation mockedJsonRequest(
			String jsonString )
			throws IOException {
		Representation request = mockedRequest(MediaType.APPLICATION_JSON);

		Mockito.when(
				request.getText()).thenReturn(
				jsonString);

		return request;
	}

	private Representation mockedFormRequest() {
		return mockedRequest(MediaType.APPLICATION_WWW_FORM);
	}

	private Representation mockedRequest(
			MediaType mediaType ) {

		Representation request = Mockito.mock(Representation.class);

		Mockito.when(
				request.getMediaType()).thenReturn(
				mediaType);

		return request;
	}

	@Before
	public void setUp()
			throws Exception {
		classUnderTest = new RequestParameters();
	}

	@After
	public void tearDown()
			throws Exception {}

	@Test(expected = UnsupportedMediaTypeException.class)
	public void injectThrowsWithNullRequest()
			throws Exception {
		classUnderTest.inject(null);
	}

	@Test(expected = UnsupportedMediaTypeException.class)
	public void injectThrowsWithWrongMediaType()
			throws Exception {
		Representation request = mockedRequest(MediaType.APPLICATION_PDF);

		classUnderTest.inject(request);
	}

	@Test
	public void injectSuccessfulWithJson()
			throws Exception {
		Representation request = mockedJsonRequest("{}");

		classUnderTest.inject(request);
	}

	@Test
	public void injectSuccessfulWithForm()
			throws Exception {
		Representation request = mockedFormRequest();

		classUnderTest.inject(request);
	}

	@Test
	public void getValueReturnsJsonString()
			throws Exception {
		testJSON = new JSONObject();
		String testString = "bar";
		testJSON.put(
				"foo",
				"bar");
		Representation request = mockedJsonRequest(testJSON.toString());
		classUnderTest.inject(request);

		assertEquals(
				testString,
				classUnderTest.getValue("foo"));
	}

	@Test
	public void getValueReturnsJsonList()
			throws Exception {
		testJSON = new JSONObject();
		List<String> testList = new ArrayList<String>(
				Arrays.asList(
						"bar",
						"baz"));
		testJSON.put(
				"foo",
				testList);
		Representation request = mockedJsonRequest(testJSON.toString());
		classUnderTest.inject(request);

		assertEquals(
				testList,
				classUnderTest.getValue("foo"));
	}

	@Test
	public void getValueReturnsJsonNumber()
			throws Exception {
		testJSON = new JSONObject();
		int testNumber = 42;
		testJSON.put(
				"foo",
				testNumber);
		Representation request = mockedJsonRequest(testJSON.toString());
		classUnderTest.inject(request);

		assertEquals(
				testNumber,
				classUnderTest.getValue("foo"));
	}

}

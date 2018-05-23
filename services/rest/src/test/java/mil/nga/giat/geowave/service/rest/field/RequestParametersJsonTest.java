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

public class RequestParametersJsonTest
{

	private RequestParametersJson classUnderTest;

	private JSONObject testJSON;

	private int testNumber = 42;
	private String testKey = "foo";
	private String testString = "bar";
	private List<String> testList = new ArrayList<String>(
			Arrays.asList(
					"bar",
					"baz"));
	private String[] testArray = {
		"foo",
		"bar"
	};

	private Representation mockedJsonRequest(
			String jsonString )
			throws IOException {
		Representation request = mockedRequest(MediaType.APPLICATION_JSON);

		Mockito.when(
				request.getText()).thenReturn(
				jsonString);

		return request;
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
			throws Exception {}

	@After
	public void tearDown()
			throws Exception {}

	@Test
	public void instantiationSuccessfulWithJson()
			throws Exception {
		Representation request = mockedJsonRequest("{}");

		classUnderTest = new RequestParametersJson(
				request);
	}

	@Test
	public void getValueReturnsJsonString()
			throws Exception {
		testJSON = new JSONObject();
		testJSON.put(
				testKey,
				testString);
		Representation request = mockedJsonRequest(testJSON.toString());
		classUnderTest = new RequestParametersJson(
				request);

		assertEquals(
				testString,
				classUnderTest.getValue(testKey));
	}

	@Test
	public void getStringReturnsJsonString()
			throws Exception {
		testJSON = new JSONObject();

		testJSON.put(
				testKey,
				testString);
		Representation request = mockedJsonRequest(testJSON.toString());
		classUnderTest = new RequestParametersJson(
				request);

		assertEquals(
				testString,
				classUnderTest.getString(testKey));
	}

	@Test
	public void getListReturnsJsonList()
			throws Exception {
		testJSON = new JSONObject();

		testJSON.put(
				testKey,
				testList);
		Representation request = mockedJsonRequest(testJSON.toString());
		classUnderTest = new RequestParametersJson(
				request);

		assertEquals(
				testList,
				classUnderTest.getList(testKey));
	}

	@Test
	public void getArrayReturnsJsonArray()
			throws Exception {
		testJSON = new JSONObject();

		testJSON.put(
				testKey,
				testArray);
		Representation request = mockedJsonRequest(testJSON.toString());
		classUnderTest = new RequestParametersJson(
				request);

		assertArrayEquals(
				testArray,
				classUnderTest.getArray(testKey));
	}

	@Test
	public void getValueReturnsJsonNumber()
			throws Exception {
		testJSON = new JSONObject();

		testJSON.put(
				testKey,
				testNumber);
		Representation request = mockedJsonRequest(testJSON.toString());
		classUnderTest = new RequestParametersJson(
				request);

		assertEquals(
				testNumber,
				classUnderTest.getValue(testKey));
	}

}

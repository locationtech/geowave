package mil.nga.giat.geowave.service.rest.field;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.restlet.data.Form;
import org.restlet.data.MediaType;
import org.restlet.data.Parameter;
import org.restlet.representation.Representation;

import mil.nga.giat.geowave.service.rest.exceptions.UnsupportedMediaTypeException;

public class RequestParametersTest
{

	private RequestParameters classUnderTest;

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

	private Form mockedForm(Map<String,String> inputKeyValuePairs) {
		String keyName;
		Form form = Mockito.mock(Form.class);
		Mockito.when(form.getNames()).thenReturn(inputKeyValuePairs.keySet());
		Mockito.when(form.getFirst(Mockito.anyString())).thenAnswer(i -> mockedFormParameter(inputKeyValuePairs.get(i.getArguments()[0])));
		
		return form;
	}

	private Parameter mockedFormParameter(
			String value ) {
		Parameter param = Mockito.mock(Parameter.class);

		Mockito.when(
				param.getValue()).thenReturn(
				value);

		return param;
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

		classUnderTest = new RequestParameters(
				request);
	}

	@Test
	public void instantiationSuccessfulWithForm()
			throws Exception {
		Map<String, String> testKVP = new HashMap<String, String>();

		Form form = mockedForm(testKVP);

		classUnderTest = new RequestParameters(
				form);
	}

	@Test
	public void getValueReturnsJsonString()
			throws Exception {
		testJSON = new JSONObject();
		testJSON.put(
				testKey,
				testString);
		Representation request = mockedJsonRequest(testJSON.toString());
		classUnderTest = new RequestParameters(
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
		classUnderTest = new RequestParameters(
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
		classUnderTest = new RequestParameters(
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
		classUnderTest = new RequestParameters(
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
		classUnderTest = new RequestParameters(
				request);

		assertEquals(
				testNumber,
				classUnderTest.getValue(testKey));
	}

	@Test
	public void getStringReturnsFormString()
			throws Exception {
		Map<String, String> testKVP = new HashMap<String, String>();

		Form form = mockedForm(testKVP);
		testKVP.put(
				testKey,
				testString);

		classUnderTest = new RequestParameters(
				form);

		assertEquals(
				testString,
				classUnderTest.getString(testKey));
	}

	@Test
	public void getListReturnsFormList()
			throws Exception {
		Map<String, String> testKVP = new HashMap<String, String>();

		String testJoinedString = String.join(
				",",
				testList);
		Form form = mockedForm(testKVP);
		testKVP.put(
				testKey,
				testJoinedString);

		classUnderTest = new RequestParameters(
				form);

		assertEquals(
				testList,
				classUnderTest.getList(testKey));
	}

	@Test
	public void getArrayReturnsFormArray()
			throws Exception {
		Map<String, String> testKVP = new HashMap<String, String>();

		String testJoinedString = String.join(
				",",
				testArray);
		Form form = mockedForm(testKVP);
		testKVP.put(
				testKey,
				testJoinedString);

		classUnderTest = new RequestParameters(
				form);

		assertArrayEquals(
				testArray,
				classUnderTest.getArray(testKey));
	}

}

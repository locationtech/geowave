package mil.nga.giat.geowave.service.rest.field;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.restlet.data.Form;
import org.restlet.data.Parameter;

public class RequestParametersFormTest
{

	private RequestParametersForm classUnderTest;

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
	public void instantiationSuccessfulWithForm()
			throws Exception {
		Map<String, String> testKVP = new HashMap<String, String>();

		Form form = mockedForm(testKVP);

		classUnderTest = new RequestParametersForm(
				form);
	}

	@Test
	public void getStringReturnsFormString()
			throws Exception {
		Map<String, String> testKVP = new HashMap<String, String>();

		Form form = mockedForm(testKVP);
		testKVP.put(
				testKey,
				testString);

		classUnderTest = new RequestParametersForm(
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

		classUnderTest = new RequestParametersForm(
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

		classUnderTest = new RequestParametersForm(
				form);

		assertArrayEquals(
				testArray,
				classUnderTest.getArray(testKey));
	}

}

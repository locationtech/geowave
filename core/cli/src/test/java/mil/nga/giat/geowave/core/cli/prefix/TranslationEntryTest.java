package mil.nga.giat.geowave.core.cli.prefix;

import static org.junit.Assert.*;

import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameterized;
import com.beust.jcommander.ParametersDelegate;
import com.beust.jcommander.WrappedParameter;

import junit.framework.Assert;
import mil.nga.giat.geowave.core.cli.annotations.PrefixParameter;

public class TranslationEntryTest
{

	private static class Arguments
	{
		@Parameter(names = "-name", description = "name description")
		Integer field;

		@ParametersDelegate
		@PrefixParameter(prefix = "obj")
		Map<String, Integer> map;
	}

	static TranslationEntry entry;
	static Parameterized param;
	static Integer obj;
	static String prefix;
	static AnnotatedElement aElement;

	@Before
	public void setUp() {
		try {

			Arguments args = new Arguments();
			ArrayList<Parameterized> params = (ArrayList<Parameterized>) Parameterized.parseArg(args);
			if (params.size() == 0) {
				fail("Could not find parameter");
			}

			param = params.get(0);

		}
		catch (SecurityException e) {
			// Should never trigger
			e.printStackTrace();
		}
		obj = 4;
		prefix = "prefix";
		aElement = Integer.class;
		entry = new TranslationEntry(
				param,
				(Object) obj,
				prefix,
				aElement);
	}

	@Test
	public void testGetParam() {
		Assert.assertEquals(
				param,
				entry.getParam());
	}

	@Test
	public void testGetObject() {
		Assert.assertEquals(
				obj,
				entry.getObject());
	}

	@Test
	public void testGetPrefix() {
		Assert.assertEquals(
				prefix,
				entry.getPrefix());
	}

	@Test
	public void testIsMethod() {
		Assert.assertFalse(entry.isMethod());
	}

	@Test
	public void testGetMember() {
		Assert.assertEquals(
				aElement,
				entry.getMember());
	}

	@Test
	public void testGetPrefixedNames() {
		Assert.assertTrue(Arrays.asList(
				entry.getPrefixedNames()).contains(
				"-" + prefix + ".name"));
	}

	@Test
	public void testGetDescription() {
		Assert.assertEquals(
				"name description",
				entry.getDescription());
	}

	@Test
	public void testIsPassword() {
		Assert.assertFalse(entry.isPassword());
	}

	@Test
	public void testIsHidden() {
		Assert.assertFalse(entry.isHidden());
	}

	@Test
	public void testIsRequired() {
		Assert.assertFalse(entry.isRequired());
	}

	@Test
	public void testGetAsPropertyName() {
		Assert.assertEquals(
				"prefix.name",
				entry.getAsPropertyName());
	}
}

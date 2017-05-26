package mil.nga.giat.geowave.core.cli.prefix;

import org.junit.Test;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;

import org.junit.Assert;

public class JCommanderTranslationMapTest
{
	@Test
	public void testCreateFacadesWithoutDelegate() {
		JCommanderPrefixTranslator translator = new JCommanderPrefixTranslator();
		translator.addObject(new ArgumentChildren());
		JCommanderTranslationMap map = translator.translate();
		map.createFacadeObjects();
		Assert.assertEquals(
				1,
				map.getObjects().size());
	}

	@Test
	public void testCreateFacadesWithDelegate() {
		JCommanderPrefixTranslator translator = new JCommanderPrefixTranslator();
		translator.addObject(new Arguments());
		JCommanderTranslationMap map = translator.translate();
		map.createFacadeObjects();
		Assert.assertEquals(
				2,
				map.getObjects().size());
	}

	public static class Arguments
	{
		@ParametersDelegate
		private ArgumentChildren children = new ArgumentChildren();

		@Parameter(names = "--arg2")
		private String arg2;
	}

	public static class ArgumentChildren
	{
		@Parameter(names = "--arg")
		private String arg;
	}

}

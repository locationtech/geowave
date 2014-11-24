package mil.nga.giat.geowave.analytics.tools;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.HashSet;
import java.util.Set;

import mil.nga.giat.geowave.analytics.parameters.ExtractParameters;
import mil.nga.giat.geowave.analytics.parameters.GlobalParameters;
import mil.nga.giat.geowave.analytics.parameters.ParameterEnum;
import mil.nga.giat.geowave.store.query.DistributableQuery;
import mil.nga.giat.geowave.store.query.SpatialQuery;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.junit.Test;

public class PropertyManagementTest
{

	@Test
	public void testQuery()
			throws Exception {
		PropertyManagement pm = new PropertyManagement();
		pm.store(
				new ParameterEnum[] {
					ExtractParameters.Extract.QUERY
				},
				new Object[] {
					"POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))"
				});
		DistributableQuery q = pm.getPropertyAsQuery(ExtractParameters.Extract.QUERY);
		assertNotNull(q);
		assertNotNull(((SpatialQuery) q).getQueryGeometry());
		assertEquals(
				"POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))",
				((SpatialQuery) q).getQueryGeometry().toText());
	}

	@Test
	public void testOption() {
		Set<Option> options = new HashSet<Option>();

		GlobalParameters.fillOptions(
				options,
				new GlobalParameters.Global[] {
					GlobalParameters.Global.ACCUMULO_INSTANCE
				});
		assertEquals(
				4,
				options.size());
		PropertyManagement.removeOption(
				options,
				GlobalParameters.Global.ACCUMULO_INSTANCE);
		assertEquals(
				3,
				options.size());
	}

	@Test
	public void testStore() {
		PropertyManagement pm = new PropertyManagement();
		pm.store(
				new ParameterEnum[] {
					ExtractParameters.Extract.ADAPTER_ID
				},
				new Object[] {
					"bar"
				});
		assertEquals(
				"bar",
				pm.getProperty(ExtractParameters.Extract.ADAPTER_ID));
	}

	@Test
	public void testCommandLine()
			throws ParseException {
		PropertyManagement pm = new PropertyManagement();
		final Options options = new Options();
		options.addOption(pm.newOption(
				ExtractParameters.Extract.ADAPTER_ID,
				"id",
				"test id",
				true));
		options.addOption(pm.newOption(
				ExtractParameters.Extract.MAX_INPUT_SPLIT,
				"mi",
				"test id",
				false));
		options.addOption(pm.newOption(
				ExtractParameters.Extract.REDUCER_COUNT,
				"rd",
				"test id",
				false));
		final BasicParser parser = new BasicParser();
		final CommandLine commandLine = parser.parse(
				options,
				new String[] {
					"-id",
					"y",
					"-rd"
				});
		pm.buildFromOptions(commandLine);

		assertTrue(pm.getPropertyAsBoolean(
				ExtractParameters.Extract.REDUCER_COUNT,
				false));
		assertFalse(pm.getPropertyAsBoolean(
				ExtractParameters.Extract.MAX_INPUT_SPLIT,
				false));
		assertEquals(
				"y",
				pm.getProperty(ExtractParameters.Extract.ADAPTER_ID));
	}
}

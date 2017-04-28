package mil.nga.giat.geowave.core.cli.operations;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import mil.nga.giat.geowave.core.cli.parser.CommandLineOperationParams;
import mil.nga.giat.geowave.core.cli.parser.OperationParser;
import mil.nga.giat.geowave.core.cli.spi.OperationRegistry;

public class HelpCommandTest
{
	@Test
	public void testPrepare() {
		final String[] args = {
			"help"
		};
		final OperationRegistry registry = OperationRegistry.getInstance();
		final OperationParser parser = new OperationParser(
				registry);
		final CommandLineOperationParams params = parser.parse(
				GeowaveTopLevelSection.class,
				args);

		final HelpCommand helpcommand = new HelpCommand();
		helpcommand.prepare(params);
		assertEquals(
				false,
				params.isValidate());
		assertEquals(
				true,
				params.isAllowUnknown());
	}
}

package mil.nga.giat.geowave.core.cli.operations;

import static org.junit.Assert.*;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import org.junit.Test;

import mil.nga.giat.geowave.core.cli.parser.CommandLineOperationParams;
import mil.nga.giat.geowave.core.cli.parser.OperationParser;
import mil.nga.giat.geowave.core.cli.spi.OperationRegistry;

public class ExplainCommandTest
{

	@Test
	public void testPrepare() {
		String[] args = {
			"explain"
		};
		OperationRegistry registry = OperationRegistry.getInstance();
		OperationParser parser = new OperationParser(
				registry);
		CommandLineOperationParams params = parser.parse(
				GeowaveTopLevelSection.class,
				args);

		ExplainCommand expcommand = new ExplainCommand();
		expcommand.prepare(params);
		assertEquals(
				false,
				params.isValidate());
		assertEquals(
				true,
				params.isAllowUnknown());
	}
}

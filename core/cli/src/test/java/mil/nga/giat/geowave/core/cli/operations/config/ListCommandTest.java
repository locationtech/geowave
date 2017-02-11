package mil.nga.giat.geowave.core.cli.operations.config;

import static org.junit.Assert.*;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import org.junit.Test;

import mil.nga.giat.geowave.core.cli.operations.GeowaveTopLevelSection;
import mil.nga.giat.geowave.core.cli.parser.CommandLineOperationParams;
import mil.nga.giat.geowave.core.cli.parser.OperationParser;
import mil.nga.giat.geowave.core.cli.spi.OperationRegistry;

public class ListCommandTest {

	@Test
	public void testExecute() {
		ByteArrayOutputStream output = new ByteArrayOutputStream();
		System.setOut(new PrintStream(output));
		
		String[] args = {"config", "list"};
		OperationRegistry registry = OperationRegistry.getInstance();
		OperationParser parser = new OperationParser(registry);
		CommandLineOperationParams params = parser.parse(
				GeowaveTopLevelSection.class,
				args);
		ListCommand lstcommand = new ListCommand();
		lstcommand.execute(params);
		
		String expectedoutput = "PROPERTIES (unknownversion-config.properties)\n";
		assertEquals(expectedoutput, output.toString());
	}

}

package mil.nga.giat.geowave.core.cli.operations;

import static org.junit.Assert.*;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import org.junit.Test;

import mil.nga.giat.geowave.core.cli.parser.CommandLineOperationParams;
import mil.nga.giat.geowave.core.cli.parser.OperationParser;
import mil.nga.giat.geowave.core.cli.spi.OperationRegistry;

public class HelpCommandTest {

	@Test
	public void testPrepare() {
		String[] args = {"help"};
		OperationRegistry registry = OperationRegistry.getInstance();
		OperationParser parser = new OperationParser(registry);
		final CommandLineOperationParams params = parser.parse(
				GeowaveTopLevelSection.class,
				args);
		
		HelpCommand helpcommand = new HelpCommand();
		helpcommand.prepare(params);
		assertEquals(false, params.isValidate());
		assertEquals(true, params.isAllowUnknown());
	}
	
	@Test
	public void testExecute() {
		ByteArrayOutputStream output = new ByteArrayOutputStream();
		System.setOut(new PrintStream(output));
		
		String[] args = {"help"};
		OperationRegistry registry = OperationRegistry.getInstance();
		OperationParser parser = new OperationParser(registry);
		final CommandLineOperationParams params = parser.parse(
				GeowaveTopLevelSection.class,
				args);
		
		HelpCommand helpcommand = new HelpCommand();
		helpcommand.execute(params);
		
		String expectedoutput = "Usage: geowave help [options]\n";
		assertEquals(expectedoutput, output.toString());
	}

}

package mil.nga.giat.geowave.core.cli.operations;

import static org.junit.Assert.*;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import org.junit.Test;

import mil.nga.giat.geowave.core.cli.parser.CommandLineOperationParams;
import mil.nga.giat.geowave.core.cli.parser.OperationParser;
import mil.nga.giat.geowave.core.cli.spi.OperationRegistry;

public class ExplainCommandTest {

	@Test
	public void testPrepare() {
		String[] args = {"explain"};
		OperationRegistry registry = OperationRegistry.getInstance();
		OperationParser parser = new OperationParser(registry);
		CommandLineOperationParams params = parser.parse(
				GeowaveTopLevelSection.class,
				args);
		
		ExplainCommand expcommand = new ExplainCommand();
		expcommand.prepare(params);
		assertEquals(false, params.isValidate());
		assertEquals(true, params.isAllowUnknown());
	}
	
	@Test
	public void testExecute() {
		ByteArrayOutputStream output = new ByteArrayOutputStream();
		System.setOut(new PrintStream(output));
		
		String[] args = {"explain"};
		OperationRegistry registry = OperationRegistry.getInstance();
		OperationParser parser = new OperationParser(registry);
		CommandLineOperationParams params = parser.parse(
				GeowaveTopLevelSection.class,
				args);
		ExplainCommand expcommand = new ExplainCommand();
		expcommand.execute(params);
		
		String expectedoutput = 
				"Command: geowave [options] <subcommand> ...\n\n"
				+ "                VALUE  NEEDED  PARAMETER NAMES                         \n"
				+ "----------------------------------------------\n"
				+ "{                    }         -cf, --config-file,                     \n"
				+ "{                    }         --debug,                                \n"
				+ "{                    }         --version,\n";
		assertEquals(expectedoutput, output.toString());
	}

}

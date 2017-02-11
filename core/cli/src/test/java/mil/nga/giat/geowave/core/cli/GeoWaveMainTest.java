package mil.nga.giat.geowave.core.cli;

import static org.junit.Assert.*;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import org.junit.Test;

public class GeoWaveMainTest {

	@Test
	public void testRun() {
		ByteArrayOutputStream output = new ByteArrayOutputStream();
		System.setOut(new PrintStream(output));
		
		String[] args = {"explain"};
		GeoWaveMain.main(args);
		
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

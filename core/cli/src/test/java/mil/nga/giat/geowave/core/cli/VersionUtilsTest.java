package mil.nga.giat.geowave.core.cli;

import static org.junit.Assert.*;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import org.junit.Test;

public class VersionUtilsTest {

	@Test
	public void testVersion() {
		String version = ""; //not sure what the version is
		assertEquals(version, VersionUtils.getVersion());
	}
	
	@Test
	public void testPrintVersionInfo() {
		ByteArrayOutputStream output = new ByteArrayOutputStream();
		System.setOut(new PrintStream(output));
		VersionUtils.printVersionInfo();
		String expectedoutput = ""; //empty properties
		assertEquals(expectedoutput, output);
	}

}

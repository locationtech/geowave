package mil.nga.giat.geowave.core.cli;

import static org.junit.Assert.*;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import org.junit.Test;

public class VersionUtilsTest
{

	@Test
	public void testVersion() {
		String version = null; // change this value when it gives a version
		assertEquals(
				version, // change this value when it gives a version
				VersionUtils.getVersion());
	}

	@Test
	public void testPrintVersionInfo() {
		ByteArrayOutputStream output = new ByteArrayOutputStream();
		System.setOut(new PrintStream(
				output));
		VersionUtils.printVersionInfo();
		String expectedoutput = "{}\n"; // change this value when it gives a
										// version
		assertEquals(
				expectedoutput,
				output.toString());
	}

}

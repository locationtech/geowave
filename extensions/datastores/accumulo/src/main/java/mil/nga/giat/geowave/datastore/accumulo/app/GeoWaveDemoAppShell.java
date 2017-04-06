package mil.nga.giat.geowave.datastore.accumulo.app;

//import java.io.IOException;
//import java.io.PrintWriter;

// @formatter:off

import org.apache.accumulo.shell.Shell;

// @formatter:on
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

//import jline.WindowsTerminal;
//import jline.console.ConsoleReader;
import mil.nga.giat.geowave.security.utils.SecurityUtils;

public class GeoWaveDemoAppShell
{

	public static void main(
			final String[] args )
			throws Exception {
		Logger.getRootLogger().setLevel(
				Level.WARN);

		final String instanceName = (System.getProperty("instanceName") != null) ? System.getProperty("instanceName")
				: "geowave";
		// GeoWave:811 - providing ability to support encrypted passwords
		final String password = SecurityUtils.decryptHexEncodedValue(((System.getProperty("password") != null) ? System
				.getProperty("password") : "password"));

		final String[] shellArgs = new String[] {
			"-u",
			"root",
			"-p",
			password,
			"-z",
			instanceName,
			"localhost:2181"
		};

		/*
		 * ConsoleReader reader = new ConsoleReader(System.in, System.out, new
		 * WindowsTerminal()); Shell s = new Shell(reader, new
		 * PrintWriter(System.out)); s.execute(shellArgs);
		 */

		Shell.main(shellArgs);
	}
}

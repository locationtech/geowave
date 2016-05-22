package mil.nga.giat.geowave.datastore.accumulo.app;

import java.io.IOException;
import java.io.PrintWriter;

// @formatter:off
/*if[accumulo.api=1.6]
import org.apache.accumulo.core.util.shell.Shell;
else[accumulo.api=1.6]*/
import org.apache.accumulo.shell.Shell;
/*end[accumulo.api=1.6]*/
// @formatter:on
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import jline.WindowsTerminal;
import jline.console.ConsoleReader;

public class GeoWaveDemoAppShell
{

	public static void main(
			final String[] args )
			throws Exception {
		Logger.getRootLogger().setLevel(
				Level.WARN);

		final String instanceName = (System.getProperty("instanceName") != null) ? System.getProperty("instanceName")
				: "geowave";
		final String password = (System.getProperty("password") != null) ? System.getProperty("password") : "password";

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

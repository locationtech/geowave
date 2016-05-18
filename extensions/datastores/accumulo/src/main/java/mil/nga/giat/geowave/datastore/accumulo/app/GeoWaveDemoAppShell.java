package mil.nga.giat.geowave.datastore.accumulo.app;

import java.io.IOException;

// @formatter:off
/*if[accumulo.api=1.6]
import org.apache.accumulo.core.util.shell.Shell;
else[accumulo.api=1.6]*/
import org.apache.accumulo.shell.Shell;
/*end[accumulo.api=1.6]*/
// @formatter:on
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class GeoWaveDemoAppShell
{

	public static void main(
			final String[] args )
			throws IOException {
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

		Shell.main(shellArgs);
	}
}

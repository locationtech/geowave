package mil.nga.giat.geowave.datastore.accumulo.app;

import org.apache.accumulo.core.util.shell.Shell;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.IOException;

public class GeoWaveDemoAppShell
{

	public static void main(
			String[] args )
			throws IOException {
		Logger.getRootLogger().setLevel(
				Level.WARN);

		final String instanceName = (System.getProperty("instanceName") != null) ? System.getProperty("instanceName") : "geowave";
		final String password = (System.getProperty("password") != null) ? System.getProperty("password") : "password";

		String[] shellArgs = new String[] {
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

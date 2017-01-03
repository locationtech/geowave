package mil.nga.giat.geowave.test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.net.URL;

import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecuteResultHandler;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteException;
import org.apache.commons.exec.ExecuteWatchdog;
import org.apache.commons.exec.Executor;
import org.apache.commons.io.IOUtils;
import org.codehaus.plexus.archiver.tar.TarGZipUnArchiver;
import org.codehaus.plexus.logging.console.ConsoleLogger;
import org.slf4j.LoggerFactory;

import com.jcraft.jsch.Logger;

public class DynamoDBLocal
{
	private final static org.slf4j.Logger LOGGER = LoggerFactory.getLogger(DynamoDBLocal.class);

	// these need to move to config
	private final static String DYNDB_URL = "http://dynamodb-local.s3-website-us-west-2.amazonaws.com/";
	private final static String DYNDB_TAR = "dynamodb_local_latest.tar.gz";
	private static final String HOST_PORT = "8000";

	private static final long EMULATOR_SPINUP_DELAY_MS = 30000L;

	private final File dynLocalDir;
	private ExecuteWatchdog watchdog;

	public DynamoDBLocal(
			String localDir ) {
		if (TestUtils.isSet(localDir)) {
			this.dynLocalDir = new File(
					localDir);
		}
		else {
			this.dynLocalDir = new File(
					TestUtils.TEMP_DIR,
					"dynamodb");
		}

		if (!this.dynLocalDir.exists() && !this.dynLocalDir.mkdirs()) {
			LOGGER.warn("unable to create directory " + this.dynLocalDir.getAbsolutePath());
		}
	}

	public boolean start() {
		if (!isInstalled()) {
			try {
				if (!install()) {
					return false;
				}
			}
			catch (IOException e) {
				LOGGER.error(e.getMessage());
				return false;
			}
		}

		try {
			startDynamoLocal();
		}
		catch (IOException | InterruptedException e) {
			LOGGER.error(e.getMessage());
			return false;
		}

		return true;
	}

	public boolean isRunning() {
		return (watchdog != null && watchdog.isWatching());
	}

	public void stop() {
		// first, ask the watchdog nicely:
		watchdog.destroyProcess();

		// hardStop();
	}

	private void hardStop() {
		// then kill all the extra emulator processes like this:
		final String KILL_CMD_1 = "for i in $(ps -ef | grep -i \"[b]eta emulators bigtable\" | awk '{print $2}'); do kill -9 $i; done";
		final String KILL_CMD_2 = "for i in $(ps -ef | grep -i \"[c]btemulator\" | awk '{print $2}'); do kill -9 $i; done";

		File bashFile = new File(
				TestUtils.TEMP_DIR,
				"kill-bigtable.sh");

		PrintWriter scriptWriter;
		try {
			Writer w = new OutputStreamWriter(
					new FileOutputStream(
							bashFile),
					"UTF-8");
			scriptWriter = new PrintWriter(
					w);
			scriptWriter.println("#!/bin/bash");
			scriptWriter.println("set -ev");
			scriptWriter.println(KILL_CMD_1);
			scriptWriter.println(KILL_CMD_2);
			scriptWriter.close();

			bashFile.setExecutable(true);
		}
		catch (FileNotFoundException e1) {
			LOGGER.error(
					"Unable to create dynamo local kill script",
					e1);
			return;
		}
		catch (UnsupportedEncodingException e) {
			LOGGER.error(
					"Unable to create dynamo local kill script",
					e);
		}

		CommandLine cmdLine = new CommandLine(
				bashFile.getAbsolutePath());
		DefaultExecutor executor = new DefaultExecutor();
		int exitValue = 0;

		try {
			exitValue = executor.execute(cmdLine);
		}
		catch (IOException ex) {
			LOGGER.error(
					"Unable to execute dynamo local kill script",
					ex);
		}

		LOGGER.warn("Dynamo local " + (exitValue == 0 ? "stopped" : "failed to stop"));
	}

	private boolean isInstalled() {
		final File dynLocalJar = new File(
				dynLocalDir,
				"DynamoDBLocal.jar");

		return (dynLocalJar.canRead());
	}

	protected boolean install()
			throws IOException {
		URL url = new URL(
				DYNDB_URL + DYNDB_TAR);

		final File downloadFile = new File(
				dynLocalDir,
				DYNDB_TAR);
		if (!downloadFile.exists()) {
			try (FileOutputStream fos = new FileOutputStream(
					downloadFile)) {
				IOUtils.copyLarge(
						url.openStream(),
						fos);
				fos.flush();
			}
		}

		final TarGZipUnArchiver unarchiver = new TarGZipUnArchiver();
		unarchiver.enableLogging(new ConsoleLogger(
				Logger.WARN,
				"DynamoDB Local Unarchive"));
		unarchiver.setSourceFile(downloadFile);
		unarchiver.setDestDirectory(dynLocalDir);
		unarchiver.extract();

		if (!downloadFile.delete()) {
			LOGGER.warn("cannot delete " + downloadFile.getAbsolutePath());
		}

		// Check the install
		if (!isInstalled()) {
			LOGGER.error("DynamoDB Local install failed");
			return false;
		}

		return true;
	}

	/**
	 * Using apache commons exec for cmd line execution
	 * 
	 * @param command
	 * @return exitCode
	 * @throws ExecuteException
	 * @throws IOException
	 * @throws InterruptedException
	 */
	private void startDynamoLocal()
			throws ExecuteException,
			IOException,
			InterruptedException {
		// java -Djava.library.path=./DynamoDBLocal_lib -jar DynamoDBLocal.jar
		// -sharedDb
		CommandLine cmdLine = new CommandLine(
				"java");

		cmdLine.addArgument("-Djava.library.path=" + dynLocalDir + "/DynamoDBLocal_lib");
		cmdLine.addArgument("-jar");
		cmdLine.addArgument(dynLocalDir + "/DynamoDBLocal.jar");
		cmdLine.addArgument("-sharedDb");
		cmdLine.addArgument("-inMemory");
		cmdLine.addArgument("-port");
		cmdLine.addArgument(HOST_PORT);

		// Using a result handler makes the emulator run async
		DefaultExecuteResultHandler resultHandler = new DefaultExecuteResultHandler();

		// watchdog shuts down the emulator, later
		watchdog = new ExecuteWatchdog(
				ExecuteWatchdog.INFINITE_TIMEOUT);
		Executor executor = new DefaultExecutor();
		executor.setWatchdog(watchdog);
		executor.execute(
				cmdLine,
				resultHandler);

		// we need to wait here for a bit, in case the emulator needs to update
		// itself
		Thread.sleep(EMULATOR_SPINUP_DELAY_MS);
	}
}

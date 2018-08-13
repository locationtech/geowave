/*******************************************************************************
 * Copyright (c) 2013-2017 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License,
 * Version 2.0 which accompanies this distribution and is available at
 * http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package mil.nga.giat.geowave.test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.net.URL;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;

import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecuteResultHandler;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteException;
import org.apache.commons.exec.ExecuteWatchdog;
import org.apache.commons.exec.Executor;
import org.apache.commons.exec.PumpStreamHandler;
import org.apache.commons.io.IOUtils;
import org.codehaus.plexus.archiver.tar.TarGZipUnArchiver;
import org.codehaus.plexus.logging.Logger;
import org.codehaus.plexus.logging.console.ConsoleLogger;
import org.slf4j.LoggerFactory;

import com.google.common.io.ByteStreams;

import mil.nga.giat.geowave.adapter.raster.util.ZipUtils;
import mil.nga.giat.geowave.core.index.StringUtils;

public class BigtableEmulator
{
	private final static org.slf4j.Logger LOGGER = LoggerFactory.getLogger(BigtableEmulator.class);

	// Property names
	public static final String HOST_PORT_PROPERTY = "bigtable.emulator.endpoint";
	public static final String INTERNAL_PROPERTY = "bigtable.emulator.internal";
	public static final String DOWNLOAD_URL_PROPERTY = "bigtable.sdk.url";
	public static final String DOWNLOAD_FILE_PROPERTY = "bigtable.sdk.file";

	// Download and executable paths
	private final String downloadUrl;
	private final String fileName;

	private final static String GCLOUD_EXE_DIR = "google-cloud-sdk/bin";
	private final Object STARTUP_LOCK = new Object();
	private boolean matchFound = false;
	private final long MAX_STARTUP_WAIT = 60000L; // if it doesn't start in 1
													// minute, just move on and
													// get it over with
	private final File sdkDir;
	private ExecuteWatchdog watchdog;

	public BigtableEmulator(
			final String sdkDir,
			final String sdkDownloadUrl,
			final String sdkFileName ) {
		if (TestUtils.isSet(sdkDir)) {
			this.sdkDir = new File(
					sdkDir);
		}
		else {
			this.sdkDir = new File(
					TestUtils.TEMP_DIR,
					"gcloud");
		}

		downloadUrl = sdkDownloadUrl;
		fileName = sdkFileName;

		if (!this.sdkDir.exists() && !this.sdkDir.mkdirs()) {
			LOGGER.warn("unable to create directory " + this.sdkDir.getAbsolutePath());
		}
	}

	public boolean start(
			final String emulatorHostPort ) {
		if (!isInstalled()) {
			try {
				if (!install()) {
					return false;
				}
			}
			catch (final IOException e) {
				LOGGER.error(e.getMessage());
				return false;
			}
		}

		try {
			startEmulator(emulatorHostPort);
		}
		catch (IOException | InterruptedException e) {
			LOGGER.error(e.getMessage());
			return false;
		}

		return true;
	}

	public boolean isRunning() {
		return ((watchdog != null) && watchdog.isWatching());
	}

	public void stop() {
		// first, ask the watchdog nicely:
		watchdog.destroyProcess();

		// then kill all the extra emulator processes like this:
		final String KILL_CMD_1 = "for i in $(ps -ef | grep -i \"[b]eta emulators bigtable\" | awk '{print $2}'); do kill -9 $i; done";
		final String KILL_CMD_2 = "for i in $(ps -ef | grep -i \"[c]btemulator\" | awk '{print $2}'); do kill -9 $i; done";

		final File bashFile = new File(
				TestUtils.TEMP_DIR,
				"kill-bigtable.sh");

		PrintWriter scriptWriter;
		try {
			final Writer w = new OutputStreamWriter(
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
		catch (final FileNotFoundException e1) {
			LOGGER.error(
					"Unable to create bigtable emulator kill script",
					e1);
			return;
		}
		catch (final UnsupportedEncodingException e) {
			LOGGER.error(
					"Unable to create bigtable emulator kill script",
					e);
		}

		final CommandLine cmdLine = new CommandLine(
				bashFile.getAbsolutePath());
		final DefaultExecutor executor = new DefaultExecutor();
		int exitValue = 0;

		try {
			exitValue = executor.execute(cmdLine);
		}
		catch (final IOException ex) {
			LOGGER.error(
					"Unable to execute bigtable emulator kill script",
					ex);
		}

		LOGGER.warn("Bigtable emulator " + (exitValue == 0 ? "stopped" : "failed to stop"));
	}

	private boolean isInstalled() {
		final File gcloudExe = new File(
				sdkDir,
				GCLOUD_EXE_DIR + "/gcloud");

		return (gcloudExe.canExecute());
	}

	protected boolean install()
			throws IOException {
		final URL url = new URL(
				downloadUrl + "/" + fileName);

		final File downloadFile = new File(
				sdkDir.getParentFile(),
				fileName);
		if (!downloadFile.exists()) {
			try (FileOutputStream fos = new FileOutputStream(
					downloadFile)) {
				IOUtils.copyLarge(
						url.openStream(),
						fos);
				fos.flush();
			}
		}
		if (downloadFile.getName().endsWith(
				".zip")) {
			ZipUtils.unZipFile(
					downloadFile,
					sdkDir.getAbsolutePath());
		}
		else if (downloadFile.getName().endsWith(
				".tar.gz")) {
			final TarGZipUnArchiver unarchiver = new TarGZipUnArchiver();
			unarchiver.enableLogging(new ConsoleLogger(
					Logger.LEVEL_WARN,
					"Gcloud SDK Unarchive"));
			unarchiver.setSourceFile(downloadFile);
			unarchiver.setDestDirectory(sdkDir);
			unarchiver.extract();
		}
		if (!downloadFile.delete()) {
			LOGGER.warn("cannot delete " + downloadFile.getAbsolutePath());
		}
		// Check the install
		if (!isInstalled()) {
			LOGGER.error("Gcloud install failed");
			return false;
		}

		// Install the beta components
		final File gcloudExe = new File(
				sdkDir,
				GCLOUD_EXE_DIR + "/gcloud");

		final CommandLine cmdLine = new CommandLine(
				gcloudExe);
		cmdLine.addArgument("components");
		cmdLine.addArgument("install");
		cmdLine.addArgument("beta");
		cmdLine.addArgument("--quiet");
		final DefaultExecutor executor = new DefaultExecutor();
		final int exitValue = executor.execute(cmdLine);

		return (exitValue == 0);
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
	private void startEmulator(
			final String emulatorHostPort )
			throws ExecuteException,
			IOException,
			InterruptedException {
		final CommandLine cmdLine = new CommandLine(
				sdkDir + "/" + GCLOUD_EXE_DIR + "/gcloud");
		cmdLine.addArgument("beta");
		cmdLine.addArgument("emulators");
		cmdLine.addArgument("bigtable");
		cmdLine.addArgument("start");
		cmdLine.addArgument("--quiet");
		cmdLine.addArgument("--host-port");
		cmdLine.addArgument(emulatorHostPort);

		// Using a result handler makes the emulator run async
		final DefaultExecuteResultHandler resultHandler = new DefaultExecuteResultHandler();

		// watchdog shuts down the emulator, later
		watchdog = new ExecuteWatchdog(
				ExecuteWatchdog.INFINITE_TIMEOUT);
		final Executor executor = new DefaultExecutor();
		executor.setWatchdog(watchdog);
		executor.setStreamHandler(new PumpStreamHandler(
				ByteStreams.nullOutputStream(),
				ByteStreams.nullOutputStream(),
				null) {
			@Override
			protected Thread createPump(
					final InputStream is,
					final OutputStream os,
					final boolean closeWhenExhausted ) {
				final FilterInputStream fis = new FilterInputStream(
						is) {
					byte[] startupBytes = ("running on " + emulatorHostPort).getBytes(StringUtils.UTF8_CHAR_SET);
					Queue<Integer> queue = new LinkedList<>();

					private boolean isStartupFound() {
						final Integer[] array = queue.toArray(new Integer[] {});
						final byte[] ba = new byte[array.length];
						for (int i = 0; i < ba.length; i++) {
							ba[i] = array[i].byteValue();
						}
						final Iterator<Integer> iterator = queue.iterator();

						for (final byte b : startupBytes) {
							if (!iterator.hasNext() || (b != iterator.next())) {
								return false;
							}
						}
						return true;
					}

					private void readAhead()
							throws IOException {
						// Work up some look-ahead.
						while (queue.size() < startupBytes.length) {
							final int next = super.read();
							queue.offer(next);

							if (next == -1) {
								break;
							}
						}
					}

					@Override
					public int read()
							throws IOException {
						if (matchFound) {
							super.read();
						}

						readAhead();

						if (isStartupFound()) {
							synchronized (STARTUP_LOCK) {

								STARTUP_LOCK.notifyAll();
							}
							matchFound = true;
						}

						return queue.remove();
					}

					@Override
					public int read(
							final byte b[] )
							throws IOException {
						if (matchFound) {
							super.read(b);
						}
						return read(
								b,
								0,
								b.length);
					}

					// copied straight from InputStream implementation,
					// just need to use `read()`
					// from this class
					@Override
					public int read(
							final byte b[],
							final int off,
							final int len )
							throws IOException {
						if (matchFound) {
							super.read(
									b,
									off,
									len);
						}
						if (b == null) {
							throw new NullPointerException();
						}
						else if ((off < 0) || (len < 0) || (len > (b.length - off))) {
							throw new IndexOutOfBoundsException();
						}
						else if (len == 0) {
							return 0;
						}

						int c = read();
						if (c == -1) {
							return -1;
						}
						b[off] = (byte) c;

						int i = 1;
						try {
							for (; i < len; i++) {
								c = read();
								if (c == -1) {
									break;
								}
								b[off + i] = (byte) c;
							}
						}
						catch (final IOException ee) {}
						return i;
					}
				};
				return super.createPump(
						fis,
						os,
						closeWhenExhausted);
			}
		});

		LOGGER.warn("Starting BigTable Emulator: " + cmdLine.toString());
		synchronized (STARTUP_LOCK) {
			executor.execute(
					cmdLine,
					resultHandler);
			STARTUP_LOCK.wait(MAX_STARTUP_WAIT);
		}
	}
}
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jline.WindowsTerminal;
import jline.console.ConsoleReader;

import mil.nga.giat.geowave.core.cli.operations.config.security.utils.SecurityUtils;

public class AccumuloMiniClusterShell
{

	public static void main(
			final String[] args )
			throws Exception {
		org.apache.log4j.Logger.getRootLogger().setLevel(
				org.apache.log4j.Level.WARN);

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

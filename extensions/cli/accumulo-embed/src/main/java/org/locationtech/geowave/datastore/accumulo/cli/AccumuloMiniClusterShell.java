/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.accumulo.cli;

import org.apache.accumulo.shell.Shell;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.config.Configurator;

public class AccumuloMiniClusterShell {

  public static void main(final String[] args) throws Exception {
    Configurator.setLevel(LogManager.getRootLogger().getName(), Level.WARN);

    final String instanceName =
        (System.getProperty("instanceName") != null) ? System.getProperty("instanceName")
            : "geowave";
    final String password =
        (System.getProperty("password") != null) ? System.getProperty("password") : "password";

    final String[] shellArgs =
        new String[] {"-u", "root", "-p", password, "-z", instanceName, "localhost:2181"};

    Shell.main(shellArgs);
  }
}

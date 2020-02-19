/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.vector.export;

import java.io.File;
import com.beust.jcommander.Parameter;

public class VectorLocalExportOptions extends VectorExportOptions {
  @Parameter(names = "--outputFile", required = true)
  private File outputFile;

  public File getOutputFile() {
    return outputFile;
  }

  public void setOutputFile(final File outputFile) {
    this.outputFile = outputFile;
  }
}

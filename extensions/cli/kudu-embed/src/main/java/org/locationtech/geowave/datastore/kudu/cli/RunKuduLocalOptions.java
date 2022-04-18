/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.kudu.cli;

import java.io.IOException;
import com.beust.jcommander.Parameter;

public class RunKuduLocalOptions {
  @Parameter(names = {"--directory", "-d"}, description = "The directory to use for Kudu")
  private String directory = KuduLocal.DEFAULT_DIR.getPath();


  @Parameter(names = {"--tablets", "-t"}, description = "The number of tablets to use for Kudu")
  private int tablets = 0;

  public String getDirectory() {
    return directory;
  }

  public int getTablets() {
    return tablets;
  }

  public void setDirectory(String directory) {
    this.directory = directory;
  }

  public void setTablets(int tablets) {
    this.tablets = tablets;
  }


  public KuduLocal getServer() throws IOException {
    return new KuduLocal(this);
  }
}

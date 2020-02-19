/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.dynamodb.cli;

import java.io.File;
import java.io.IOException;
import com.beust.jcommander.Parameter;

public class RunDynamoDBLocalOptions {
  @Parameter(names = {"--directory", "-d"}, description = "The directory to use for DynamoDB")
  private String directory = DynamoDBLocal.DEFAULT_DIR.getPath();


  public String getDirectory() {
    return directory;
  }

  public void setDirectory(String directory) {
    this.directory = directory;
  }


  public DynamoDBLocal getServer() throws IOException {
    return new DynamoDBLocal(directory);
  }
}

/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.cli.osm.operations;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.locationtech.geowave.cli.osm.parser.OsmPbfParser;
import org.locationtech.geowave.cli.osm.parser.OsmPbfParserOptions;
import org.locationtech.geowave.core.cli.annotations.GeowaveOperation;
import org.locationtech.geowave.core.cli.api.Command;
import org.locationtech.geowave.core.cli.api.DefaultOperation;
import org.locationtech.geowave.core.cli.api.OperationParams;
import org.locationtech.geowave.core.cli.operations.config.options.ConfigOptions;
import org.locationtech.geowave.mapreduce.operations.ConfigHDFSCommand;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;

@GeowaveOperation(name = "stage", parentOperation = OSMSection.class)
@Parameters(commandDescription = "Stage OSM data to HDFS")
public class StageOSMToHDFSCommand extends DefaultOperation implements Command {

  @Parameter(description = "<file or directory> <path to base directory to write to>")
  private List<String> parameters = new ArrayList<String>();

  @ParametersDelegate
  private OsmPbfParserOptions parserOptions = new OsmPbfParserOptions();

  @Override
  public void execute(final OperationParams params) throws Exception {

    // Ensure we have all the required arguments
    if (parameters.size() != 2) {
      throw new ParameterException(
          "Requires arguments: <file or directory>  <path to base directory to write to>");
    }

    final String inputPath = parameters.get(0);
    final String basePath = parameters.get(1);

    // Config file
    final File configFile = getGeoWaveConfigFile(params);
    final Properties configProperties = ConfigOptions.loadProperties(configFile);
    final String hdfsHostPort = ConfigHDFSCommand.getHdfsUrl(configProperties);

    // These are set as main parameter arguments, to keep consistency with
    // GeoWave.
    parserOptions.setIngestDirectory(inputPath);
    parserOptions.setHdfsBasePath(basePath);
    parserOptions.setNameNode(hdfsHostPort);

    final OsmPbfParser osmPbfParser = new OsmPbfParser();
    final Configuration conf = osmPbfParser.stageData(parserOptions);

    final ContentSummary cs = getHDFSFileSummary(conf, basePath);
    System.out.println("**************************************************");
    System.out.println("Directories: " + cs.getDirectoryCount());
    System.out.println("Files: " + cs.getFileCount());
    System.out.println(
        "Nodes size: " + getHDFSFileSummary(conf, parserOptions.getNodesBasePath()).getLength());
    System.out.println(
        "Ways size: " + getHDFSFileSummary(conf, parserOptions.getWaysBasePath()).getLength());
    System.out.println(
        "Relations size: "
            + getHDFSFileSummary(conf, parserOptions.getRelationsBasePath()).getLength());
    System.out.println("**************************************************");
    System.out.println("finished osmpbf ingest");
  }

  public List<String> getParameters() {
    return parameters;
  }

  public void setParameters(final String fileOrDirectory, final String hdfsPath) {
    parameters.clear();

    parameters.add(fileOrDirectory);
    parameters.add(hdfsPath);
  }

  public OsmPbfParserOptions getParserOptions() {
    return parserOptions;
  }

  public void setParserOptions(final OsmPbfParserOptions parserOptions) {
    this.parserOptions = parserOptions;
  }

  private static ContentSummary getHDFSFileSummary(final Configuration conf, final String filename)
      throws IOException {
    final org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(filename);
    final FileSystem file = path.getFileSystem(conf);
    final ContentSummary cs = file.getContentSummary(path);
    file.close();
    return cs;
  }
}

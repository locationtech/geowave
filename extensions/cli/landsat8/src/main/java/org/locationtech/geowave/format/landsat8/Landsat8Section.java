/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.format.landsat8;

import org.locationtech.geowave.core.cli.annotations.GeowaveOperation;
import org.locationtech.geowave.core.cli.api.DefaultOperation;
import org.locationtech.geowave.core.cli.operations.util.UtilSection;
import com.beust.jcommander.Parameters;

@GeowaveOperation(name = "landsat", parentOperation = UtilSection.class)
@Parameters(
    commandDescription = "Commands to analyze, download, and ingest Landsat 8 imagery publicly available on AWS")
public class Landsat8Section extends DefaultOperation {
}

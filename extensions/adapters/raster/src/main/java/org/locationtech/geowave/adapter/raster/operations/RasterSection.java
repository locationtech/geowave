/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.raster.operations;

import org.locationtech.geowave.core.cli.annotations.GeowaveOperation;
import org.locationtech.geowave.core.cli.api.DefaultOperation;
import org.locationtech.geowave.core.cli.operations.GeowaveTopLevelSection;
import com.beust.jcommander.Parameters;

@GeowaveOperation(name = "raster", parentOperation = GeowaveTopLevelSection.class)
@Parameters(commandDescription = "Operations to perform transformations on raster data in GeoWave")
public class RasterSection extends DefaultOperation {
}

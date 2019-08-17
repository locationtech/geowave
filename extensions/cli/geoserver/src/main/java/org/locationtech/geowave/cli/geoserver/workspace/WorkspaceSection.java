/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.cli.geoserver.workspace;

import org.locationtech.geowave.cli.geoserver.GeoServerSection;
import org.locationtech.geowave.core.cli.annotations.GeowaveOperation;
import org.locationtech.geowave.core.cli.api.DefaultOperation;
import com.beust.jcommander.Parameters;

@GeowaveOperation(name = {"ws", "workspace"}, parentOperation = GeoServerSection.class)
@Parameters(commandDescription = "Commands for configuring GeoServer workspaces")
public class WorkspaceSection extends DefaultOperation {
}

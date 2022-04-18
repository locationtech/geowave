/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 * 
 * See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.kudu.cli;

import org.locationtech.geowave.core.cli.annotations.GeowaveOperation;
import org.locationtech.geowave.core.cli.api.DefaultOperation;
import org.locationtech.geowave.core.cli.operations.util.UtilSection;
import com.beust.jcommander.Parameters;

@GeowaveOperation(name = "kudu", parentOperation = UtilSection.class)
@Parameters(commandDescription = "Kudu embedded server commands")
public class KuduSection extends DefaultOperation {

}

/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 * 
 * See the NOTICE file distributed with this work for additional information regarding copyright ownership. All rights reserved. This program and the accompanying materials are made available under the terms of the Apache License, Version 2.0 which accompanies this distribution and is available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.vector.cli;

import org.locationtech.geowave.core.cli.annotations.GeowaveOperation;
import org.locationtech.geowave.core.cli.api.DefaultOperation;
import org.locationtech.geowave.core.cli.operations.GeowaveTopLevelSection;

import com.beust.jcommander.Parameters;

@GeowaveOperation(name = "vector", parentOperation = GeowaveTopLevelSection.class)
@Parameters(commandDescription = "Vector data operations")
public class VectorSection extends
		DefaultOperation
{

}

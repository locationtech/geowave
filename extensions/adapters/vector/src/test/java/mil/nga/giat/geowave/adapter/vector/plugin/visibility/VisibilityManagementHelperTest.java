/*******************************************************************************
 * Copyright (c) 2013-2017 Contributors to the Eclipse Foundation
 * 
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License,
 * Version 2.0 which accompanies this distribution and is available at
 * http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package mil.nga.giat.geowave.adapter.vector.plugin.visibility;

import static org.junit.Assert.*;
import mil.nga.giat.geowave.adapter.vector.plugin.visibility.VisibilityManagementHelper;

import org.junit.Test;

public class VisibilityManagementHelperTest
{

	@Test
	public void test() {
		assertNotNull(VisibilityManagementHelper.loadVisibilityManagement());
	}

}

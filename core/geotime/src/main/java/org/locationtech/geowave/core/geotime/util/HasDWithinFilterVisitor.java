/*******************************************************************************
 * Copyright (c) 2013-2018 Contributors to the Eclipse Foundation
 *   
 *  See the NOTICE file distributed with this work for additional
 *  information regarding copyright ownership.
 *  All rights reserved. This program and the accompanying materials
 *  are made available under the terms of the Apache License,
 *  Version 2.0 which accompanies this distribution and is available at
 *  http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package org.locationtech.geowave.core.geotime.util;

import org.geotools.filter.visitor.NullFilterVisitor;
import org.opengis.filter.spatial.DWithin;

public class HasDWithinFilterVisitor extends
		NullFilterVisitor
{
	private boolean hasDWithin = false;

	@Override
	public Object visit(
			DWithin filter,
			Object data ) {
		hasDWithin = true;
		return super.visit(
				filter,
				data);
	}

	public boolean hasDWithin() {
		return hasDWithin;
	}
}

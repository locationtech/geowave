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
package org.locationtech.geowave.analytic.spark.spatial;

import java.io.Serializable;

public class JoinOptions implements
		Serializable
{

	public static enum BuildSide {
		LEFT,
		RIGHT;
	}

	private BuildSide joinBuildSide = BuildSide.LEFT;
	private boolean negativePredicate = false;

	public JoinOptions() {}

	public JoinOptions(
			boolean negativeTest ) {
		this.negativePredicate = negativeTest;
	}

	public boolean isNegativePredicate() {
		return negativePredicate;
	}

	public void setNegativePredicate(
			boolean negativePredicate ) {
		this.negativePredicate = negativePredicate;
	}

	public BuildSide getJoinBuildSide() {
		return joinBuildSide;
	}

	public void setJoinBuildSide(
			BuildSide joinBuildSide ) {
		this.joinBuildSide = joinBuildSide;
	}

}

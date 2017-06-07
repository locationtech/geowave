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
package mil.nga.giat.geowave.analytic.mapreduce;

import org.apache.hadoop.io.Text;

public class GroupIDText extends
		Text
{

	public void set(
			final String groupID,
			final String id ) {
		super.set((groupID == null ? "##" : groupID) + "," + id);
	}

	public String getGroupID() {
		final String t = toString();
		final String groupID = t.substring(
				0,
				t.indexOf(','));
		return ("##".equals(groupID)) ? null : groupID;
	}

	public String getID() {
		final String t = toString();
		return t.substring(t.indexOf(',') + 1);
	}
}

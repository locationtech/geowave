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
package mil.nga.giat.geowave.analytic.kmeans;

import mil.nga.giat.geowave.analytic.clustering.CentroidPairing;

/**
 * 
 * Callback with the pairing of a point to its closest centroid at a zoom level.
 * 
 * @see CentroidAssociationFn
 * @param <T>
 */
public interface AssociationNotification<T>
{
	public void notify(
			CentroidPairing<T> pairing );
}

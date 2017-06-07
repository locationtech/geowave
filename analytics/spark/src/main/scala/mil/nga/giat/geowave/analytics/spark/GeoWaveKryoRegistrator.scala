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
package mil.nga.giat.geowave.analytics.spark

import org.apache.spark.serializer.KryoRegistrator
import com.esotericsoftware.kryo.Kryo
import mil.nga.giat.geowave.analytic.kryo.FeatureSerializer
import org.opengis.feature.simple.SimpleFeature
import org.geotools.feature.simple.SimpleFeatureImpl

class GeoWaveKryoRegistrator extends KryoRegistrator {
	  override def registerClasses(kryo: Kryo) {
	    kryo.register(classOf[SimpleFeatureImpl], new FeatureSerializer )
	  }
}

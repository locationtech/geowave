/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.analytic.mapreduce.kmeans;

import java.io.Serializable;
import java.util.UUID;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;

public class TestObject implements Serializable {
  /** */
  private static final long serialVersionUID = 1L;

  private static final GeometryFactory factory = new GeometryFactory();

  public Geometry geo;
  public String id;
  public String groupID = "1";
  public String name;
  public int level = 1;

  public TestObject() {
    id = UUID.randomUUID().toString();
  }

  public TestObject(final Geometry geo, final String id, final String groupID) {
    super();
    this.geo = geo;
    this.id = id;
    this.groupID = groupID;
    name = id;
  }

  public TestObject(final Coordinate coor, final String id) {
    geo = factory.createPoint(coor);
    geo.setSRID(2029);
    this.id = id;
    name = id;
  }

  public int getLevel() {
    return level;
  }

  public void setLevel(final int level) {
    this.level = level;
  }

  public String getName() {
    return name;
  }

  public void setName(final String name) {
    this.name = name;
  }

  public String getGroupID() {
    return groupID;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = (prime * result) + ((geo == null) ? 0 : geo.hashCode());
    result = (prime * result) + ((id == null) ? 0 : id.hashCode());
    return result;
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    final TestObject other = (TestObject) obj;
    if (geo == null) {
      if (other.geo != null) {
        return false;
      }
    } else if (!geo.equals(other.geo)) {
      return false;
    }
    if (id == null) {
      if (other.id != null) {
        return false;
      }
    } else if (!id.equals(other.id)) {
      return false;
    }
    return true;
  }
}

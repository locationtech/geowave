/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.kudu;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Type;
import java.util.List;
import java.util.function.BiConsumer;

public enum KuduColumnType {
  PARTITION_KEY((final List<ColumnSchema> c, final Pair<String, Type> f) -> c.add(
      new ColumnSchema.ColumnSchemaBuilder(f.getLeft(), f.getRight()).key(true).build())),
  CLUSTER_COLUMN((final List<ColumnSchema> c, final Pair<String, Type> f) -> c.add(
      new ColumnSchema.ColumnSchemaBuilder(f.getLeft(), f.getRight()).key(true).build())),
  OTHER_COLUMN((final List<ColumnSchema> c, final Pair<String, Type> f) -> c.add(
      new ColumnSchema.ColumnSchemaBuilder(f.getLeft(), f.getRight()).build()));

  BiConsumer<List<ColumnSchema>, Pair<String, Type>> createFunction;

  KuduColumnType(final BiConsumer<List<ColumnSchema>, Pair<String, Type>> createFunction) {
    this.createFunction = createFunction;
  }
}

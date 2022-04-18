/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.adapter.annotation;

import java.lang.reflect.Field;
import org.locationtech.geowave.core.store.adapter.FieldDescriptor;

/**
 * Interface for creating field descriptors from annotated fields.
 */
public interface AnnotatedFieldDescriptorBuilder {
  FieldDescriptor<?> buildFieldDescriptor(Field field);
}

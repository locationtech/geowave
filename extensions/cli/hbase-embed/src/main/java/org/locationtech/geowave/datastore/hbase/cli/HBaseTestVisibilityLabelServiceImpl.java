/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.hbase.cli;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.security.visibility.DefaultVisibilityLabelServiceImpl;

/**
 * This class exists to circumvent the issue with the Visibility IT failing when the user running
 * the test is a superuser.
 *
 */
public class HBaseTestVisibilityLabelServiceImpl extends DefaultVisibilityLabelServiceImpl {
  @Override
  protected boolean isReadFromSystemAuthUser() throws IOException {
    return false;
  }

  @Override
  public List<Tag> createVisibilityExpTags(
      final String visExpression,
      final boolean withSerializationFormat,
      final boolean checkAuths) throws IOException {
    if ((visExpression != null) && visExpression.isEmpty()) {
      return null;
    }

    return super.createVisibilityExpTags(visExpression, withSerializationFormat, checkAuths);
  }
}

/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.cli.api;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.locationtech.geowave.core.cli.api.ServiceEnabledCommand.HttpMethod;

public class ServiceEnableCommandTest {

  private class ServiceEnabledCommand_TESTING extends ServiceEnabledCommand {

    private final HttpMethod method;

    public ServiceEnabledCommand_TESTING(final HttpMethod method) {
      this.method = method;
    }

    @Override
    public void execute(final OperationParams params) throws Exception {}

    @Override
    public Object computeResults(final OperationParams params) throws Exception {
      return null;
    }

    @Override
    public HttpMethod getMethod() {
      return method;
    }
  }

  @Before
  public void setUp() throws Exception {}

  @After
  public void tearDown() throws Exception {}

  @Test
  public void defaultSuccessStatusIs200ForGET() {

    final ServiceEnabledCommand_TESTING classUnderTest =
        new ServiceEnabledCommand_TESTING(HttpMethod.GET);

    Assert.assertEquals(true, classUnderTest.successStatusIs200());
  }

  @Test
  public void defaultSuccessStatusIs201ForPOST() {

    final ServiceEnabledCommand_TESTING classUnderTest =
        new ServiceEnabledCommand_TESTING(HttpMethod.POST);

    Assert.assertEquals(false, classUnderTest.successStatusIs200());
  }
}

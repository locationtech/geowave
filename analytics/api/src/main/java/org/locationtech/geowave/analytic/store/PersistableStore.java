/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.analytic.store;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.index.VarintUtils;
import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.store.cli.store.DataStorePluginOptions;

public class PersistableStore implements Persistable {
  // Using this here instead of raw DataStorePluginOptions, so we can
  // use the convenient methods
  private DataStorePluginOptions pluginOptions;

  public PersistableStore() {}

  public PersistableStore(final DataStorePluginOptions options) {
    pluginOptions = options;
  }

  public DataStorePluginOptions getDataStoreOptions() {
    return pluginOptions;
  }

  @Override
  public byte[] toBinary() {
    // Persist
    final Properties strOptions = new Properties();
    pluginOptions.save(strOptions, null);
    final List<byte[]> strOptionsBinary = new ArrayList<>(strOptions.size());
    int optionsLength = 0;
    for (final String key : strOptions.stringPropertyNames()) {
      final byte[] keyBinary = StringUtils.stringToBinary(key);
      final byte[] valueBinary = StringUtils.stringToBinary(strOptions.getProperty(key));
      final int entryLength =
          keyBinary.length
              + valueBinary.length
              + VarintUtils.unsignedIntByteLength(keyBinary.length)
              + VarintUtils.unsignedIntByteLength(valueBinary.length);
      final ByteBuffer buf = ByteBuffer.allocate(entryLength);
      VarintUtils.writeUnsignedInt(keyBinary.length, buf);
      buf.put(keyBinary);
      VarintUtils.writeUnsignedInt(valueBinary.length, buf);
      buf.put(valueBinary);
      strOptionsBinary.add(buf.array());
      optionsLength += entryLength;
    }
    optionsLength += VarintUtils.unsignedIntByteLength(strOptionsBinary.size());
    final ByteBuffer buf = ByteBuffer.allocate(optionsLength);
    VarintUtils.writeUnsignedInt(strOptionsBinary.size(), buf);
    for (final byte[] strOption : strOptionsBinary) {
      buf.put(strOption);
    }
    return buf.array();
  }

  @Override
  public void fromBinary(final byte[] bytes) {
    final ByteBuffer buf = ByteBuffer.wrap(bytes);
    final int configOptionLength = VarintUtils.readUnsignedInt(buf);
    final Properties configOptions = new Properties();
    for (int i = 0; i < configOptionLength; i++) {
      final int keyLength = VarintUtils.readUnsignedInt(buf);
      final byte[] keyBinary = new byte[keyLength];
      buf.get(keyBinary);
      final int valueLength = VarintUtils.readUnsignedInt(buf);
      final byte[] valueBinary = new byte[valueLength];
      buf.get(valueBinary);
      configOptions.put(
          StringUtils.stringFromBinary(keyBinary),
          StringUtils.stringFromBinary(valueBinary));
    }
    pluginOptions = new DataStorePluginOptions();
    pluginOptions.load(configOptions, null);
  }
}

/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
/**
 * Autogenerated by Avro
 *
 * <p> DO NOT EDIT DIRECTLY
 */
package org.locationtech.geowave.core.ingest.hdfs;

@SuppressWarnings("all")
@org.apache.avro.specific.AvroGenerated
public class HdfsFile extends org.apache.avro.specific.SpecificRecordBase implements
    org.apache.avro.specific.SpecificRecord {
  public static final org.apache.avro.Schema SCHEMA$ =
      new org.apache.avro.Schema.Parser().parse(
          "{\"type\":\"record\",\"name\":\"HdfsFile\",\"namespace\":\"org.locationtech.geowave.core.ingest.hdfs\",\"fields\":[{\"name\":\"originalFile\",\"type\":\"bytes\",\"doc\":\"Original file data\"},{\"name\":\"originalFilePath\",\"type\":[\"string\",\"null\"],\"doc\":\"Original file path\"}],\"doc:\":\"Stores the original files from a local file system in HDFS\"}");

  public static org.apache.avro.Schema getClassSchema() {
    return SCHEMA$;
  }

  /** Original file data */
  @Deprecated
  public java.nio.ByteBuffer originalFile;
  /** Original file path */
  @Deprecated
  public java.lang.CharSequence originalFilePath;

  /**
   * Default constructor. Note that this does not initialize fields to their default values from the
   * schema. If that is desired then one should use <code>newBuilder()</code>.
   */
  public HdfsFile() {}

  /** All-args constructor. */
  public HdfsFile(
      final java.nio.ByteBuffer originalFile,
      final java.lang.CharSequence originalFilePath) {
    this.originalFile = originalFile;
    this.originalFilePath = originalFilePath;
  }

  @Override
  public org.apache.avro.Schema getSchema() {
    return SCHEMA$;
  }

  // Used by DatumWriter. Applications should not call.
  @Override
  public java.lang.Object get(final int field$) {
    switch (field$) {
      case 0:
        return originalFile;
      case 1:
        return originalFilePath;
      default:
        throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  // Used by DatumReader. Applications should not call.
  @Override
  @SuppressWarnings(value = "unchecked")
  public void put(final int field$, final java.lang.Object value$) {
    switch (field$) {
      case 0:
        originalFile = (java.nio.ByteBuffer) value$;
        break;
      case 1:
        originalFilePath = (java.lang.CharSequence) value$;
        break;
      default:
        throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  /** Gets the value of the 'originalFile' field. Original file data */
  public java.nio.ByteBuffer getOriginalFile() {
    return originalFile;
  }

  /**
   * Sets the value of the 'originalFile' field. Original file data * @param value the value to set.
   */
  public void setOriginalFile(final java.nio.ByteBuffer value) {
    originalFile = value;
  }

  /** Gets the value of the 'originalFilePath' field. Original file path */
  public java.lang.CharSequence getOriginalFilePath() {
    return originalFilePath;
  }

  /**
   * Sets the value of the 'originalFilePath' field. Original file path * @param value the value to
   * set.
   */
  public void setOriginalFilePath(final java.lang.CharSequence value) {
    originalFilePath = value;
  }

  /** Creates a new HdfsFile RecordBuilder */
  public static org.locationtech.geowave.core.ingest.hdfs.HdfsFile.Builder newBuilder() {
    return new org.locationtech.geowave.core.ingest.hdfs.HdfsFile.Builder();
  }

  /** Creates a new HdfsFile RecordBuilder by copying an existing Builder */
  public static org.locationtech.geowave.core.ingest.hdfs.HdfsFile.Builder newBuilder(
      final org.locationtech.geowave.core.ingest.hdfs.HdfsFile.Builder other) {
    return new org.locationtech.geowave.core.ingest.hdfs.HdfsFile.Builder(other);
  }

  /** Creates a new HdfsFile RecordBuilder by copying an existing HdfsFile instance */
  public static org.locationtech.geowave.core.ingest.hdfs.HdfsFile.Builder newBuilder(
      final org.locationtech.geowave.core.ingest.hdfs.HdfsFile other) {
    return new org.locationtech.geowave.core.ingest.hdfs.HdfsFile.Builder(other);
  }

  /** RecordBuilder for HdfsFile instances. */
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<HdfsFile>
      implements
      org.apache.avro.data.RecordBuilder<HdfsFile> {

    private java.nio.ByteBuffer originalFile;
    private java.lang.CharSequence originalFilePath;

    /** Creates a new Builder */
    private Builder() {
      super(org.locationtech.geowave.core.ingest.hdfs.HdfsFile.SCHEMA$);
    }

    /** Creates a Builder by copying an existing Builder */
    private Builder(final org.locationtech.geowave.core.ingest.hdfs.HdfsFile.Builder other) {
      super(other);
      if (isValidValue(fields()[0], other.originalFile)) {
        originalFile = data().deepCopy(fields()[0].schema(), other.originalFile);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.originalFilePath)) {
        originalFilePath = data().deepCopy(fields()[1].schema(), other.originalFilePath);
        fieldSetFlags()[1] = true;
      }
    }

    /** Creates a Builder by copying an existing HdfsFile instance */
    private Builder(final org.locationtech.geowave.core.ingest.hdfs.HdfsFile other) {
      super(org.locationtech.geowave.core.ingest.hdfs.HdfsFile.SCHEMA$);
      if (isValidValue(fields()[0], other.originalFile)) {
        originalFile = data().deepCopy(fields()[0].schema(), other.originalFile);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.originalFilePath)) {
        originalFilePath = data().deepCopy(fields()[1].schema(), other.originalFilePath);
        fieldSetFlags()[1] = true;
      }
    }

    /** Gets the value of the 'originalFile' field */
    public java.nio.ByteBuffer getOriginalFile() {
      return originalFile;
    }

    /** Sets the value of the 'originalFile' field */
    public org.locationtech.geowave.core.ingest.hdfs.HdfsFile.Builder setOriginalFile(
        final java.nio.ByteBuffer value) {
      validate(fields()[0], value);
      originalFile = value;
      fieldSetFlags()[0] = true;
      return this;
    }

    /** Checks whether the 'originalFile' field has been set */
    public boolean hasOriginalFile() {
      return fieldSetFlags()[0];
    }

    /** Clears the value of the 'originalFile' field */
    public org.locationtech.geowave.core.ingest.hdfs.HdfsFile.Builder clearOriginalFile() {
      originalFile = null;
      fieldSetFlags()[0] = false;
      return this;
    }

    /** Gets the value of the 'originalFilePath' field */
    public java.lang.CharSequence getOriginalFilePath() {
      return originalFilePath;
    }

    /** Sets the value of the 'originalFilePath' field */
    public org.locationtech.geowave.core.ingest.hdfs.HdfsFile.Builder setOriginalFilePath(
        final java.lang.CharSequence value) {
      validate(fields()[1], value);
      originalFilePath = value;
      fieldSetFlags()[1] = true;
      return this;
    }

    /** Checks whether the 'originalFilePath' field has been set */
    public boolean hasOriginalFilePath() {
      return fieldSetFlags()[1];
    }

    /** Clears the value of the 'originalFilePath' field */
    public org.locationtech.geowave.core.ingest.hdfs.HdfsFile.Builder clearOriginalFilePath() {
      originalFilePath = null;
      fieldSetFlags()[1] = false;
      return this;
    }

    @Override
    public HdfsFile build() {
      try {
        final HdfsFile record = new HdfsFile();
        record.originalFile =
            fieldSetFlags()[0] ? originalFile : (java.nio.ByteBuffer) defaultValue(fields()[0]);
        record.originalFilePath =
            fieldSetFlags()[1] ? originalFilePath
                : (java.lang.CharSequence) defaultValue(fields()[1]);
        return record;
      } catch (final Exception e) {
        throw new org.apache.avro.AvroRuntimeException(e);
      }
    }
  }
}

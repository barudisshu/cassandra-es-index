/*
 * Copyright Ericsson AB 2019 - All Rights Reserved.
 * The copyright to the computer program(s) herein is the property of Ericsson AB.
 * The programs may be used and/or copied only with written permission from Ericsson AB
 * or in accordance with the terms and conditions stipulated in the agreement/contract under which the program(s) have been supplied.
 */
package com.ericsson.godzilla.cassandra.index.config;

import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.utils.FBUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import static org.apache.commons.lang3.StringUtils.isBlank;

/**
 * This reads options from es-index.properties, override Cassandra options is such file is found and
 * provide per dc/rack reading
 */
public class OptionReader {
  private static final Logger LOGGER = LoggerFactory.getLogger(OptionReader.class);

  private static final String CLASSPATH_PREFIX = "classpath:";
  private static final String CFG_FILE_KEY = IndexConfig.ES_CONFIG_PREFIX + IndexConfig.ES_FILE;
  private static final String[] FILES = {"/es-index.properties", "es-index.properties"};
  private static final String[] FOLDERS = {".", "./conf/", "../conf/", "./bin/"};

  private final String dcName =
      DatabaseDescriptor.getEndpointSnitch().getDatacenter(FBUtilities.getBroadcastAddress());
  private final String rackName =
      DatabaseDescriptor.getEndpointSnitch().getRack(FBUtilities.getBroadcastAddress());
  private final String indexName;
  private Map<String, String> options = new HashMap<>();

  public OptionReader(@Nonnull String indexName, @Nonnull Map<String, String> options) {
    this.indexName = indexName;
    reload(options);
  }

  private static String findFile(@Nonnull String path, @Nonnull String... names) {
    for (String name : names) {
      @SuppressWarnings("resource")
      InputStream defaultFile = OptionReader.class.getResourceAsStream(name);
      if (defaultFile != null) {
        String foundFile = CLASSPATH_PREFIX + name;
        try {
          defaultFile.close();
        } catch (IOException e) {
          LOGGER.error("Can't close {}", name, e);
        }
        return foundFile;
      }

      File file = new File(path + name);
      if (file.exists()) {
        return file.getAbsolutePath();
      }
    }
    return null;
  }

  @Nonnull
  public Map<String, String> getOptions() {
    return options;
  }

  public boolean reload(@Nonnull Map<String, String> cassandraOptions) {
    Map<String, String> newOptions = new HashMap<>(cassandraOptions);

    Map<String, String> fileOptions = loadFromFile();
    if (fileOptions != null) {
      newOptions.putAll(fileOptions);
    }

    if (newOptions.equals(this.options)) {
      return false;
    } else {
      MapDifference<String, String> diff = Maps.difference(this.options, newOptions);
      LOGGER.warn(
          "Reloaded {} options changed: \n\tadded:{} \n\tremoved:{} \n\tchanged:{}",
          indexName,
          diff.entriesOnlyOnRight(),
          diff.entriesOnlyOnLeft(),
          diff.entriesDiffering());
      this.options = newOptions;
      return true;
    }
  }

  public boolean getBoolean(@Nonnull String key, boolean defValue) {
    return Boolean.parseBoolean(getString(key, String.valueOf(defValue)));
  }

  public int getInteger(@Nonnull String key, int defValue) {
    String value = getString(key, null);
    if (value == null) {
      return defValue;
    }

    try {
      return Integer.parseInt(value);
    } catch (NumberFormatException ex) {
      LOGGER.warn(
          "{} option {} has invalid value {} using default {}", indexName, key, value, defValue);
      return defValue;
    }
  }

  @Nullable
  public String getString(@Nonnull String key, @Nullable String defValue) {
    String value = get("<" + dcName + "." + rackName + ">." + key); // Try specific dc/rack
    if (value != null) {
      return value;
    }

    value =
        get(dcName + "." + rackName + "." + key); // Try specific dc/rack, keep GWE compatibility
    if (value != null) {
      return value;
    }

    value = get("<" + dcName + ">." + key); // Try specific dc
    if (value != null) {
      return value;
    }

    value = get(dcName + "." + key); // Try specific dc, keep GWE compatibility
    if (value != null) {
      return value;
    }

    value = get(key);
    if (value != null) {
      return value;
    }

    return defValue;
  }

  @Nullable
  private String get(String key) {
    String value = System.getProperty(IndexConfig.ES_CONFIG_PREFIX + key); // Support for sysprops
    if (isBlank(value)) {
      value = System.getenv(IndexConfig.ES_CONFIG_PREFIX + key); // Support for env vars
    }

    if (isBlank(value)) {
      value =
          options.getOrDefault(
              key,
              options.get(key.replace('-', '.'))); // Try hyphen format then try in doted format
    }

    return isBlank(value) ? null : value;
  }

  @Nullable
  private Map<String, String> loadFromFile() {
    String cfgFile = System.getProperty(CFG_FILE_KEY);
    if (cfgFile == null) {
      cfgFile = System.getProperty(CFG_FILE_KEY.replace('-', '.'));
    }

    if (cfgFile == null) {
      for (String folder : FOLDERS) {
        cfgFile = findFile(folder, FILES);
        if (cfgFile != null) {
          LOGGER.info("Found default configuration file '{}'", cfgFile);
          break;
        }
      }
    }

    if (cfgFile == null) {
      return null;
    }

    boolean fromCp = cfgFile.startsWith(CLASSPATH_PREFIX);
    String filePath =
        fromCp ? cfgFile.substring(CLASSPATH_PREFIX.length(), cfgFile.length()) : cfgFile;

    Map<String, String> fileOptions = new HashMap<>();
    try (InputStream ios =
        fromCp
            ? this.getClass().getResourceAsStream(filePath)
            : new FileInputStream(new File(filePath))) {

      Properties props = new Properties();
      props.load(ios);

      for (Entry<Object, Object> en : props.entrySet()) {
        fileOptions.put(String.valueOf(en.getKey()), String.valueOf(en.getValue()));
      }

      return fileOptions;
    } catch (IOException e) {
      LOGGER.error("Can't read file '{}' {}", filePath, e.getMessage(), e);
      throw new RuntimeException("Index option file read exception", e);
    }
  }
}

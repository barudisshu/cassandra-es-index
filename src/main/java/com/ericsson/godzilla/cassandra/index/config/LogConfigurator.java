/*
 * Copyright Ericsson AB 2019 - All Rights Reserved.
 * The copyright to the computer program(s) herein is the property of Ericsson AB.
 * The programs may be used and/or copied only with written permission from Ericsson AB
 * or in accordance with the terms and conditions stipulated in the agreement/contract under which the program(s) have been supplied.
 */
package com.ericsson.godzilla.cassandra.index.config;

/**
 * Used to force Jest log levels that are too high by default
 *
 * <p>
 */
public class LogConfigurator {

  private static final org.slf4j.Logger LOGGER =
      org.slf4j.LoggerFactory.getLogger(LogConfigurator.class);

  private static final boolean DEBUG_LOGS = Boolean.getBoolean("jest.debug.logs");
  private static final String JEST_LEVEL = System.getProperty("jest.level", "INFO");
  private static final String[] LOGGERS = {
    "io.searchbox",
    "org.apache.http",
    "org.apache.http.wire",
    "org.apache.http.impl.conn",
    "org.apache.http.impl.client",
    "org.apache.http.client"
  };

  public static void configure() {
    if (!DEBUG_LOGS) {
      try {
        for (String name : LOGGERS) {
          org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(name);

          // using class name won't cause problems with classloader
          switch (logger.getClass().getName()) {
            case "org.apache.log4j.Logger":
              ((org.apache.log4j.Logger) logger)
                  .setLevel(org.apache.log4j.Level.toLevel(JEST_LEVEL));
              break;

            case "org.slf4j.helpers.NOPLogger": // Nothing to configure on NOP logger
              break;

            case "org.apache.logging.slf4j.Log4jLogger":
              reconfigureLog4j(logger.getName());
              break;

            case "ch.qos.logback.classic.Logger":
              ((ch.qos.logback.classic.Logger) logger)
                  .setLevel(ch.qos.logback.classic.Level.toLevel(JEST_LEVEL));
              break;

            default:
              //
          }
        }
      } catch (Throwable ex) {
        LOGGER.warn("Can't reconfigure logs {}", ex.getMessage());
      }
    }
  }

  private static void reconfigureLog4j(String loggerName) {
    try {
      org.apache.log4j.LogManager.getLogger(loggerName)
          .setLevel(org.apache.log4j.Level.toLevel(JEST_LEVEL));
    } catch (Throwable ignore) {
      // can fail if log4j1 is not on the CP
    }

    try {
      Class<?> levelClass = Class.forName("org.apache.logging.log4j.Level");
      Object level = levelClass.getMethod("toLevel", String.class).invoke(null, JEST_LEVEL);
      Class.forName("org.apache.logging.log4j.core.config.Configurator")
          .getMethod("setLevel", String.class, levelClass)
          .invoke(null, loggerName, level);
    } catch (Throwable ignore) {
      // can fail if log4j2 is not on the CP
    }
  }
}

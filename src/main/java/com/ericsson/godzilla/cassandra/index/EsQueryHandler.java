/*
 * Copyright Ericsson AB 2019 - All Rights Reserved.
 * The copyright to the computer program(s) herein is the property of Ericsson AB.
 * The programs may be used and/or copied only with written permission from Ericsson AB
 * or in accordance with the terms and conditions stipulated in the agreement/contract under which the program(s) have been supplied.
 */
package com.ericsson.godzilla.cassandra.index;

import com.ericsson.godzilla.cassandra.index.requests.EsRequestExecutionException;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.cql3.statements.BatchStatement;
import org.apache.cassandra.cql3.statements.IndexTarget;
import org.apache.cassandra.cql3.statements.ParsedStatement;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.ReadQuery;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.exceptions.ExceptionCode;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.MD5Digest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.cassandra.cql3.statements.RequestValidations.checkNotNull;

/** Payload is ignore here because we want to use elasticsearch expression */
public class EsQueryHandler implements QueryHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(EsQueryHandler.class);

  private static Method processResults;

  static {
    try {
      processResults =
          SelectStatement.class.getDeclaredMethod(
              "processResults", PartitionIterator.class, QueryOptions.class, int.class, int.class);
      processResults.setAccessible(true);
    } catch (NoSuchMethodException e) {
      LOGGER.error("cassandra-all does not exists or with compatibility version");
    }
  }

  public static void activate() {
    LOGGER.debug(
        "Current query handler is \u001B[34m{}\u001B[0m",
        ClientState.getCQLQueryHandler().getClass().getSimpleName());
    if (!(ClientState.getCQLQueryHandler() instanceof EsQueryHandler)) {
      try {
        Field field = ClientState.class.getDeclaredField("cqlQueryHandler");
        field.setAccessible(true);
        Field modifiersField = Field.class.getDeclaredField("modifiers");
        modifiersField.setAccessible(true);
        modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);
        field.set(null, new EsQueryHandler());
      } catch (Exception e) {
        LOGGER.error("Unable to set Elasticsearch CQL query handler");
      }
    }
  }

  @Override
  public ResultMessage process(
      String query,
      QueryState state,
      QueryOptions options,
      Map<String, ByteBuffer> customPayload,
      long queryStartNanoTime)
      throws RequestExecutionException, RequestValidationException {
    ParsedStatement.Prepared p = QueryProcessor.getStatement(query, state.getClientState());
    options.prepare(p.boundNames);
    CQLStatement prepared = p.statement;
    if (prepared.getBoundTerms() != options.getValues().size()) {
      throw new InvalidRequestException("Invalid amount of bind variables");
    }
    if (!state.getClientState().isInternal) {
      QueryProcessor.metrics.regularStatementsExecuted.inc();
    }
    return processStatement(prepared, state, options, queryStartNanoTime);
  }

  @Override
  public ResultMessage.Prepared prepare(
      String query, QueryState state, Map<String, ByteBuffer> customPayload)
      throws RequestValidationException {
    return QueryProcessor.instance.prepare(query, state);
  }

  @Override
  public ParsedStatement.Prepared getPrepared(MD5Digest id) {
    return QueryProcessor.instance.getPrepared(id);
  }

  @Override
  public ParsedStatement.Prepared getPreparedForThrift(Integer id) {
    return QueryProcessor.instance.getPreparedForThrift(id);
  }

  @Override
  public ResultMessage processPrepared(
      CQLStatement statement,
      QueryState state,
      QueryOptions options,
      Map<String, ByteBuffer> customPayload,
      long queryStartNanoTime)
      throws RequestExecutionException, RequestValidationException {
    QueryProcessor.metrics.preparedStatementsExecuted.inc();
    return processStatement(statement, state, options, queryStartNanoTime);
  }

  @Override
  public ResultMessage processBatch(
      BatchStatement statement,
      QueryState state,
      BatchQueryOptions options,
      Map<String, ByteBuffer> customPayload,
      long queryStartNanoTime)
      throws RequestExecutionException, RequestValidationException {
    return QueryProcessor.instance.processBatch(
        statement, state, options, customPayload, queryStartNanoTime);
  }

  private ResultMessage processStatement(
      CQLStatement statement, QueryState state, QueryOptions options, long queryStartNanoTime)
      throws RequestExecutionException {
    LOGGER.trace("Process {} @CL.{}", statement, options.getConsistency());
    ClientState clientState = state.getClientState();
    statement.checkAccess(clientState);
    statement.validate(clientState);

    // Intercept elasticsearch syntax searches
    if (statement instanceof SelectStatement) {
      SelectStatement select = (SelectStatement) statement;
      Map<RowFilter.Expression, EsSecondaryIndex> expressions = elasticExpressions(select, options);
      LOGGER.debug(
          "Expression query list {}",
          expressions.values().stream().distinct().collect(Collectors.toList()));
      if (!expressions.isEmpty()) {
        return executeElasticQuery(select, state, options, expressions, queryStartNanoTime);
      }
    }
    return execute(statement, state, options, queryStartNanoTime);
  }

  private Map<RowFilter.Expression, EsSecondaryIndex> elasticExpressions(
      SelectStatement select, QueryOptions options) {
    Map<RowFilter.Expression, EsSecondaryIndex> map = new LinkedHashMap<>();
    List<RowFilter.Expression> expressions = select.getRowFilter(options).getExpressions();
    ColumnFamilyStore cfs =
        Keyspace.open(select.keyspace()).getColumnFamilyStore(select.columnFamily());
    Set<Index> indexes =
        cfs.indexManager.listIndexes().stream()
            .filter(e -> e instanceof EsSecondaryIndex)
            .collect(Collectors.toSet());
    for (RowFilter.Expression expression : expressions) {
      if (expression instanceof RowFilter.CustomExpression) {
        String clazz =
            ((RowFilter.CustomExpression) expression)
                .getTargetIndex()
                .options
                .get(IndexTarget.CUSTOM_INDEX_OPTION_NAME);
        if (clazz.equals(Index.class.getCanonicalName())) {
          EsSecondaryIndex index =
              (EsSecondaryIndex)
                  cfs.indexManager.getIndex(
                      ((RowFilter.CustomExpression) expression).getTargetIndex());
          map.put(expression, index);
        } else {
          indexes.stream()
              .filter(i -> i.supportsExpression(expression.column(), expression.operator()))
              .map(k -> (EsSecondaryIndex) k)
              .forEach(e -> map.put(expression, e));
        }
      }
    }
    return map;
  }

  private ResultMessage execute(
      CQLStatement statement, QueryState state, QueryOptions options, long queryStartNanoTime) {
    ResultMessage result = statement.execute(state, options, queryStartNanoTime);
    if (result == null) return new ResultMessage.Void();
    else return result;
  }

  private ResultMessage executeElasticQuery(
      SelectStatement select,
      QueryState state,
      QueryOptions options,
      Map<RowFilter.Expression, EsSecondaryIndex> expressions,
      long queryStartNanoTime)
      throws RequestExecutionException {
    if (expressions.size() > 1) {
      throw new InvalidRequestException(
          "Elasticsearch index only supports one search expression per query.");
    }

    // Validate expression
    Map.Entry<RowFilter.Expression, EsSecondaryIndex> entry =
        expressions.entrySet().stream().findFirst().get();
    RowFilter.Expression expression = entry.getKey();
    EsSecondaryIndex index = entry.getValue();
    String queryString = null;
    try {
      queryString = ByteBufferUtil.string(expression.getIndexValue(), UTF_8);
    } catch (CharacterCodingException e) {
      throw new EsRequestExecutionException(ExceptionCode.INVALID, e.getMessage());
    }
    index.esIndex.validate(queryString);

    // Get paging info
    int now = FBUtilities.nowInSeconds();
    int limit = select.getLimit(options);
    int userPerPartitionLimit = select.getPerPartitionLimit(options);
    int page = options.getPageSize();

    // Take control of paging if there is paging and the query requires post processing
    ConsistencyLevel consistency = options.getConsistency();
    checkNotNull(consistency, "Invalid empty consistency level");
    consistency.validateForRead(select.keyspace());

    ReadQuery query = select.getQuery(options, now, limit, userPerPartitionLimit, page);

    try (PartitionIterator data =
        query.execute(consistency, state.getClientState(), queryStartNanoTime)) {
      return (ResultMessage.Rows) processResults.invoke(select, data, options, now, page);
    } catch (IllegalAccessException | InvocationTargetException e) {
      throw new EsRequestExecutionException(ExceptionCode.INVALID, e.getMessage());
    }
  }
}

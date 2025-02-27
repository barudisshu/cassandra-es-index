/*
 * Copyright Ericsson AB 2019 - All Rights Reserved.
 * The copyright to the computer program(s) herein is the property of Ericsson AB.
 * The programs may be used and/or copied only with written permission from Ericsson AB
 * or in accordance with the terms and conditions stipulated in the agreement/contract under which the program(s) have been supplied.
 */
package com.ericsson.godzilla.cassandra.index;

import com.ericsson.godzilla.cassandra.index.config.IndexConfig;
import com.ericsson.godzilla.cassandra.index.config.IndexConfiguration;
import com.ericsson.godzilla.cassandra.index.config.LogConfigurator;
import com.ericsson.godzilla.cassandra.index.indexers.*;
import com.google.common.base.Stopwatch;
import com.google.common.base.Strings;
import com.google.gson.JsonObject;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.statements.IndexTarget;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.IndexRegistry;
import org.apache.cassandra.index.transactions.IndexTransaction;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableFlushObserver;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.service.MigrationManager;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.concurrent.OpOrder.Group;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.security.SecureRandom;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

import static com.ericsson.godzilla.cassandra.index.DefaultIndexManager.INDEX_POSTFIX;
import static com.ericsson.godzilla.cassandra.index.JsonUtils.unQuote;
import static com.ericsson.godzilla.cassandra.index.config.IndexConfig.ES_CONFIG_PREFIX;

/** Interesting read to adapt to Cassandra 3: http://www.doanduyhai.com/blog/?p=2058 */
public class EsSecondaryIndex implements Index {

  public static final boolean DEBUG_SHOW_VALUES =
      Boolean.getBoolean(ES_CONFIG_PREFIX + "show-values");
  /**
   * if this option is true it allows Cassandra to start even if index is not properly initialized
   */
  static final boolean START_WITH_FAILED_INDEX =
      Boolean.getBoolean(ES_CONFIG_PREFIX + "force-start");

  private static final Logger LOGGER = LoggerFactory.getLogger(EsSecondaryIndex.class);

  private static final String UPDATE = "#update#";
  private static final String GET_MAPPING = "#get_mapping#";
  private static final String PUT_MAPPING = "#put_mapping#";
  private static final String FAKE_ID = "FakeId";

  private static final boolean DEPENDS_ON_COLUMN_DEFINITION =
      false; // we support dynamic addition/removal of columns

  public final ColumnFamilyStore baseCfs;
  public final String indexColumnName;
  public final String name;
  final IndexConfig indexConfig;
  @Nonnull final IndexInterface esIndex;
  private final SecureRandom random = new SecureRandom();
  private final boolean isDummyMode;
  private IndexMetadata indexMetadata;
  private List<String> partitionKeysNames;
  private List<String> clusteringColumnsNames;
  private boolean hasClusteringColumns;
  private ConsistencyLevel readConsistencyLevel;
  private boolean skipLogReplay;
  private boolean skipNonLocalUpdates;
  private boolean discardNullValues;
  private boolean analyticMode;
  private boolean indexAvailableWhenBuilding;

  public EsSecondaryIndex(ColumnFamilyStore sourceCfs, IndexMetadata indexMetadata)
      throws Exception {
    synchronized (EsSecondaryIndex.class) { // we create one index at a time
      LogConfigurator.configure(); // disable the very verbose Apache client logs
      EsQueryHandler
          .activate(); // Sets this query handler as the Cassandra CQL query handler, replacing the
                       // previous one.

      this.baseCfs = sourceCfs;
      this.indexMetadata = indexMetadata;
      indexColumnName = unQuote(this.indexMetadata.options.get(IndexTarget.TARGET_OPTION_NAME));
      name = "EsSecondaryIndex [" + baseCfs.metadata.ksName + "." + this.indexMetadata.name + "]";
      indexConfig = new IndexConfiguration(name, indexMetadata.options);

      LOGGER.info("Creating {} with options {}", name, indexConfig.getIndexOptions());

      IndexInterface index;

      try {
        if (indexConfig.isDummyMode()) {
          LOGGER.warn("{} dummy mode enabled", name);
          index = new EsDummyIndex();
        } else {
          LOGGER.warn(
              "EsSecondaryIndex {} initializing #{}", name, Integer.toHexString(hashCode()));
          partitionKeysNames =
              Collections.unmodifiableList(CStarUtils.getPartitionKeyNames(baseCfs.metadata));
          clusteringColumnsNames =
              Collections.unmodifiableList(CStarUtils.getClusteringColumnsNames(baseCfs.metadata));

          LOGGER.debug(
              "ReadConsistencyLevel is '{}', skip startup log replay:{}, skip non local updates:{}",
              readConsistencyLevel,
              skipLogReplay,
              skipNonLocalUpdates);

          hasClusteringColumns = !clusteringColumnsNames.isEmpty();
          index =
              new ElasticIndex(
                  indexConfig,
                  baseCfs.metadata.ksName,
                  baseCfs.name,
                  partitionKeysNames,
                  clusteringColumnsNames);
          index.init();
          LOGGER.warn("Initialized {} ", name);
        }
      } catch (Exception ex) {
        if (START_WITH_FAILED_INDEX) {
          index = new EsDummyIndex();
          LOGGER.error("Index {} initialization failed, starting in dummy mode", name, ex);
        } else {
          throw ex;
        }
      }

      isDummyMode = index instanceof EsDummyIndex;
      esIndex = index;
      updateIndexConfigOptions();
    }
  }

  private void updateIndexConfigOptions() {
    readConsistencyLevel = indexConfig.getReadConsistencyLevel();
    skipLogReplay = indexConfig.isSkipLogReplay();
    skipNonLocalUpdates = indexConfig.isSkipNonLocalUpdates();
    discardNullValues = indexConfig.isDiscardNullValues();
    analyticMode = indexConfig.isAnalyticMode();
    indexAvailableWhenBuilding = indexConfig.isIndexAvailableWhenBuilding();
    esIndex.updateIndexConfigOptions();
  }

  /**
   * @param decoratedKey PK of the update
   * @param newRow the new version of the row
   * @param oldRow provided in case of updates, null for inserts
   * @param nowInSec time of the update
   */
  public void index(
      @Nonnull DecoratedKey decoratedKey, @Nonnull Row newRow, @Nullable Row oldRow, int nowInSec) {

    String id = ByteBufferUtil.bytesToHex(decoratedKey.getKey());
    Tracing.trace("ESI decoding row {}", id); // This is CQL "tracing on" support

    try {
      List<Pair<String, String>> partitionKeys =
          CStarUtils.getPartitionKeys(decoratedKey.getKey(), baseCfs.metadata);
      List<CellElement> elements = new ArrayList<>();

      for (Cell cell : newRow.cells()) {
        if (cell.isLive(nowInSec) || !discardNullValues) { // optionally ignore null values

          // Skip the cells with empty name (row marker) // looks like isEmpty() now (2.2?) returns
          // false with empty string
          String cellName = cell.column().name.toString();

          if (!Strings.isNullOrEmpty(cellName)) { // Skip the cells with empty name (row marker)
            CellElement element = new CellElement();
            element.name = cellName;

            if (hasClusteringColumns) {
              element.clusteringKeys =
                  CStarUtils.getClusteringKeys(newRow, baseCfs.metadata, clusteringColumnsNames);
            }

            if (CStarUtils.isCollection(cell)) {
              element.collectionValue = CStarUtils.getCollectionElement(cell);
            } else {
              element.value = CStarUtils.cellValueToString(cell);
            }
            elements.add(element);
          }
        }
      }

      if (elements.isEmpty()) {
        Tracing.trace("ESI skip empty update {} done", id);
        LOGGER.debug("{} skip empty update for row {}", name, id);

      } else {
        if (DEBUG_SHOW_VALUES) {
          LOGGER.debug("{} indexing row {} content:\u001B[33m{}\u001B[0m", name, id, newRow);
        } else {
          LOGGER.debug("{} indexing row {}", name, id);
        }

        Tracing.trace("ESI writing {} to ES index", id);
        esIndex.index(
            partitionKeys,
            elements,
            newRow.primaryKeyLivenessInfo().localExpirationTime(),
            oldRow == null);
        Tracing.trace("ESI index {} done", id);
      }

    } catch (Exception e) {
      LOGGER.error("{} can't index row {} {}", name, id, e);
      Tracing.trace("ESI error: can't index row {} {}", id, e.getMessage());

      throw new RuntimeException(e);
    }
  }

  public void delete(DecoratedKey decoratedKey) {
    String id = ByteBufferUtil.bytesToHex(decoratedKey.getKey());

    try {
      Token token = decoratedKey.getToken();
      if (CStarUtils.isOwner(baseCfs, token)) {
        LOGGER.trace("{} deleting {}", name, id);
      } else {
        LOGGER.trace("{} skipping {} because {} is not in our range", name, id, token);
        return;
      }

      esIndex.delete(CStarUtils.getPartitionKeys(decoratedKey.getKey(), baseCfs.metadata));
    } catch (Exception e) {
      LOGGER.error("{} can't delete row {} {}", name, id, e);
      throw new RuntimeException(e);
    }
  }

  boolean reloadSettings() {
    boolean changed = indexConfig.reload(indexMetadata.options);
    if (changed) {
      updateIndexConfigOptions();
    }
    return changed;
  }

  @Override
  public String toString() {
    return name;
  }

  @Override
  public IndexBuildingSupport getBuildTaskSupport() { // This is for rebuild command
    if (isDummyMode) {
      return null;
    }
    return (cfs, indexes, ssTables) -> new EsIndexBuilder(EsSecondaryIndex.this, ssTables);
  }

  @Override
  public Callable<?>
      getInitializationTask() { // This is done when starting Cassandra or when creating an index
                                // with CQL command
    return () -> {
      if (esIndex
          .isNewIndex()) { // FIXME will this rebuild all data since we only have ssTables for our
                           // replicas?
        if (indexAvailableWhenBuilding) {
          LOGGER.info("{} marking index as built while rebuilding is in progress", name);
          baseCfs.indexManager.markIndexBuilt(indexMetadata.name);
          new EsIndexBuilder(EsSecondaryIndex.this).build();
        } else {
          LOGGER.info("{} index rebuild completed, marking as built", name);
          new EsIndexBuilder(EsSecondaryIndex.this).build();
          baseCfs.indexManager.markIndexBuilt(indexMetadata.name);
        }
      } else {
        LOGGER.debug("{} already exists, nothing to rebuild", name);
        baseCfs.indexManager.markIndexBuilt(indexMetadata.name);
      }
      return null;
    };
  }

  @Override
  public IndexMetadata getIndexMetadata() {
    return indexMetadata;
  }

  @Override
  public Callable<?> getMetadataReloadTask(IndexMetadata indexMetadata) {
    return this::reloadSettings;
  }

  @Override
  public void register(IndexRegistry registry) {
    LOGGER.info("Registering {} against {}", name, registry);
    registry.registerIndex(this);
  }

  @Override
  public Optional<ColumnFamilyStore> getBackingTable() {
    return Optional.empty(); // We don't use a CFS to store index data
  }

  @Override
  public Callable<?> getBlockingFlushTask() {
    return esIndex::flush;
  }

  @Override
  public Callable<?> getInvalidateTask() {
    return esIndex::drop;
  }

  @Override
  public Callable<?> getTruncateTask(long truncatedAt) {
    return esIndex::truncate;
  }

  @Override
  public boolean shouldBuildBlocking() {
    return false; // We don't want to block table/index access while it's (re)building
  }

  @Override
  public SSTableFlushObserver getFlushObserver(Descriptor descriptor, OperationType opType) {
    return null; // Don't think we care about table flushes
  }

  @Override
  public boolean dependsOn(ColumnDefinition column) {
    return DEPENDS_ON_COLUMN_DEFINITION;
  }

  @Override
  public boolean supportsExpression(ColumnDefinition column, Operator operator) {
    return true; // We support any kind of C* expressions because it's actually in the ES query.
  }

  @Override
  public AbstractType<?> customExpressionValueType() {
    return UTF8Type.instance; // we hope support custom expressions, for not to increasing column.
  }

  @Override
  public RowFilter getPostIndexQueryFilter(RowFilter filter) {
    return RowFilter.NONE; // we don't have further filtering to do
  }

  @Override
  public long getEstimatedResultRows() {
    /* From http://www.doanduyhai.com/blog/?p=2058#sasi_read_path
     * As a result, every search with SASI currently always hit the same node, which is the node responsible for the first token range
     * on the cluster. Subsequent rounds of query (if any) will spread out to other nodes eventually
     */

    // Trying to be smart here, if each node returns random negative we'll get load spread and will
    // still be selected over native index
    return -Math.abs(random.nextLong());
  }

  @Override
  public Indexer indexerFor(
      DecoratedKey key,
      PartitionColumns columns,
      int nowInSec,
      Group opGroup,
      IndexTransaction.Type txType) {
    if (isDummyMode) {
      return NoOpIndexer.INSTANCE; // Dummy mode
    }

    if (skipLogReplay && !StorageService.instance.isInitialized()) {
      if (LOGGER.isTraceEnabled()) {
        String id = ByteBufferUtil.bytesToHex(key.getKey());
        LOGGER.trace(
            "Index {} skipping index {} storage service is not initialized yet (commit log replay)",
            name,
            id);
      }
      return NoOpIndexer.INSTANCE;
    }

    Token token = key.getToken();
    if (skipNonLocalUpdates && !CStarUtils.isOwner(baseCfs, token)) {
      if (LOGGER.isTraceEnabled()) {
        String id = ByteBufferUtil.bytesToHex(key.getKey());
        LOGGER.trace(
            "Index {} skipping update on {} because {} is not in our range", name, id, token);
      }
      return NoOpIndexer.INSTANCE;
    }

    return new EsIndexer(this, key, nowInSec, !analyticMode);
  }

  @Override
  public BiFunction<PartitionIterator, ReadCommand, PartitionIterator> postProcessorFor(
      ReadCommand command) {
    return NoOpPartitionIterator.INSTANCE;
  }

  @Override
  public Searcher searcherFor(final ReadCommand command) {
    return executionController -> search(command);
  }

  @Override
  public void validate(PartitionUpdate update) throws InvalidRequestException {
    // nothing to check for now
  }

  @Override
  public void validate(ReadCommand command) throws InvalidRequestException {
    String queryString = CStarUtils.queryString(command);
    // don't validate commands
    if (!queryString.startsWith(UPDATE)
        && !queryString.startsWith(GET_MAPPING)
        && !queryString.startsWith(PUT_MAPPING)) {
      LOGGER.trace(
          "Index {} validate query: {}",
          name,
          queryString); // reducing level because we'll see it as search later
      esIndex.validate(queryString);
    }
  }

  public UnfilteredPartitionIterator search(ReadCommand command) {
    final Stopwatch time = Stopwatch.createStarted();
    final String searchId = UUID.randomUUID().toString();
    final String queryString = CStarUtils.queryString(command);

    if (queryString.startsWith(UPDATE)) {
      handleUpdateCommand(queryString.substring(UPDATE.length(), queryString.length() - 1));
      return EmptyIterators.unfilteredPartition(command.metadata(), command.isForThrift());
    }

    if (!(command instanceof PartitionRangeReadCommand)) {
      LOGGER.error(
          "Index {} class type {} is not supported for searches",
          name,
          command.getClass().getName());
      throw new UnsupportedOperationException(
          command.getClass().getName() + " is not supported for searches");
    }

    PartitionRangeReadCommand readCommand = (PartitionRangeReadCommand) command;

    if (queryString.startsWith(GET_MAPPING)) {
      return getMapping(readCommand, searchId);
    }

    if (queryString.startsWith(PUT_MAPPING)) {
      return putMapping(readCommand, queryString);
    }

    LOGGER.debug("Index {} search query {} '{}'", name, searchId, queryString);
    Tracing.trace(
        "ESI {} Searching '{}'", searchId, queryString); // This is CQL "tracing on" support

    // Extract query metadata if any
    QueryMetaData queryMetaData = new QueryMetaData(queryString);
    SearchResult searchResult = esIndex.search(queryMetaData);

    LOGGER.debug(
        "{} {} Found {} matching ES docs in {}ms",
        name,
        searchId,
        searchResult.items.size(),
        time.elapsed(TimeUnit.MILLISECONDS));
    Tracing.trace(
        "ESI {} Found {} matching ES docs in {}ms",
        searchId,
        searchResult.items.size(),
        time.elapsed(TimeUnit.MILLISECONDS));

    if (searchResult.items.isEmpty()) {
      return EmptyIterators.unfilteredPartition(command.metadata(), command.isForThrift());
    }

    fillPartitionAndClusteringKeys(searchResult.items);

    Token start = readCommand.dataRange().keyRange().left.getToken();
    Token stop = readCommand.dataRange().keyRange().right.getToken();

    if (!start.equals(stop)) { // Do we have token ranges to filter out ?
      LOGGER.info(
          "Range queries will result in multiple ES queries, add 'and token(pk)=rnd.long' to your query");

      /* We must only load a row if its DecoratedKey is within the range of requested tokens. Note
       * that the same node can receive several requests for the same search but with different
       * ranges. If filtering is not done it will return duplicates. Drawback is that ES query is
       * sent several times for the same search. Also it means ordering might not work as expected.*/
      searchResult.items.removeIf(
          result ->
              !readCommand
                  .dataRange()
                  .keyRange()
                  .contains(baseCfs.getPartitioner().decorateKey(result.partitionKey)));
    }

    if (queryMetaData.loadRows()) {
      return new StreamingPartitionIterator(this, searchResult, readCommand, searchId);
    } else {
      return new FakePartitionIterator(this, searchResult, readCommand, searchId);
    }
  }

  private String indexName() {
    return baseCfs.keyspace.getName() + "_" + baseCfs.name.toLowerCase() + INDEX_POSTFIX;
  }

  private UnfilteredPartitionIterator getMapping(
      PartitionRangeReadCommand readCommand, String searchId) {
    LOGGER.info("Get index mapping for {}", baseCfs);

    JsonObject mapping = esIndex.getMapping(indexName()).metadata.getAsJsonObject();
    SearchResultRow fakeRow = new SearchResultRow(new String[] {FAKE_ID}, mapping);
    fakeRow.partitionKey = ByteBufferUtil.bytes(FAKE_ID);
    SearchResult searchResult = new SearchResult(Collections.singletonList(fakeRow), mapping);
    return new FakePartitionIterator(this, searchResult, readCommand, searchId);
  }

  private UnfilteredPartitionIterator putMapping(
      PartitionRangeReadCommand readCommand, String queryString) {
    LOGGER.info("Put index mapping {}", queryString);

    String additionalMapping =
        queryString.substring(PUT_MAPPING.length(), queryString.length() - 1);
    esIndex.putMapping(indexName(), additionalMapping);
    return EmptyIterators.unfilteredPartition(readCommand.metadata(), readCommand.isForThrift());
  }

  private void handleUpdateCommand(String newSettings) {
    try {
      updateSettings(newSettings); // Update in Cassandra index options
      boolean changed =
          reloadSettings(); // Apply new settings and merge them with es-index.properties
      if (changed) {
        esIndex.settingsUpdated(); // Notify the ES index to create new segments
      }
    } catch (IOException e) {
      LOGGER.error("Index {} update setting error: {}", name, e.getMessage(), e);
    }
  }

  private void fillPartitionAndClusteringKeys(List<SearchResultRow> searchResultRows) {
    for (SearchResultRow searchResultRow : searchResultRows) {
      String[] rawKey = searchResultRow.primaryKey;
      final String[] partitionKeys;
      final String[] clusteringKeys;

      // separate partition and clustering keys
      if (hasClusteringColumns) {
        clusteringKeys = new String[clusteringColumnsNames.size()];
        partitionKeys = new String[partitionKeysNames.size()];

        int pkPos = 0;
        int ckPos = 0;
        for (String key : rawKey) {
          if (pkPos < partitionKeysNames.size()) {
            partitionKeys[pkPos] = key;
          } else {
            clusteringKeys[ckPos] = key;
            ckPos++;
          }
          pkPos++;
        }
      } else {
        partitionKeys = rawKey;
        clusteringKeys = null;
      }

      searchResultRow.partitionKey = CStarUtils.getPartitionKeys(partitionKeys, baseCfs.metadata);
      searchResultRow.clusteringKeys = clusteringKeys;
    }
  }

  private void updateSettings(String settings) throws IOException {
    LOGGER.info("Update {} settings to '{}'", name, settings);
    Tracing.trace("Update {} settings to '{}'", name, settings); // This is CQL "tracing on" support
    Map<String, String> options = JsonUtils.jsonStringToStringMap(settings);

    // check options
    if (!options.containsKey(IndexTarget.CUSTOM_INDEX_OPTION_NAME)
        || !options.containsKey(IndexTarget.TARGET_OPTION_NAME)) {
      LOGGER.warn("We do not allow to change options class_name and target");
    }
    options.put(
        IndexTarget.CUSTOM_INDEX_OPTION_NAME,
        indexMetadata.options.get(IndexTarget.CUSTOM_INDEX_OPTION_NAME));
    options.put(
        IndexTarget.TARGET_OPTION_NAME, indexMetadata.options.get(IndexTarget.TARGET_OPTION_NAME));

    CFMetaData newMetaData = baseCfs.metadata.copy();
    IndexMetadata newIndexMetadata =
        IndexMetadata.fromSchemaMetadata(indexMetadata.name, indexMetadata.kind, options);
    newMetaData.indexes(newMetaData.getIndexes().replace(newIndexMetadata));

    indexMetadata = newIndexMetadata; // assume update will work, helps for UTs
    MigrationManager.announceColumnFamilyUpdate(newMetaData, false);
  }

  public ConsistencyLevel getReadConsistency() {
    return readConsistencyLevel;
  }
}

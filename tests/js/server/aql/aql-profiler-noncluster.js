 /*jshint globalstrict:true, strict:true, esnext: true */

"use strict";

////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2018 ArangoDB GmbH, Cologne, Germany
///
/// Licensed under the Apache License, Version 2.0 (the "License");
/// you may not use this file except in compliance with the License.
/// You may obtain a copy of the License at
///
///     http://www.apache.org/licenses/LICENSE-2.0
///
/// Unless required by applicable law or agreed to in writing, software
/// distributed under the License is distributed on an "AS IS" BASIS,
/// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
/// See the License for the specific language governing permissions and
/// limitations under the License.
///
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
///
/// @author Tobias Gödderz
////////////////////////////////////////////////////////////////////////////////

// contains common code for aql-profiler* tests
const profHelper = require("@arangodb/testutils/aql-profiler-test-helper");

const _ = require('lodash');
const db = require('@arangodb').db;
const jsunity = require("jsunity");
const internal = require("internal");

////////////////////////////////////////////////////////////////////////////////
/// @file test suite for AQL tracing/profiling: noncluster tests
/// Contains tests for EnumerateCollectionBlock, IndexBlock and TraversalBlock.
/// The main test suite (including comments) is in aql-profiler.js.
////////////////////////////////////////////////////////////////////////////////

function ahuacatlProfilerTestSuite () {

  // import some names from profHelper directly into our namespace:
  const colName = profHelper.colName;
  const edgeColName = profHelper.edgeColName;
  const viewName = profHelper.viewName;
  const defaultBatchSize = profHelper.defaultBatchSize;

  const { AsyncNode, CalculationNode, CollectNode, DistributeNode, EnumerateCollectionNode,
    EnumerateListNode, EnumerateViewNode, FilterNode, GatherNode, IndexNode,
    InsertNode, LimitNode, MaterializeNode, MutexNode, NoResultsNode, RemoteNode, RemoveNode, ReplaceNode,
    ReturnNode, ScatterNode, ShortestPathNode, SingletonNode, SortNode,
    TraversalNode, UpdateNode, UpsertNode } = profHelper;

  const { AsyncBlock, CalculationBlock, CountCollectBlock, DistinctCollectBlock,
    EnumerateCollectionBlock, EnumerateListBlock, FilterBlock,
    HashedCollectBlock, IndexBlock, LimitBlock, MaterializeBlock, MutexBlock, NoResultsBlock, RemoteBlock,
    ReturnBlock, ShortestPathBlock, SingletonBlock, SortBlock,
    SortedCollectBlock, SortingGatherBlock, TraversalBlock,
    UnsortingGatherBlock, RemoveBlock, InsertBlock, UpdateBlock, ReplaceBlock,
    UpsertBlock, ScatterBlock, DistributeBlock, IResearchViewUnorderedBlock,
    IResearchViewBlock, IResearchViewOrderedBlock } = profHelper;

  return {

    tearDown : function () {
      db._drop(colName);
      db._drop(edgeColName);
      db._dropView(viewName);
    },
    
    testMaterializeBlock: function () {
      const col = db._create(colName);
      col.ensureIndex({ type: "persistent", fields: ["value", "other", "more"]});
      
      const bind = () => ({'@col': colName});
      const prepare = (rows) => {
        col.truncate({ compact: false });
        col.insert(_.range(1, rows + 1).map((i) => ({value: i})));
      };
      const query = `FOR d IN @@col FILTER d.value <= 10 SORT d.more LIMIT 3 RETURN d`;

      const genNodeList = (rows, batches) => {
        return [
          {type: SingletonBlock, calls: 1, items: 1, filtered: 0},
          {type: IndexBlock, calls: 1, items: Math.min(rows, 10), filtered: 0},
          {type: SortBlock, calls: 1, items: Math.min(rows, 3), filtered: 0},
          {type: LimitBlock, calls: 1, items: Math.min(rows, 3), filtered: 0},
          {type: MaterializeBlock, calls: 1, items: Math.min(rows, 3), filtered: 0},
          {type: ReturnBlock, calls: 1, items: Math.min(rows, 3), filtered: 0}
        ];
      };
      profHelper.runDefaultChecks(
        {query, genNodeList, prepare, bind}
      );
    },

    testMaterializeBlockThatFilters: function () {
      if (!internal.debugCanUseFailAt()) {
        return;
      }

      try {
        const col = db._create(colName);
        col.ensureIndex({ type: "persistent", fields: ["value", "other", "more"]});
        
        const bind = () => ({'@col': colName});
        const prepare = (rows) => {
          col.truncate({ compact: false });
          col.insert(_.range(1, rows + 1).map((i) => ({value: i})));
        };
        const query = `FOR d IN @@col FILTER d.value <= 10 SORT d.more LIMIT 3 RETURN d`;

        const genNodeList = (rows, batches) => {
          return [
            {type: SingletonBlock, calls: 1, items: 1, filtered: 0},
            {type: IndexBlock, calls: 1, items: Math.min(rows, 10), filtered: 0},
            {type: SortBlock, calls: 1, items: Math.min(rows, 3), filtered: 0},
            {type: LimitBlock, calls: 1, items: Math.min(rows, 3), filtered: 0},
            {type: MaterializeBlock, calls: 1, items: 0, filtered: Math.min(rows, 3)},
            {type: ReturnBlock, calls: 1, items: 0, filtered: 0}
          ];
        };
        
        internal.debugSetFailAt("MaterializeExecutor::all_fail_and_count");
        profHelper.runDefaultChecks(
          {query, genNodeList, prepare, bind}
        );
      } finally {
        // clear failure points!
        internal.debugClearFailAt();
      }
    },

    testEnumerateCollectionBlock: function () {
      const col = db._create(colName);
      const prepare = (rows) => {
        col.truncate({ compact: false });
        col.insert(_.range(1, rows + 1).map((i) => ({value: i})));
      };
      const bind = () => ({'@col': colName});
      const query = `FOR d IN @@col RETURN d.value`;

      const genNodeList = (rows, batches) => {
        return [
          {type: SingletonBlock, calls: 1, items: 1, filtered: 0},
          {type: EnumerateCollectionBlock, calls: batches, items: rows, filtered: 0},
          {type: CalculationBlock, calls: batches, items: rows, filtered: 0},
          {type: ReturnBlock, calls: batches, items: rows, filtered: 0}
        ];
      };
      profHelper.runDefaultChecks(
        {query, genNodeList, prepare, bind}
      );
    },
    
    testEnumerateCollectionBlockWithFilter: function () {
      const col = db._create(colName);
      const prepare = (rows) => {
        col.truncate({ compact: false });
        col.insert(_.range(1, rows + 1).map((i) => ({value: i})));
      };
      const bind = () => ({'@col': colName});
      const query = `FOR d IN @@col FILTER d.value <= 10 RETURN d`;

      const genNodeList = (rows, batches) => {
        return [
          {type: SingletonBlock, calls: 1, items: 1, filtered: 0},
          {type: EnumerateCollectionBlock, calls: 1, items: Math.min(rows, 10), filtered: rows <= 10 ? 0 : rows - 10},
          {type: ReturnBlock, calls: 1, items: Math.min(rows, 10), filtered: 0}
        ];
      };
      profHelper.runDefaultChecks(
        {query, genNodeList, prepare, bind}
      );
    },

    testFilterBlock: function () {
      const col = db._create(colName);
      const prepare = (rows) => {
        col.truncate({ compact: false });
        col.insert(_.range(1, rows + 1).map((i) => ({value: i})));
      };
      const bind = () => ({'@col': colName});
      const query = `FOR d IN @@col FILTER d.value <= 10 RETURN d`;

      const genNodeList = (rows, batches) => {
        return [
          {type: SingletonBlock, calls: 1, items: 1, filtered: 0},
          {type: EnumerateCollectionBlock, calls: batches, items: rows, filtered: 0},
          {type: CalculationBlock, calls: batches, items: rows, filtered: 0},
          {type: FilterBlock, calls: 1, items: Math.min(rows, 10), filtered: rows <= 10 ? 0 : rows - 10},
          {type: ReturnBlock, calls: 1, items: Math.min(rows, 10), filtered: 0}
        ];
      };
      profHelper.runDefaultChecks(
        {query, genNodeList, prepare, bind, options: { optimizer: { rules: ["-move-filters-into-enumerate"] } }}
      );
    },

////////////////////////////////////////////////////////////////////////////////
/// @brief test IndexBlock
////////////////////////////////////////////////////////////////////////////////
    
    testIndexBlock1 : function () {
      const col = db._create(colName);
      col.ensureIndex({ type: "hash", fields: [ "value" ] });
      const prepare = (rows) => {
        col.truncate({ compact: false });
        col.insert(_.range(1, rows + 1).map((i) => ({value: i})));
      };
      const bind = (rows) => ({'@col': colName, rows});
      const query = `FOR i IN 1..@rows FOR d IN @@col FILTER i == d.value RETURN d.value`;

      const genNodeList = (rows, batches) => {
        // IndexBlock returns HASMORE when asked for the exact number of items
        // it has left. This could be improved.
        const optimalBatches = Math.ceil(rows / defaultBatchSize);
        const maxIndexBatches = Math.floor(rows / defaultBatchSize) + 1;
        const indexBatches = [optimalBatches, maxIndexBatches];

        return [
          {type: SingletonBlock, calls: 1, items: 1, filtered: 0},
          {type: CalculationBlock, calls: 1, items: 1, filtered: 0},
          {type: EnumerateListBlock, calls: batches, items: rows, filtered: 0},
          {type: IndexBlock, calls: indexBatches, items: rows, filtered: 0},
          {type: CalculationBlock, calls: indexBatches, items: rows, filtered: 0},
          {type: ReturnBlock, calls: indexBatches, items: rows, filtered: 0}
        ];
      };
      profHelper.runDefaultChecks(
        {query, genNodeList, prepare, bind, options: { optimizer: { rules: ["-interchange-adjacent-enumerations"] } }}
      );
    },

////////////////////////////////////////////////////////////////////////////////
/// @brief test IndexBlock, asking for every third document
////////////////////////////////////////////////////////////////////////////////

    testIndexBlock2 : function () {
      const col = db._create(colName);
      col.ensureIndex({ type: "hash", fields: [ "value" ] });
      const prepare = (rows) => {
        col.truncate({ compact: false });
        col.insert(_.range(1, rows + 1).map((i) => ({value: i})));
      };
      const bind = (rows) => ({'@col': colName, rows});
      const query = `FOR i IN 0..FLOOR(@rows / 3)
        FOR d IN @@col
        FILTER i * 3 + 1 == d.value
        RETURN d.value`;

      const genNodeList = (rows) => {
        const enumRows = Math.floor(rows / 3) + 1;
        const enumBatches = Math.ceil(enumRows / defaultBatchSize);
        const indexRows = Math.ceil(rows / 3);

        const optimalBatches = Math.ceil(indexRows / defaultBatchSize);
        // IndexBlock returns HASMORE when asked for the exact number of items
        // it has left. This could be improved.
        const maxIndexBatches = Math.max(1, Math.floor(indexRows / defaultBatchSize) + 1);
        // Number of calls made to the index block. See comment for
        // maxIndexBatches. As of now, maxIndexBatches is exact, but we don't
        // want to fail when this improves.
        const indexBatches = [
          optimalBatches,
          maxIndexBatches
        ];

        return [
          {type: SingletonBlock, calls: 1, items: 1, filtered: 0},
          {type: CalculationBlock, calls: 1, items: 1, filtered: 0},
          {type: EnumerateListBlock, calls: enumBatches, items: enumRows, filtered: 0},
          {type: IndexBlock, calls: indexBatches, items: indexRows, filtered: 0},
          {type: CalculationBlock, calls: indexBatches, items: indexRows, filtered: 0},
          {type: ReturnBlock, calls: indexBatches, items: indexRows, filtered: 0}
        ];
      };
      profHelper.runDefaultChecks(
        {query, genNodeList, prepare, bind, options: { optimizer: { rules: ["-interchange-adjacent-enumerations"] } }}
      );
    },
    
    testIndexBlockWithProjection: function () {
      const col = db._create(colName);
      col.ensureIndex({ type: "hash", fields: [ "value" ] });
      const prepare = (rows) => {
        col.truncate({ compact: false });
        col.insert(_.range(1, rows + 1).map((i) => ({value: i})));
      };
      const bind = () => ({'@col': colName});
      const query = `FOR d IN @@col RETURN d.value`;

      const genNodeList = (rows, batches) => {
        return [
          {type: SingletonBlock, calls: 1, items: 1, filtered: 0},
          {type: IndexBlock, calls: batches, items: rows, filtered: 0},
          {type: CalculationBlock, calls: batches, items: rows, filtered: 0},
          {type: ReturnBlock, calls: batches, items: rows, filtered: 0}
        ];
      };
      profHelper.runDefaultChecks(
        {query, genNodeList, prepare, bind}
      );
    },
    
    testIndexBlockWithFilter: function () {
      const col = db._create(colName);
      col.ensureIndex({ type: "hash", fields: [ "value" ] });
      const prepare = (rows) => {
        col.truncate({ compact: false });
        col.insert(_.range(1, rows + 1).map((i) => ({value: i})));
      };
      const bind = () => ({'@col': colName});
      const query = `FOR d IN @@col FILTER d.value <= 10 RETURN d`;

      const genNodeList = (rows, batches) => {
        return [
          {type: SingletonBlock, calls: 1, items: 1, filtered: 0},
          {type: IndexBlock, calls: 1, items: Math.min(rows, 10), filtered: 0},
          {type: ReturnBlock, calls: 1, items: Math.min(rows, 10), filtered: 0}
        ];
      };
      profHelper.runDefaultChecks(
        {query, genNodeList, prepare, bind}
      );
    },

////////////////////////////////////////////////////////////////////////////////
/// @brief test TraversalBlock: traverse a tree
////////////////////////////////////////////////////////////////////////////////

    testTraversalBlock1: function () {
      const col = db._createDocumentCollection(colName);
      const edgeCol = db._createEdgeCollection(edgeColName);
      const prepare = (rows) => {
        profHelper.createBinaryTree(col, edgeCol, rows);
      };
      const query = `WITH @@vertexCol FOR v IN 0..@rows OUTBOUND @root @@edgeCol RETURN v`;
      const rootNodeId = `${colName}/1`;
      const bind = rows => ({
        rows: rows,
        root: rootNodeId,
        '@edgeCol': edgeColName,
        '@vertexCol': colName,
      });

      const genNodeList = (rows, batches) => {
        return [
          {type: SingletonBlock, calls: 1, items: 1, filtered: 0},
          {type: TraversalBlock, calls: rows % defaultBatchSize === 0 ? batches + 1 : batches, items: rows, filtered: 0},
          {type: ReturnBlock, calls: rows % defaultBatchSize === 0 ? batches + 1 : batches, items: rows, filtered: 0}
        ];
      };
      profHelper.runDefaultChecks(
        {query, genNodeList, prepare, bind}
      );
    },

////////////////////////////////////////////////////////////////////////////////
/// @brief test TraversalBlock: traverse ~half a tree
////////////////////////////////////////////////////////////////////////////////

    testTraversalBlock2: function () {
      const col = db._createDocumentCollection(colName);
      const edgeCol = db._createEdgeCollection(edgeColName);
      const prepare = (rows) => {
        profHelper.createBinaryTree(col, edgeCol, rows);
      };
      const query = `WITH @@vertexCol FOR v IN 0..@depth OUTBOUND @root @@edgeCol RETURN v`;
      const rootNodeId = `${colName}/1`;
      // actual tree depth:
      // const treeDepth = rows => Math.ceil(Math.log2(rows));
      // tree is perfect up to this depth:
      const maxFullDepth = rows => Math.floor(Math.log2(rows));
      // substract one to get rid of ~half the nodes, but never go below 0
      const depth = rows => Math.max(0, maxFullDepth(rows) - 1);
      const bind = rows => ({
        depth: depth(rows),
        root: rootNodeId,
        '@edgeCol': edgeColName,
        '@vertexCol': colName,
      });
      const visitedNodes = rows => Math.pow(2, depth(rows)+1) - 1;

      const genNodeList = (rows, batches) => {
        rows = visitedNodes(rows);
        batches = Math.ceil(rows / defaultBatchSize);
        return [
          {type: SingletonBlock, calls: 1, items: 1, filtered: 0},
          {type: TraversalBlock, calls: batches, items: rows, filtered: 0},
          {type: ReturnBlock, calls: batches, items: rows, filtered: 0}
        ];
      };
      profHelper.runDefaultChecks(
        {query, genNodeList, prepare, bind}
      );
    },

////////////////////////////////////////////////////////////////////////////////
/// @brief test TraversalBlock: skip ~half a tree
////////////////////////////////////////////////////////////////////////////////

    testTraversalBlock3: function () {
      const col = db._createDocumentCollection(colName);
      const edgeCol = db._createEdgeCollection(edgeColName);
      const prepare = (rows) => {
        profHelper.createBinaryTree(col, edgeCol, rows);
      };
      const query = `WITH @@vertexCol FOR v IN @depth..@rows OUTBOUND @root @@edgeCol RETURN v`;
      const rootNodeId = `${colName}/1`;
      // actual tree depth:
      // const treeDepth = rows => Math.ceil(Math.log2(rows));
      // tree is perfect up to this depth:
      const maxFullDepth = rows => Math.floor(Math.log2(rows));
      // substract one to leave ~half the nodes, but never go below 0
      const depth = rows => Math.max(0, maxFullDepth(rows) - 1);
      const bind = rows => ({
        rows: rows,
        depth: depth(rows),
        root: rootNodeId,
        '@edgeCol': edgeColName,
        '@vertexCol': colName,
      });
      const skippedNodes = rows => Math.pow(2, depth(rows)) - 1;
      const visitedNodes = rows => rows - skippedNodes(rows);

      const genNodeList = (rows, batches) => {
        rows = visitedNodes(rows);
        batches = Math.max(1, Math.ceil(rows / defaultBatchSize));
        return [
          {type: SingletonBlock, calls: 1, items: 1, filtered: 0},
          {type: TraversalBlock, calls: batches, items: rows, filtered: 0},
          {type: ReturnBlock, calls: batches, items: rows, filtered: 0}
        ];
      };
      profHelper.runDefaultChecks(
        {query, genNodeList, prepare, bind}
      );
    },
    
    ////////////////////////////////////////////////////////////////////////////////
    /// @brief test TraversalBlock: check filter result
    ////////////////////////////////////////////////////////////////////////////////
    
    testTraversalBlockWithFilter: function () {
      const col = db._createDocumentCollection(colName);
      const edgeCol = db._createEdgeCollection(edgeColName);
      const prepare = (rows) => {
        profHelper.createBinaryTree(col, edgeCol, rows);
      };
      const query = `WITH @@vertexCol FOR v, e, p IN @depth..@rows OUTBOUND @root @@edgeCol FILTER p.vertices[*].value ALL > 100000 RETURN v`;
      const rootNodeId = `${colName}/1`;
      // actual tree depth:
      // const treeDepth = rows => Math.ceil(Math.log2(rows));
      // tree is perfect up to this depth:
      const maxFullDepth = rows => Math.floor(Math.log2(rows));
      // substract one to leave ~half the nodes, but never go below 0
      const depth = rows => Math.max(0, maxFullDepth(rows) - 1);
      const bind = rows => ({
        rows: rows,
        depth: depth(rows),
        root: rootNodeId,
        '@edgeCol': edgeColName,
        '@vertexCol': colName,
      });
      const skippedNodes = rows => Math.pow(2, depth(rows)) - 1;
      const visitedNodes = rows => rows - skippedNodes(rows);

      const genNodeList = (rows, batches) => {
        // note: filter condition is never true
        rows = 0;
        batches = Math.max(1, Math.ceil(rows / defaultBatchSize));

        return [
          {type: SingletonBlock, calls: 1, items: 1, filtered: 0},
          {type: TraversalBlock, calls: batches, items: rows, filtered: 1},
          {type: ReturnBlock, calls: batches, items: rows, filtered: 0}
        ];
      };
      profHelper.runDefaultChecks(
        {query, genNodeList, prepare, bind}
      );
    },

    ////////////////////////////////////////////////////////////////////////////////
    /// @brief test TraversalBlock: traverse ~half a tree
    ////////////////////////////////////////////////////////////////////////////////

    /* TODO: enable this test once we have parallelism ready 
    testTraversalBlockParallel: function () {
      const col = db._createDocumentCollection(colName);
      const edgeCol = db._createEdgeCollection(edgeColName);
      const prepare = (rows) => {
        profHelper.createBinaryTree(col, edgeCol, rows);
      };
      const query = `FOR v IN 0..@depth OUTBOUND @root @@edgeCol OPTIONS {parallelism:2} RETURN v`;
      const rootNodeId = `${colName}/1`;
      // actual tree depth:
      // const treeDepth = rows => Math.ceil(Math.log2(rows));
      // tree is perfect up to this depth:
      const maxFullDepth = rows => Math.floor(Math.log2(rows));
      // substract one to get rid of ~half the nodes, but never go below 0
      const depth = rows => Math.max(0, maxFullDepth(rows) - 1);
      const bind = rows => ({
        depth: depth(rows),
        root: rootNodeId,
        '@edgeCol': edgeColName,
      });
      const visitedNodes = rows => Math.pow(2, depth(rows)+1)-1;

      const genNodeList = (rows, batches) => {
        rows = visitedNodes(rows);
        batches = Math.ceil(rows / defaultBatchSize);
        return [
          {type: SingletonBlock, calls: 1, items: 1},
          {type: MutexBlock, calls: batches, items: rows},
          {type: TraversalBlock, calls: batches, items: rows},
          {type: AsyncBlock, calls: batches, items: rows},
          {type: UnsortingGatherBlock, calls: batches, items: rows},
          {type: ReturnBlock, calls: batches, items: rows}
        ];
      };
      profHelper.runDefaultChecks(
        {query, genNodeList, prepare, bind}
      );
    },
    */

////////////////////////////////////////////////////////////////////////////////
/// @brief test EnumerateViewBlock1
////////////////////////////////////////////////////////////////////////////////

    testEnumerateViewBlock1: function () {
      const col = db._create(colName);
      const view = db._createView(viewName, "arangosearch", { links: { [colName]: { includeAllFields: true } } });
      const prepare = (rows) => {
        col.truncate({ compact: false });
        col.insert(_.range(1, rows + 1).map((i) => ({value: i})));
      };
      const bind = () => ({'@view': viewName});
      const query = `FOR d IN @@view SEARCH d.value != 0 OPTIONS { waitForSync: true } RETURN d.value`;

      const genNodeList = (rows, batches) => {
        // EnumerateViewBlock returns HASMORE when asked for the exact number
        // of items it has left. This could be improved.
        const optimalBatches = Math.ceil(rows / defaultBatchSize);
        const maxViewBatches = Math.floor(rows / defaultBatchSize) + 1;
        const viewBatches = [optimalBatches, maxViewBatches];

        return [
          {type: SingletonBlock, calls: 1, items: 1, filtered: 0},
          {type: EnumerateViewNode, calls: viewBatches, items: rows, filtered: 0},
          {type: CalculationBlock, calls: rows % defaultBatchSize === 0 ? batches + 1 : batches, items: rows, filtered: 0},
          {type: ReturnBlock, calls: rows % defaultBatchSize === 0 ? batches + 1 : batches, items: rows, filtered: 0}
        ];
      };
      profHelper.runDefaultChecks(
        {query, genNodeList, prepare, bind}
      );
    },

////////////////////////////////////////////////////////////////////////////////
/// @brief test EnumerateViewBlock2
////////////////////////////////////////////////////////////////////////////////

    testEnumerateViewBlock2: function () {
      const col = db._create(colName);
      const view = db._createView(viewName, "arangosearch", { links: { [colName]: { includeAllFields: true } } });
      const prepare = (rows) => {
        col.truncate({ compact: false });
        col.insert(_.range(1, rows + 1).map((i) => ({value: i})));
      };
      const bind = () => ({'@view': viewName});
      const query = `FOR d IN @@view SEARCH d.value != 0 OPTIONS { waitForSync: true } SORT d.value DESC RETURN d.value`;

      const genNodeList = (rows, batches) => {
        // EnumerateViewBlock returns HASMORE when asked for the exact number
        // of items it has left. This could be improved.
        const optimalBatches = Math.ceil(rows / defaultBatchSize);
        const maxViewBatches = Math.floor(rows / defaultBatchSize) + 1;
        const viewBatches = [optimalBatches, maxViewBatches];

        return [
          {type: SingletonBlock, calls: 1, items: 1, filtered: 0},
          {type: EnumerateViewNode, calls: viewBatches, items: rows, filtered: 0},
          {type: CalculationBlock, calls: rows % defaultBatchSize === 0 ? batches + 1 : batches, items: rows, filtered: 0},
          {type: SortBlock, calls: batches, items: rows, filtered: 0},
          {type: ReturnBlock, calls: batches, items: rows, filtered: 0}
        ];
      };
      profHelper.runDefaultChecks(
        {query, genNodeList, prepare, bind}
      );
    },

////////////////////////////////////////////////////////////////////////////////
/// @brief test EnumerateViewBlock3
////////////////////////////////////////////////////////////////////////////////

    testEnumerateViewBlock3: function () {
      const col = db._create(colName);
      const view = db._createView(viewName, "arangosearch", { links: { [colName]: { includeAllFields: true } } });
      const prepare = (rows) => {
        col.truncate({ compact: false });
        col.insert(_.range(1, rows + 1).map((i) => ({value: i})));
      };
      const bind = () => ({'@view': viewName});
      const query = `FOR d IN @@view SEARCH d.value != 0 OPTIONS { waitForSync: true } SORT TFIDF(d) ASC, BM25(d) RETURN d.value`;

      const genNodeList = (rows, batches) => {
        // EnumerateViewBlock returns HASMORE when asked for the exact number
        // of items it has left. This could be improved.
        const optimalBatches = Math.ceil(rows / defaultBatchSize);
        const maxViewBatches = Math.floor(rows / defaultBatchSize) + 1;
        const viewBatches = [optimalBatches, maxViewBatches];

        return [
          {type: SingletonBlock, calls: 1, items: 1, filtered: 0},
          {type: EnumerateViewNode, calls: viewBatches, items: rows, filtered: 0},
          {type: SortBlock, calls: batches, items: rows, filtered: 0},
          {type: CalculationBlock, calls: batches, items: rows, filtered: 0 },
          {type: ReturnBlock, calls: batches, items: rows, filtered: 0}
        ];
      };

      profHelper.runDefaultChecks(
        {query, genNodeList, prepare, bind}
      );
    },

    testLimitCollectCombination: function () {
      const query = `
            FOR x IN 1..@rows
              COLLECT AGGREGATE total = SUM(x)
              LIMIT 0, 1
              RETURN total
      `;
      const prepare = () => {};
      const bind = (rows) => ({rows});
      const genNodeList = (rows, batches) => {
        return [
          {type: SingletonBlock, calls: 1, items: 1, filtered: 0},
          {type: CalculationBlock, calls: 1, items: 1, filtered: 0},
          {type: EnumerateListBlock, calls: batches, items: rows, filtered: 0},
          {type: SortedCollectBlock, calls: 1, items: 1, filtered: 0},
          {type: LimitBlock, calls: 1, items: 1, filtered: 0},
          {type: ReturnBlock, calls: 1, items: 1, filtered: 0}
        ];
      };

      profHelper.runDefaultChecks(
        {query, genNodeList, prepare, bind}
      );
    }

  };
}

////////////////////////////////////////////////////////////////////////////////
/// @brief executes the test suite
////////////////////////////////////////////////////////////////////////////////

jsunity.run(ahuacatlProfilerTestSuite);

return jsunity.done();

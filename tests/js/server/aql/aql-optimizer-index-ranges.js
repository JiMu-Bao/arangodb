/*jshint globalstrict:false, strict:false, maxlen: 500 */
/*global assertTrue, assertEqual, assertNotEqual, AQL_EXPLAIN, AQL_EXECUTE */

////////////////////////////////////////////////////////////////////////////////
/// @brief tests for index usage
///
/// @file
///
/// DISCLAIMER
///
/// Copyright 2010-2012 triagens GmbH, Cologne, Germany
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
/// Copyright holder is triAGENS GmbH, Cologne, Germany
///
/// @author Jan Steemann
/// @author Copyright 2012, triAGENS GmbH, Cologne, Germany
////////////////////////////////////////////////////////////////////////////////

var jsunity = require("jsunity");
var db = require("@arangodb").db;

////////////////////////////////////////////////////////////////////////////////
/// @brief test suite
////////////////////////////////////////////////////////////////////////////////

function optimizerIndexesRangesTestSuite () {
  var c;

  return {
    setUp : function () {
      db._drop("UnitTestsCollection");
      c = db._create("UnitTestsCollection");

      for (var i = 0; i < 2000; ++i) {
        c.save({ _key: "test" + String(i).padStart(4,'0'), value1: (i % 100), value2: i });
      }
    },

    tearDown : function () {
      db._drop("UnitTestsCollection");
    },

////////////////////////////////////////////////////////////////////////////////
/// @brief test index usage
////////////////////////////////////////////////////////////////////////////////

    testRangesSingle : function () {
      c.ensureSkiplist("value1");
      var queries = [
        [ "FOR i IN " + c.name() + " FILTER i.value1 < 3 || i.value1 == 5 || i.value1 == 9 || i.value1 > 98 RETURN i.value1", [ 0, 1, 2, 5, 9, 99 ] ],
        [ "FOR i IN " + c.name() + " FILTER ((i.value1 < 2 || i.value1 == 3) || (i.value1 == 96 || i.value1 > 97)) RETURN i.value1", [ 0, 1, 3, 96, 98, 99 ] ],
        [ "FOR i IN " + c.name() + " FILTER i.value1 IN [ 35, 34, 33 ] && i.value1 < 35 RETURN i.value1", [ 33, 34 ] ],
        [ "FOR i IN " + c.name() + " FILTER i.value1 IN [ 35, 34, 33, 99 ] && i.value1 >= 34 RETURN i.value1", [ 34, 35, 99 ] ],
        [ "FOR i IN " + c.name() + " FILTER (i.value1 == 9 || i.value1 > 10) && (i.value1 <= 12) RETURN i.value1", [ 9, 11, 12 ] ],
        [ "FOR i IN " + c.name() + " FILTER (i.value1 == 9 || i.value1 >= 10) && (i.value1 <= 12) RETURN i.value1", [ 9, 10, 11, 12 ] ],
        [ "FOR i IN " + c.name() + " FILTER i.value1 >=9 && (i.value1 == 10 || i.value1 <= 12) RETURN i.value1", [ 9, 10, 11, 12 ] ],
        [ "FOR i IN " + c.name() + " FILTER i.value1 >=9 && (i.value1 == 10 || i.value1 <= 12 || i.value1 IN [ 5, 3 ]) RETURN i.value1", [ 9, 10, 11, 12 ] ],
        [ "FOR i IN " + c.name() + " FILTER (i.value1 >= 10) && (i.value1 == 10 || i.value1 <= 12) RETURN i.value1", [ 10, 11, 12 ] ],
        [ "FOR i IN " + c.name() + " FILTER (i.value1 == 9 || i.value1 >= 10) && (i.value1 == 10 || i.value1 <= 12) RETURN i.value1", [ 9, 10, 11, 12 ] ],
        [ "FOR i IN " + c.name() + " FILTER (i.value1 > 5 && i.value1 < 10) || (i.value1 > 37 && i.value1 <= 41) RETURN i.value1", [ 6, 7, 8, 9, 38, 39, 40, 41 ] ],
        [ "FOR i IN " + c.name() + " FILTER (i.value1 > 37 && i.value1 <= 41) || (i.value1 > 5 && i.value1 < 10) RETURN i.value1", [ 6, 7, 8, 9, 38, 39, 40, 41 ] ],
        [ "FOR i IN " + c.name() + " FILTER (i.value1 <= 41 && i.value1 > 37) || (i.value1 < 10 && i.value1 > 5) RETURN i.value1", [ 6, 7, 8, 9, 38, 39, 40, 41 ] ],
        [ "FOR i IN " + c.name() + " FILTER ((i.value1 <= 41 && i.value1 > 37) || i.value1 IN [ 3, 12 ]) || (i.value1 IN [ 74, 77 ] || (i.value1 < 10 && i.value1 > 5)) RETURN i.value1", [ 3, 6, 7, 8, 9, 12, 38, 39, 40, 41, 74, 77 ] ],
        [ "FOR i IN " + c.name() + " FILTER (i.value1 > 12 || i.value1 < 7) && (i.value1 > 97 || i.value1 < 5) RETURN i.value1", [ 0, 1, 2, 3, 4, 98, 99 ] ],
        [ "FOR i IN " + c.name() + " FILTER (i.value1 >= 11 && i.value1 <= 15) && (i.value1 > 7 && i.value < 28) && i.value1 IN [ 23, 17, 11, 12, 13, 29, 28, 27, 14, 15, 15, 15 ] RETURN i.value1", [ 11, 12, 13, 14, 15 ] ],
      ];

      queries.forEach(function(query) {
        var plan = AQL_EXPLAIN(query[0]).plan;
        var nodeTypes = plan.nodes.map(function(node) {
          return node.type;
        });

        // ensure an index is used
        assertNotEqual(-1, nodeTypes.indexOf("IndexNode"), query);

        var results = AQL_EXECUTE(query[0]);
        assertEqual(query[1].length * 20, results.json.length, query);
        // var last = query[1][0];
        results.json.forEach(function(value) {
          assertNotEqual(-1, query[1].indexOf(value));
          // This is not guaranteed any more
          // assertTrue(value >= last, query[0]);
          // last = value;
        });

        assertTrue(results.stats.scannedIndex > 0);
        assertEqual(0, results.stats.scannedFull);
      });
    },


    testPrimaryRanges : function () {
      var queries = [ // queries[0] - query -- queries[1] - expected result
        // _key and _id mixed
        [
          "FOR i IN " + c.name() + " FILTER (i._key >= 'test1990' && i._id <= '" + c.name() + "/test2') RETURN i._key",
          [ "test1999", "test1998", "test1997", "test1996", "test1995", "test1994", "test1993", "test1992", "test1991", "test1990" ]
        ],[
          "FOR i IN " + c.name() + " FILTER (i._key >= 'test1990' && i._id <= '" + c.name() + "/test2') RETURN i._key",
          [ "test1999", "test1998", "test1997", "test1996", "test1995", "test1994", "test1993", "test1992", "test1991", "test1990" ]
        ],

        // more than one condition
        [
          "FOR i IN " + c.name() + " FILTER (i._key >= 'test1990' && i._key <= 'test2') RETURN i._key",
          [ "test1999", "test1998", "test1997", "test1996", "test1995", "test1994", "test1993", "test1992", "test1991", "test1990" ]
        ],[
          "FOR i IN " + c.name() + " FILTER (i._key >= 'test1990' && i._key <= 'test2') RETURN i._key",
          [ "test1999", "test1998", "test1997", "test1996", "test1995", "test1994", "test1993", "test1992", "test1991", "test1990" ]

        ],

        // tests for _key
        [
          "FOR i IN " + c.name() + " FILTER (i._key < 'test0002') RETURN i._key",
          [ "test0000", "test0001" ]
        ],[
          "FOR i IN " + c.name() + " FILTER ('test0002' > i._key) RETURN i._key",
          [ "test0000", "test0001" ]
        ],[
          "FOR i IN " + c.name() + " FILTER (i._key > 'test1997') RETURN i._key",
          [ "test1998", "test1999" ]
        ],[
          "FOR i IN " + c.name() + " FILTER ('test1997' < i._key) RETURN i._key",
          [ "test1998", "test1999" ]
        ],[
          "FOR i IN " + c.name() + " FILTER (i._key <= 'test0002') RETURN i._key",
          [ "test0000", "test0001", "test0002" ]
        ],[
          "FOR i IN " + c.name() + " FILTER ('test0002' >= i._key) RETURN i._key",
          [ "test0000", "test0001", "test0002" ]
        ],[
          "FOR i IN " + c.name() + " FILTER (i._key >= 'test1997') RETURN i._key",
          [ "test1997", "test1998", "test1999" ]
        ],[
          "FOR i IN " + c.name() + " FILTER ('test1997' <= i._key) RETURN i._key",
          [ "test1997", "test1998", "test1999" ]
        ],

        // tests for _id
        [
          "FOR i IN " + c.name() + " FILTER (i._id < '" + c.name() + "/test0002') RETURN i._key",
          [ "test0000", "test0001" ]
        ],[
          "FOR i IN " + c.name() + " FILTER ('" + c.name() + "/test0002' > i._id) RETURN i._key",
          [ "test0000", "test0001" ]
        ],[
          "FOR i IN " + c.name() + " FILTER (i._id > '" + c.name() + "/test1997') RETURN i._key",
          [ "test1998", "test1999" ]
        ],[
          "FOR i IN " + c.name() + " FILTER ('" + c.name() + "/test1997' < i._id) RETURN i._key",
          [ "test1998", "test1999" ]
        ],[
          "FOR i IN " + c.name() + " FILTER (i._id <= '" + c.name() + "/test0002') RETURN i._key",
          [ "test0000", "test0001", "test0002" ]
        ],[
          "FOR i IN " + c.name() + " FILTER ('" + c.name() + "/test0002' >= i._id) RETURN i._key",
          [ "test0000", "test0001", "test0002" ]
        ],[
          "FOR i IN " + c.name() + " FILTER (i._id >= '" + c.name() + "/test1997') RETURN i._key",
          [ "test1997", "test1998", "test1999" ]
        ],[
          "FOR i IN " + c.name() + " FILTER ('" + c.name() + "/test1997' <= i._id) RETURN i._key",
          [ "test1997", "test1998", "test1999" ]
        ],

        //// edge cases
        [
        // one element in range
          "FOR i IN " + c.name() + " FILTER ('test1997' <= i._key && 'test1997' >= i._key) RETURN i._key",
          [ "test1997" ]
        ]
      ]; //end of array

      queries.forEach(function(query) {
        var plan = AQL_EXPLAIN(query[0]).plan;
        var nodeTypes = plan.nodes.map(function(node) {
          return node.type;
        });

        //db._explain(query[0]);
        // ensure an index is used
        assertNotEqual(-1, nodeTypes.indexOf("IndexNode"), query);

        var results = AQL_EXECUTE(query[0]);
        //print("#########################################################");
        //print(results);
        //print("#########################################################");

        assertEqual(query[1].length , results.json.length, query);
        var last = query[1][0];
        results.json.forEach(function(value) {
          assertNotEqual(-1, query[1].indexOf(value));
        });

        assertTrue(results.stats.scannedIndex > 0);
        assertEqual(0, results.stats.scannedFull);
      });
    },



    testPrimaryRangesEdgeCases : function () {
      var queries = [ // queries[0] - query -- queries[1] - expected result
        [
          // upper is greater than lower
          "FOR i IN " + c.name() + " FILTER (i._key <= 'test1997' && i._key >= 'test1998') RETURN i._key",
          [ ]
        ],[
          "FOR i IN " + c.name() + " FILTER (i._key == 'test1997' && i._key >= 'test1997') RETURN i._key",
          [ 'test1997' ]
        ],[
          "FOR i IN " + c.name() + " FILTER (i._key == 'test1997' && i._key == 'test1998') RETURN i._key",
          [  ]
        ],[
          "FOR i IN " + c.name() + " FILTER (i._key < true) RETURN i._key",
          [  ]
        ]
      ]; //end of array

      queries.forEach(function(query) {
        var plan = AQL_EXPLAIN(query[0]).plan;
        var nodeTypes = plan.nodes.map(function(node) {
          return node.type;
        });

        //db._explain(query[0]);

        var results = AQL_EXECUTE(query[0]);
        //print("#########################################################");
        //print(results);
        //print("#########################################################");

        assertEqual(query[1].length , results.json.length, query);
        var last = query[1][0];
        results.json.forEach(function(value) {
          assertNotEqual(-1, query[1].indexOf(value));
        });

        assertEqual(0, results.stats.scannedFull);
      });
    },


////////////////////////////////////////////////////////////////////////////////
/// @brief test index usage
////////////////////////////////////////////////////////////////////////////////

    testRangesWithRational : function () {
      c.ensureSkiplist("value1");
      for (var i = 0; i < 20; ++i) {
        c.save({ value1: 3.5 });
        c.save({ value1: 12.5 });
        c.save({ value1: 10.1 });
        c.save({ value1: 10.9 });
        c.save({ value1: 42.23 });
      }
      var queries = [
        [ "FOR i IN " + c.name() + " FILTER i.value1 IN [ 3, 4, 5 ] RETURN i.value1", [ 3, 4, 5 ] ],
        [ "FOR i IN " + c.name() + " FILTER (i.value1 >= 10 && i.value1 < 11) RETURN i.value1", [ 10, 10.1, 10.9 ] ],
        [ "FOR i IN " + c.name() + " FILTER (i.value1 >= 10.1 && i.value1 <= 10.9) RETURN i.value1", [ 10.1, 10.9 ] ],
        [ "FOR i IN " + c.name() + " FILTER (i.value1 >= 10 && i.value1 <= 11) RETURN i.value1", [ 10, 10.1, 10.9, 11 ] ],
        [ "FOR i IN " + c.name() + " FILTER i.value1 IN [ 3, 4, 5 ] || i.value1 < 5 RETURN i.value1", [ 0, 1, 2, 3, 3.5, 4, 5 ] ],
        [ "FOR i IN " + c.name() + " FILTER i.value1 IN [ 3, 4, 5 ] && i.value1 < 5 RETURN i.value1", [ 3, 4 ] ],
        [ "FOR i IN " + c.name() + " FILTER i.value1 IN [ 3, 4, 5 ] && i.value1 <= 5 RETURN i.value1", [ 3, 4, 5 ] ],
        [ "FOR i IN " + c.name() + " FILTER i.value1 > 40 && i.value1 > 42 && i.value1 < 43 && i.value1 < 44 RETURN i.value1", [ 42.23 ] ],
        [ "FOR i IN " + c.name() + " FILTER (i.value1 < 44 && i.value1 > 42) || (i.value1 >= 41 && i.value1 <= 44) RETURN i.value1", [ 41, 42, 42.23, 43, 44 ] ]
      ];

      queries.forEach(function(query) {
        var plan = AQL_EXPLAIN(query[0]).plan;
        var nodeTypes = plan.nodes.map(function(node) {
          return node.type;
        });

        // ensure an index is used
        assertNotEqual(-1, nodeTypes.indexOf("IndexNode"), query);

        var results = AQL_EXECUTE(query[0]);
        assertEqual(query[1].length * 20, results.json.length, query);
        // var last = query[1][0];
        results.json.forEach(function(value) {
          assertNotEqual(-1, query[1].indexOf(value));
          // This is not guaranteed any more
          // assertTrue(value >= last, query[0]);
          // last = value;
        });

        assertTrue(results.stats.scannedIndex > 0);
        assertEqual(0, results.stats.scannedFull);
      });
    },

////////////////////////////////////////////////////////////////////////////////
/// @brief test index usage
////////////////////////////////////////////////////////////////////////////////

    testMultipleFilters : function () {
      c.ensureSkiplist("value1");
      var queries = [
        [ "FOR i IN " + c.name() + " FILTER i.value1 < 3 FILTER i.value1 < 10 RETURN i.value1", [ 0, 1, 2 ] ],
        [ "FOR i IN " + c.name() + " FILTER i.value1 < 10 FILTER i.value1 IN [ 7, 3, 1 ] RETURN i.value1", [ 1, 3, 7 ] ],
        [ "FOR i IN " + c.name() + " FILTER i.value1 < 10 FILTER i.value1 < 9 FILTER i.value1 < 8 FILTER i.value1 < 7 RETURN i.value1", [ 0, 1, 2, 3, 4, 5, 6 ] ],
        [ "FOR i IN " + c.name() + " FILTER i.value1 < 10 FILTER i.value1 < 9 FILTER i.value1 < 8 FILTER i.value1 == 3 RETURN i.value1", [ 3 ] ],
        [ "FOR i IN " + c.name() + " FILTER i.value1 < 10 FILTER i.value1 < 9 FILTER i.value1 < 8 FILTER i.value1 > 3 RETURN i.value1", [ 4, 5, 6, 7 ] ],
        [ "FOR i IN " + c.name() + " FILTER i.value1 IN [ 12, 13, 14, 15, 17 ] FILTER i.value1 < 20 FILTER i.value1 > 10 FILTER i.value1 IN [ 11, 12, 13, 14, 15, 16, 17, 18 ] RETURN i.value1", [ 12, 13, 14, 15, 17 ] ]
      ];

      queries.forEach(function(query) {
        var plan = AQL_EXPLAIN(query[0]).plan;
        var nodeTypes = plan.nodes.map(function(node) {
          return node.type;
        });

        // ensure an index is used
        assertNotEqual(-1, nodeTypes.indexOf("IndexNode"), query);

        var results = AQL_EXECUTE(query[0]);
        assertEqual(query[1].length * 20, results.json.length, query);
        var last = query[1][0];
        results.json.forEach(function(value) {
          assertNotEqual(-1, query[1].indexOf(value));
          assertTrue(value >= last);
          last = value;
        });

        assertTrue(results.stats.scannedIndex > 0);
        assertEqual(0, results.stats.scannedFull);
      });
    }

  };
}
jsunity.run(optimizerIndexesRangesTestSuite);

return jsunity.done();


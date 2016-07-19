////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2014-2016 ArangoDB GmbH, Cologne, Germany
/// Copyright 2004-2014 triAGENS GmbH, Cologne, Germany
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
/// @author Michael Hackstein
////////////////////////////////////////////////////////////////////////////////

#include "SingleServerTraverser.h"
#include "Utils/Transaction.h"
#include "VocBase/MasterPointer.h"

using namespace arangodb;
using namespace arangodb::traverser;

////////////////////////////////////////////////////////////////////////////////
/// @brief Get a document by it's ID. Also lazy locks the collection.
///        If DOCUMENT_NOT_FOUND this function will return normally
///        with a OperationResult.failed() == true.
///        On all other cases this function throws.
////////////////////////////////////////////////////////////////////////////////

static int FetchDocumentById(arangodb::Transaction* trx,
                             std::string const& id,
                             TRI_doc_mptr_t* mptr) {
  size_t pos = id.find('/');
  if (pos == std::string::npos) {
    TRI_ASSERT(false);
    return TRI_ERROR_INTERNAL;
  }
  if (id.find('/', pos + 1) != std::string::npos) {
    TRI_ASSERT(false);
    return TRI_ERROR_INTERNAL;
  }

  int res = trx->documentFastPathLocal(id.substr(0, pos), id.substr(pos + 1), mptr);

  if (res != TRI_ERROR_NO_ERROR && res != TRI_ERROR_ARANGO_DOCUMENT_NOT_FOUND) {
    THROW_ARANGO_EXCEPTION(res);
  }
  return res;
}

SingleServerEdgeCursor::SingleServerEdgeCursor() {
#warning TODO Implement
};

bool SingleServerEdgeCursor::next(std::vector<VPackSlice>& result) {
#warning TODO Implement
  return false;
}

SingleServerTraverser::SingleServerTraverser(TraverserOptions& opts,
                                             arangodb::Transaction* trx)
    : Traverser(opts), _trx(trx) {
  _edgeGetter = std::make_unique<EdgeGetter>(this, opts, trx);
  if (opts.uniqueVertices == TraverserOptions::UniquenessLevel::GLOBAL) {
    _vertexGetter = std::make_unique<UniqueVertexGetter>(this);
  } else {
    _vertexGetter = std::make_unique<VertexGetter>(this);
  }
}

SingleServerTraverser::~SingleServerTraverser() {}

bool SingleServerTraverser::edgeMatchesConditions(VPackSlice e, size_t depth) {
  if (_opts.evaluateEdgeExpression(e, depth)) {
    ++_filteredPaths;
    return false;
  }
  return true;
}

bool SingleServerTraverser::vertexMatchesConditions(VPackSlice v,
                                                    size_t depth) {
  TRI_ASSERT(v.isString());
#warning it is possible to not fetch the vertex if no check is required.
  aql::AqlValue vertex = fetchVertexData(v);
  if (!_opts.evaluateVertexExpression(vertex.slice(), depth)) {
    ++_filteredPaths;
    return false;
  }
  return true;
}

aql::AqlValue SingleServerTraverser::fetchVertexData(VPackSlice id) {
  TRI_ASSERT(id.isString());
  auto it = _vertices.find(id);

  if (it == _vertices.end()) {
    TRI_doc_mptr_t mptr;
#warning Do we need the copy here
    int res = FetchDocumentById(_trx, id.copyString(), &mptr);
    ++_readDocuments;
    if (res != TRI_ERROR_NO_ERROR) {
      return aql::AqlValue(basics::VelocyPackHelper::NullValue());
    }

    uint8_t const* p = mptr.vpack();
    _vertices.emplace(id, p);
    return aql::AqlValue(p, aql::AqlValueFromMasterPointer());
  }

  return aql::AqlValue((*it).second, aql::AqlValueFromMasterPointer());
}

aql::AqlValue SingleServerTraverser::fetchEdgeData(VPackSlice edge) {
#warning Is this enough?
  return aql::AqlValue(edge);
  /*
  auto it = _edges.find(id);
  
  TRI_ASSERT(it != _edges.end());
  return aql::AqlValue((*it).second, aql::AqlValueFromMasterPointer());
  */
}

void SingleServerTraverser::addVertexToVelocyPack(VPackSlice id,
                                                  VPackBuilder& result) {
  TRI_ASSERT(id.isString());
  auto it = _vertices.find(id);

  if (it == _vertices.end()) {
    TRI_doc_mptr_t mptr;
#warning Do we need the copy here?
    int res = FetchDocumentById(_trx, id.copyString(), &mptr);
    ++_readDocuments;
    if (res != TRI_ERROR_NO_ERROR) {
      result.add(basics::VelocyPackHelper::NullValue());
    } else {
      uint8_t const* p = mptr.vpack();
      _vertices.emplace(id, p);
      result.addExternal(p);
    }
  } else {
    result.addExternal((*it).second);
  }
}

void SingleServerTraverser::addEdgeToVelocyPack(VPackSlice edge,
    VPackBuilder& result) {
  result.addExternal(edge.begin());
}

bool SingleServerTraverser::VertexGetter::getVertex(
    VPackSlice edge, std::vector<VPackSlice>& result) {
  VPackSlice cmp = result.back();
  VPackSlice from = Transaction::extractFromFromDocument(edge);
#warning Here we have to execute VertexFilter
  /// If the vertex is not cached in _traverser->_vertices => incr. _filteredPath return false;
  /// Else check condition if ok => return true, else return false.
  /// When returning false set result = ""
  if (arangodb::basics::VelocyPackHelper::compare(cmp, from, false) != 0) {
    result.emplace_back(from);
  } else {
    result.emplace_back(Transaction::extractToFromDocument(edge));
  }
  return true;
}

bool SingleServerTraverser::VertexGetter::getSingleVertex(VPackSlice edge,
                                                          VPackSlice cmp,
                                                          size_t depth,
                                                          VPackSlice& result) {
  VPackSlice from = Transaction::extractFromFromDocument(edge);
#warning Here we have to execute VertexFilter
  /// If the vertex is not cached in _traverser->_vertices => incr. _filteredPath return false;
  /// Else check condition if ok => return true, else return false.
  /// When returning false set result = ""
  if (arangodb::basics::VelocyPackHelper::compare(cmp, from, false) != 0) {
    result = from;
  } else {
    result = Transaction::extractToFromDocument(edge);
  }
  return true;
}



void SingleServerTraverser::VertexGetter::reset(arangodb::velocypack::Slice) {
}

bool SingleServerTraverser::UniqueVertexGetter::getVertex(
  VPackSlice edge, std::vector<VPackSlice>& result) {
  VPackSlice toAdd = Transaction::extractFromFromDocument(edge);
  VPackSlice cmp = result.back();

  if (arangodb::basics::VelocyPackHelper::compare(toAdd, cmp, false) == 0) {
    toAdd = Transaction::extractToFromDocument(edge);
  }
  
#warning Here we have to execute VertexFilter
    /// If the vertex is not cached in _traverser->_vertices => incr. _filteredPath return false;
    /// Else check condition if ok => return true, else return false.
    /// When returning false set result = ""
    /// When returning true: _returnedVertices.emplace(result)

  if (_returnedVertices.find(toAdd) != _returnedVertices.end()) {
    // This vertex is not unique.
    ++_traverser->_filteredPaths;
    return false;
  } else {
    _returnedVertices.emplace(toAdd);
    result.emplace_back(toAdd);
    return true;
  }

  // This should never be reached
  return false;
}

bool SingleServerTraverser::UniqueVertexGetter::getSingleVertex(
  VPackSlice edge, VPackSlice cmp, size_t depth, VPackSlice& result) {
  result = Transaction::extractFromFromDocument(edge);

  if (arangodb::basics::VelocyPackHelper::compare(result, cmp, false) == 0) {
    result = Transaction::extractToFromDocument(edge);
  }
  
#warning Here we have to execute VertexFilter
    /// If the vertex is not cached in _traverser->_vertices => incr. _filteredPath return false;
    /// Else check condition if ok => return true, else return false.
    /// When returning false set result = ""
    /// When returning true: _returnedVertices.emplace(result)

  if (_returnedVertices.find(result) != _returnedVertices.end()) {
    // This vertex is not unique.
    ++_traverser->_filteredPaths;
    return false;
  } else {
    _returnedVertices.emplace(result);
    return true;
  }

  // This should never be reached
  return false;
}

void SingleServerTraverser::UniqueVertexGetter::reset(VPackSlice startVertex) {
  _returnedVertices.clear();
  // The startVertex always counts as visited!
  _returnedVertices.emplace(startVertex);
}

void SingleServerTraverser::setStartVertex(std::string const& v) {
  _pruneNext = false;

#warning Who is responsible for this builder?!
  VPackBuilder tmp;
  tmp.add(VPackValue(v));
  VPackSlice idSlice = tmp.slice();

  TRI_doc_mptr_t vertex;
  int result = FetchDocumentById(_trx, v, &vertex);
  ++_readDocuments;

  if (result != TRI_ERROR_NO_ERROR) {
    // Vertex does not exist
    _done = true;
    return;
  }
  VPackSlice vertexSlice(vertex.vpack());

  if (!_opts.evaluateVertexExpression(vertexSlice, 0)) {
    // Start vertex invalid
    ++_filteredPaths;
    _done = true;
    return;
  }
  _vertexGetter->reset(idSlice);
  if (_opts.useBreadthFirst) {
    _enumerator.reset(new BreadthFirstEnumerator(this, vertexSlice, &_opts));
  } else {
    _enumerator.reset(new DepthFirstEnumerator(this, vertexSlice, &_opts));
  }
  _done = false;
}

void SingleServerTraverser::getEdge(std::string const& startVertex,
                                    std::vector<std::string>& edges,
                                    size_t*& last, size_t& eColIdx) {
  return _edgeGetter->getEdge(startVertex, edges, last, eColIdx);
}

void SingleServerTraverser::getAllEdges(
    arangodb::velocypack::Slice startVertex,
    std::unordered_set<arangodb::velocypack::Slice>& edges, size_t depth) {
  return _edgeGetter->getAllEdges(startVertex, edges, depth);
}

bool SingleServerTraverser::getVertex(VPackSlice edge,
                                      std::vector<VPackSlice>& result) {
  return _vertexGetter->getVertex(edge, result);
}

bool SingleServerTraverser::getSingleVertex(VPackSlice edge, VPackSlice vertex,
                                            size_t depth, VPackSlice& result) {
  return _vertexGetter->getSingleVertex(edge, vertex, depth, result);
}

bool SingleServerTraverser::next() {
  TRI_ASSERT(!_done);
  bool res = _enumerator->next();
  if (!res) {
    _done = true;
  }
  return res;
  /*
  size_t countEdges = path.edges.size();
  if (_opts.useBreadthFirst &&
      _opts.uniqueVertices == TraverserOptions::UniquenessLevel::NONE &&
      _opts.uniqueEdges == TraverserOptions::UniquenessLevel::PATH) {
    // Only if we use breadth first
    // and vertex uniqueness is not guaranteed
    // We have to validate edges on path uniqueness.
    // Otherwise this situation cannot occur.
    // If two edges are identical than at least their start or end vertex
    // is on the path twice: A -> B <- A
    for (size_t i = 0; i < countEdges; ++i) {
      for (size_t j = i + 1; j < countEdges; ++j) {
        if (path.edges[i] == path.edges[j]) {
          // We found two idential edges. Prune.
          // Next
          _pruneNext = true;
          return next();
        }
      }
    }
  }
  */
}

aql::AqlValue SingleServerTraverser::lastVertexToAqlValue() {
  return _enumerator->lastVertexToAqlValue();
}

aql::AqlValue SingleServerTraverser::lastEdgeToAqlValue() {
  return _enumerator->lastEdgeToAqlValue();
}

aql::AqlValue SingleServerTraverser::pathToAqlValue(VPackBuilder& builder) {
  return _enumerator->pathToAqlValue(builder);
}

bool SingleServerTraverser::EdgeGetter::nextCursor(std::string const& startVertex,
                                                   size_t& eColIdx,
                                                   size_t*& last) {
#warning Reimplement
  return false;
  /*
  std::string eColName;

  while (true) {
    arangodb::Transaction::IndexHandle indexHandle;
    if (last != nullptr) {
      // The cursor is empty clean up
      last = nullptr;
      TRI_ASSERT(!_posInCursor.empty());
      TRI_ASSERT(!_cursors.empty());
      TRI_ASSERT(!_results.empty());
      _posInCursor.pop();
      _cursors.pop();
      _results.pop();
    }
    if (!_opts.getCollectionAndSearchValue(eColIdx, startVertex, eColName, indexHandle,
                                           _builder)) {
      // If we get here there are no valid edges at all
      return false;
    }
    
    std::unique_ptr<OperationCursor> cursor = _trx->indexScan(
        eColName, arangodb::Transaction::CursorType::INDEX, indexHandle,
        _builder.slice(), 0, UINT64_MAX, Transaction::defaultBatchSize(), false);
    if (cursor->failed()) {
      // Some error, ignore and go to next
      eColIdx++;
      continue;
    }
    TRI_ASSERT(_posInCursor.size() == _cursors.size());
    _cursors.push(std::move(cursor));
    _results.emplace();
    return true;
  }
  */
}

void SingleServerTraverser::EdgeGetter::nextEdge(
    std::string const& startVertex, size_t& eColIdx, size_t*& last,
    std::vector<std::string>& edges) {

  if (last == nullptr) {
    _posInCursor.push(0);
    last = &_posInCursor.top();
  } else {
    ++(*last);
  }

  while (true) {
    TRI_ASSERT(!_cursors.empty());
    auto& cursor = _cursors.top();
    TRI_ASSERT(!_results.empty());
    auto& mptrs = _results.top();

    // note: we need to check *first* that there is actually something in the mptrs list
    if (mptrs.empty() || mptrs.size() <= *last) {
      if (cursor->hasMore()) {
        // Fetch next and try again
        cursor->getMoreMptr(mptrs);
        TRI_ASSERT(last != nullptr);
        *last = 0;
        _traverser->_readDocuments += static_cast<size_t>(mptrs.size());
        continue;
      }
      eColIdx++;
      if (!nextCursor(startVertex, eColIdx, last)) {
        // No further edges.
        TRI_ASSERT(last == nullptr);
        TRI_ASSERT(_cursors.size() == _posInCursor.size());
        TRI_ASSERT(_cursors.size() == _results.size());
        return;
      }
      // There is a new Cursor on top of the stack, try it
      _posInCursor.push(0);
      last = &_posInCursor.top();
      continue;
    }

    VPackSlice edge(mptrs[*last]->vpack());
    std::string id = _trx->extractIdString(edge);
    if (!_traverser->edgeMatchesConditions(edge, edges.size())) {
      if (_opts.uniqueEdges == TraverserOptions::UniquenessLevel::GLOBAL) {
        // Insert a dummy to please the uniqueness
        _traverser->_edges.emplace(id, nullptr);
      }

      TRI_ASSERT(last != nullptr);
      (*last)++;
      continue;
    }
    if (_opts.uniqueEdges == TraverserOptions::UniquenessLevel::PATH) {
      // test if edge is already on this path
      auto found = std::find(edges.begin(), edges.end(), id);
      if (found != edges.end()) {
        // This edge is already on the path, next
        TRI_ASSERT(last != nullptr);
        (*last)++;
        continue;
      }
    } else if (_opts.uniqueEdges == TraverserOptions::UniquenessLevel::GLOBAL) {
      if (_traverser->_edges.find(id) != _traverser->_edges.end()) {
        // This edge is already on the path, next
        TRI_ASSERT(last != nullptr);
        (*last)++;
        continue;
      }
    }

    _traverser->_edges.emplace(id, edge.begin());
    edges.emplace_back(std::move(id));
    return;
  }
}

void SingleServerTraverser::EdgeGetter::getEdge(std::string const& startVertex,
                                                std::vector<std::string>& edges,
                                                size_t*& last,
                                                size_t& eColIdx) {
  if (last == nullptr) {
    eColIdx = 0;
    if (!nextCursor(startVertex, eColIdx, last)) {
      // We were not able to find any edge
      return;
    }
  }
  nextEdge(startVertex, eColIdx, last, edges);
}

void SingleServerTraverser::EdgeGetter::getAllEdges(
    VPackSlice startVertex, std::unordered_set<VPackSlice>& edges,
    size_t depth) {
#warning reimplement
/*
  size_t idxId = 0;
  std::string eColName;
  arangodb::Transaction::IndexHandle indexHandle;
  std::vector<TRI_doc_mptr_t*> mptrs;

  // We iterate over all index ids. note idxId++
  while (_opts.getCollectionAndSearchValue(idxId++, startVertex, eColName,
                                           indexHandle, _builder)) {
    std::unique_ptr<OperationCursor> cursor = _trx->indexScan(
        eColName, arangodb::Transaction::CursorType::INDEX, indexHandle,
        _builder.slice(), 0, UINT64_MAX, Transaction::defaultBatchSize(), false);
    if (cursor->failed()) {
      // Some error, ignore and go to next
      continue;
    }
    mptrs.clear();
    while (cursor->hasMore()) {
      cursor->getMoreMptr(mptrs, UINT64_MAX);
      edges.reserve(mptrs.size());

      _traverser->_readDocuments += static_cast<size_t>(mptrs.size());
        
      std::string id;
      for (auto const& mptr : mptrs) {
        VPackSlice edge(mptr->vpack());
        id = _trx->extractIdString(edge);
        if (!_traverser->edgeMatchesConditions(edge, depth)) {
          if (_opts.uniqueEdges == TraverserOptions::UniquenessLevel::GLOBAL) {
            // Insert a dummy to please the uniqueness
            _traverser->_edges.emplace(std::move(id), nullptr);
          }
          continue;
        }
        if (_opts.uniqueEdges == TraverserOptions::UniquenessLevel::PATH) {
          // test if edge is already on this path
          auto found = edges.find(id);
          if (found != edges.end()) {
            // This edge is already on the path, next
            continue;
          }
        } else if (_opts.uniqueEdges == TraverserOptions::UniquenessLevel::GLOBAL) {
          if (_traverser->_edges.find(id) != _traverser->_edges.end()) {
            // This edge is already on the path, next
            continue;
          }
        }

        _traverser->_edges.emplace(id, edge.begin());
        edges.emplace(std::move(id));
      }
    }
  }
  */
}


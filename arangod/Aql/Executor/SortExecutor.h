////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2014-2024 ArangoDB GmbH, Cologne, Germany
/// Copyright 2004-2014 triAGENS GmbH, Cologne, Germany
///
/// Licensed under the Business Source License 1.1 (the "License");
/// you may not use this file except in compliance with the License.
/// You may obtain a copy of the License at
///
///     https://github.com/arangodb/arangodb/blob/devel/LICENSE
///
/// Unless required by applicable law or agreed to in writing, software
/// distributed under the License is distributed on an "AS IS" BASIS,
/// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
/// See the License for the specific language governing permissions and
/// limitations under the License.
///
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
///
/// @author Tobias Goedderz
/// @author Michael Hackstein
/// @author Heiko Kernbach
/// @author Jan Christoph Uhde
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include "Aql/ExecutionState.h"
#include "Aql/InputAqlItemRow.h"
#include "Aql/RegisterInfos.h"

#include <cstddef>
#include <memory>
#include <vector>

namespace arangodb {
struct ResourceMonitor;
class TemporaryStorageFeature;

namespace transaction {
class Methods;
}

namespace velocypack {
struct Options;
}

namespace aql {

struct AqlCall;
class AqlItemBlockInputRange;
class AqlItemBlockManager;
class RegisterInfos;
class NoStats;
class OutputAqlItemRow;
class QueryContext;
template<BlockPassthrough>
class SingleRowFetcher;
struct SortRegister;
class SortedRowsStorageBackend;

class SortExecutorInfos {
 public:
  SortExecutorInfos(std::vector<SortRegister> sortRegisters,
                    AqlItemBlockManager& manager, QueryContext& query,
                    TemporaryStorageFeature& tempStorage,
                    velocypack::Options const* options,
                    ResourceMonitor& resourceMonitor,
                    size_t spillOverThresholdNumRows,
                    size_t spillOverThresholdMemoryUsage, bool stable);

  SortExecutorInfos() = delete;
  SortExecutorInfos(SortExecutorInfos&&) = default;
  SortExecutorInfos(SortExecutorInfos const&) = delete;
  ~SortExecutorInfos() = default;

  [[nodiscard]] velocypack::Options const* vpackOptions() const noexcept {
    return _vpackOptions;
  }
  [[nodiscard]] std::vector<SortRegister> const& sortRegisters()
      const noexcept {
    return _sortRegisters;
  }
  [[nodiscard]] ResourceMonitor& getResourceMonitor() const {
    return _resourceMonitor;
  }
  [[nodiscard]] bool stable() const { return _stable; }
  [[nodiscard]] size_t spillOverThresholdNumRows() const noexcept {
    return _spillOverThresholdNumRows;
  }
  [[nodiscard]] size_t spillOverThresholdMemoryUsage() const noexcept {
    return _spillOverThresholdMemoryUsage;
  }
  [[nodiscard]] AqlItemBlockManager& itemBlockManager() noexcept {
    return _manager;
  }
  [[nodiscard]] TemporaryStorageFeature& getTemporaryStorageFeature() noexcept {
    return _tempStorage;
  }
  [[nodiscard]] QueryContext& getQuery() const noexcept { return _query; }

 private:
  AqlItemBlockManager& _manager;
  TemporaryStorageFeature& _tempStorage;
  QueryContext& _query;
  velocypack::Options const* _vpackOptions;
  ResourceMonitor& _resourceMonitor;
  std::vector<SortRegister> _sortRegisters;
  size_t _spillOverThresholdNumRows;
  size_t _spillOverThresholdMemoryUsage;
  bool _stable;
};

/**
 * @brief Implementation of Sort Node
 */
class SortExecutor {
 public:
  struct Properties {
    static constexpr bool preservesOrder = false;
    static constexpr BlockPassthrough allowsBlockPassthrough =
        BlockPassthrough::Disable;
  };
  using Fetcher = SingleRowFetcher<Properties::allowsBlockPassthrough>;
  using Infos = SortExecutorInfos;
  using Stats = NoStats;

  SortExecutor(Fetcher&, Infos& infos);
  ~SortExecutor();

  /**
   * @brief produce the next Rows of Aql Values.
   *
   * @return ExecutorState, the stats, and a new Call that needs to be sent to
   * upstream
   */
  [[nodiscard]] std::tuple<ExecutorState, Stats, AqlCall> produceRows(
      AqlItemBlockInputRange& inputRange, OutputAqlItemRow& output);

  /**
   * @brief skip the next Row of Aql Values.
   *
   * @return ExecutorState, the stats, and a new Call that needs to be sent to
   * upstream
   */
  [[nodiscard]] std::tuple<ExecutorState, Stats, size_t, AqlCall> skipRowsRange(
      AqlItemBlockInputRange& inputRange, AqlCall& call);

 private:
  bool _inputReady = false;
  Infos& _infos;

  std::unique_ptr<SortedRowsStorageBackend> _storageBackend;
};
}  // namespace aql
}  // namespace arangodb

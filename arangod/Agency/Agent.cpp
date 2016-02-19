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
/// @author Kaveh Vahedipour
////////////////////////////////////////////////////////////////////////////////

#include "Agent.h"

using namespace arangodb::consensus;
using namespace arangodb::velocypack;

Agent::Agent () {
}

Agent::Agent (config_t const& config) : _config(config) {
  _constituent.configure(this);

}

Agent::~Agent () {
	_constituent.stop();
}

void Agent::start() {
  _constituent.start();
}

Constituent::term_t Agent::term () const {
  return _constituent.term();
}

bool Agent::vote(Constituent::id_t id, Constituent::term_t term) {
	return _constituent.vote(id, term);
}

Log::ret_t Agent::log (std::shared_ptr<Builder> const builder) {
    return _log.log(builder);
}

Config<double> const& Agent::config () const {
  return _config;
}

void Agent::print (arangodb::LoggerStream& logger) const {
  logger << _config;
}

arangodb::LoggerStream& operator<< (arangodb::LoggerStream& l, Agent const& a) {
  a.print(l);
  return l;
}

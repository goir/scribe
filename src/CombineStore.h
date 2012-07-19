//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//
// See accompanying file LICENSE

#ifndef CombineStore_H_
#define CombineStore_H_

#ifdef USE_SCRIBE_MYSQL

#include "common.h"
#include "store.h"
#include "scribe_server.h"
#include "MysqlStore.h"
#include <jansson.h>
#include <mysql/mysql.h>
#include <boost/algorithm/string.hpp>
#include <boost/foreach.hpp>
#include <boost/algorithm/string/trim.hpp>
#include <time.h>

using namespace std;
using namespace boost;

typedef map<string, map<string, vector<string> > > fieldsType;
typedef map<string, vector<string> > fieldsFunctionsType;

/*
 * This store
 */
class CombineStore: public MysqlStore {
public:
  CombineStore(StoreQueue* storeq, const string& category, bool multi_category);
  ~CombineStore();

  string applyFunctionsToValue(map<string, vector<string> > functions,
      string value, string oldValue);
  bool applyValues(map<string, map<string, string> >& data, string groupKey, fieldsType valueFields,
      map<string, string> values);
  string getGroupKeyString(fieldsType fields, map<string, string> values);
  string doSubstitutions(string const &in, map<string, string> const &subst);
  bool getColumnStringValue(json_t* root, string& _return);
  fieldsType parseFieldsConfig(string valueStr);
  double getColumnDoubleValue(json_t* root, string key, double& _return);
  boost::shared_ptr<Store> copy(const string &category);
  bool executeQuery(string query);
  bool handleMessages(boost::shared_ptr<logentry_vector_t> messages);
  void configure(pStoreConf configuration, pStoreConf parent);

protected:

  // Counter Config
  //string groupSeparator;
  fieldsType groupFields;
  fieldsType valueFields;
  string querieTemplates;

private:
  // disallow copy, assignment, and empty construction
  CombineStore();
  CombineStore(CombineStore& rhs);
  CombineStore& operator=(CombineStore& rhs);
};

#endif

#endif


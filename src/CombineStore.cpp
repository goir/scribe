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

/**
 * SQL INSERT/UPDATE Statement gzip compressed!
 */

#ifdef USE_SCRIBE_MYSQL

#include "CombineStore.h"

using namespace std;
using namespace boost;

CombineStore::CombineStore(StoreQueue* storeq, const std::string& category,
    bool multi_category) :
    MysqlStore(storeq, category, multi_category) {
}

CombineStore::~CombineStore() {
  // noop
}

std::map<std::string, std::map<std::string, std::vector<std::string> > > CombineStore::parseFieldsConfig(
    string valueStr) {
  map<string, map<string, vector<string> > > _return;

  vector<string> strs;
  boost::split(strs, valueStr, boost::is_any_of(";"));
  BOOST_FOREACH(string val, strs) {
    trim_if(val, is_any_of(" "));

    // does it have a "(" e.g. SUM(...)
    string::size_type pos1 = val.find_first_of("(");
    if (pos1 != string::npos) {
      map<string, vector<string> > functions;
      string funcArgs = val.substr(pos1 + 1, val.length() - pos1 - 2);
      vector<string> args;
      boost::split(args, funcArgs, boost::is_any_of(","));
      functions[val.substr(0, pos1)].assign(args.begin() + 1, args.end()); // ignore first value - its the field name
      _return[args[0]] = functions;
    } else {
      map<string, vector<string> > empty;
      _return[val] = empty;
    }

    map<string, vector<string> > innerData;
  }

  // debug output
//  for (map<string, map<string, vector<string> > >::iterator it =
//      _return.begin(); it != _return.end(); ++it) {
//    cout << "first: " << it->first << endl;
//    for (map<string, vector<string> >::iterator iter = it->second.begin();
//        iter != it->second.end(); ++iter) {
//      cout << "second-first:" << iter->first << endl;
//      for (vector<string>::iterator it2 = iter->second.begin();
//          it2 != iter->second.end(); ++it2) {
//        cout << "second-second:" << (*it2) << endl;
//      }
//    }
//  }

  return _return;
}

void CombineStore::configure(pStoreConf configuration, pStoreConf parent) {
  MysqlStore::configure(configuration, parent);

  // Counter Config
  /*if (!configuration->getString("group_separator", groupSeparator)) {
   groupSeparator = "@";
   }*/

  string groupFieldsRaw;
  string valueFieldsRaw;
  if (!configuration->getString("group_fields", groupFieldsRaw)) {
    LOG_OPER("[%s] Bad Config - group_fields not set", categoryHandled.c_str());
    exit(-1);
  }

  if (!configuration->getString("value_fields", valueFieldsRaw)) {
    LOG_OPER("[%s] Bad Config - value_fields not set", categoryHandled.c_str());
    exit(-1);
  }

  groupFields = parseFieldsConfig(groupFieldsRaw);
  valueFields = parseFieldsConfig(valueFieldsRaw);

  configuration->getString("queries", querieTemplates);
}

bool CombineStore::executeQuery(string query) {
  int state;
  state = mysql_query(connection, query.c_str());
  if (state != 0) {
    int errno;
    errno = mysql_errno(mysql);
    if (errno == 1064) {
      LOG_OPER("[%s] Mysql query syntax error: <%s> <%s>",
          categoryHandled.c_str(), query.c_str(), mysql_error(connection));
      return true; // not really successful but query is wrong, we cant change that, so keep going.
    } else {
      cout << "state: " << state << endl;
      cout << mysql_error(connection) << endl;
      cout << query << endl;
      return false;
    }
  }
  return true;
}

bool CombineStore::getColumnStringValue(json_t* jObj, string& _return) {
  int type = json_typeof(jObj);
  stringstream stream;
  switch (type) {
  case JSON_STRING:
    _return = json_string_value(jObj);
    return true;
  case JSON_INTEGER:
    stream << (int64_t) json_integer_value(jObj);
    _return = stream.str();
    return true;
  case JSON_TRUE:
    _return = "true";
    return true;
  case JSON_FALSE:
    _return = "false";
    return true;
  case JSON_REAL:
    stream << (double) json_real_value(jObj);
    _return = stream.str();
    return true;
  default:
    LOG_OPER(
        "[%s] [Cassandra][ERROR] value format not valid - contains NULL value ?",
        categoryHandled.c_str());
    return false;
  }
  return false;
}

string CombineStore::applyFunctionsToValue(fieldsFunctionsType functions,
    string value, string oldValue) {

  // no functions to apply
  if (functions.empty()) {
    trim_if(value, is_any_of(" "));
    return value;
  }
  string _return = "";
  map<string, vector<string> > params;
  BOOST_FOREACH(fieldsFunctionsType::value_type params, functions) {
    trim_if(value, is_any_of(" "));
    if (params.first.compare("date") == 0) {
      struct tm tm;
      char buffer[80];

      if (strptime(value.c_str(), "%Y-%m-%d %H:%M:%S", &tm) == NULL)  {
        // TODO: handle error somehow!
        cout << "could not parse date " << value << endl;
      }

      // target format params.second[0].c_str()
      strftime(buffer, 80, params.second[0].c_str(), &tm);
      value = buffer;
    }
    else if (params.first.compare("SUM") == 0) {
      if (oldValue.length() <= 0) {
        oldValue = "0.0";
      }
      stringstream ss;
      ss << atof(oldValue.c_str()) + atof(value.c_str());
      value = ss.str();
    }
  }

  trim_if(value, is_any_of(" "));
  return value;
}

bool CombineStore::applyValues(map<string, map<string, string> >& data,
    string groupKey, fieldsType valueFields, map<string, string> values) {

  map<string, map<string, string> >::iterator pos = data.find(groupKey);
  if (pos == data.end()) {
    // not found
    map<string, string> newData;
    BOOST_FOREACH(fieldsType::value_type functions, valueFields) {
      newData[functions.first] = applyFunctionsToValue(functions.second, values[functions.first], "");
    }
    data[groupKey] = newData;
  } else {
    // found
    BOOST_FOREACH(fieldsType::value_type functions, valueFields) {
      data[groupKey][functions.first] = applyFunctionsToValue(functions.second, values[functions.first], data[groupKey][functions.first]);
    }
  }

  return true;
}

string CombineStore::getGroupKeyString(fieldsType fields,
    map<string, string> values) {
  string key = "";
  BOOST_FOREACH(fieldsType::value_type field, fields) {
    key.append(applyFunctionsToValue(field.second, values[field.first], ""));
  }
  return key;
}

string CombineStore::doSubstitutions(string const &in, map<string, string> const &subst) {
    ostringstream out;
    size_t pos = 0;
    for (;;) {
        size_t subst_pos = in.find("${", pos );
        size_t end_pos = in.find("}", subst_pos );
        if ( end_pos == string::npos ) break;

        out.write( &* in.begin() + pos, subst_pos - pos );

        subst_pos += strlen("${");
        map<string, string>::const_iterator subst_it
            = subst.find( in.substr( subst_pos, end_pos - subst_pos ) );
        if (subst_it == subst.end()) {
          LOG_OPER("[%s] [Counter][ERROR] substitution failed in <%s> for <%s>", categoryHandled.c_str(), in.c_str(), in.substr( subst_pos, end_pos - subst_pos ).c_str());
          return "";
        }

        out << subst_it->second;
        pos = end_pos + strlen("}");
    }
    out << in.substr( pos, string::npos );
    return out.str();
}

bool CombineStore::handleMessages(
    boost::shared_ptr<logentry_vector_t> messages) {
  if (!isOpen()) {
    if (!open()) {
      return false;
    }
  }
  bool success = true;
  int num_combined = 0;

  // hash of values of group_fields[field]
  map<string, map<string, string> > data;

  unsigned long start = scribe::clock::nowInMsec();
  for (logentry_vector_t::iterator iter = messages->begin();
      iter != messages->end(); ++iter) {

    string message = (*iter)->message;

    map<string, string> values;
    json_error_t error;
    json_t* jsonRoot = json_loads(message.c_str(), 0, &error);
    if (jsonRoot) {
      json_t* jValueObj;
      const char* key;
      json_object_foreach(jsonRoot, key, jValueObj) {
        string curValue;
        getColumnStringValue(jValueObj, curValue);
        values[key] = curValue;
      }
    } else {
      LOG_OPER("[%s] [Counter][ERROR] not a valid JSON-String <%s>",
          categoryHandled.c_str(), message.c_str())
    }
    json_decref(jsonRoot);

    // TODO: error if value-key specified in group/value_fields does not exist

    // create group_key from group_fields and add values to this key
    // std::map<std::string, map<std::string, std::vector<std::string> > >
    string groupKey = getGroupKeyString(groupFields, values);
    applyValues(data, groupKey, valueFields, values);

    num_combined++;
  }

  int num_written = 0;
  if (executeQuery("SET AUTOCOMMIT=0")) {
    for (map<string, map<string, string> >::iterator it = data.begin(); it != data.end(); ++it) {
      pair<string, string> substitutions_init[20];
      int i = 0;
      for (map<string, string>::iterator it2 = it->second.begin(); it2 != it->second.end(); ++it2) {
        substitutions_init[i] = make_pair(it2->first, it2->second);
        i++;
      }

      map<string, string> substitutions
            ( substitutions_init, substitutions_init + sizeof(substitutions_init)/sizeof(*substitutions_init) );

      if (!executeQuery(doSubstitutions(querieTemplates, substitutions))) {
        if (!executeQuery("ROLLBACK")) {
          LOG_OPER("could not ROLLBACK transaction");
        }
        success = false;
        break;
      }
      num_written++;
    }

    if (!executeQuery("COMMIT")) {
      LOG_OPER("Could not commit transaction");
      success = false;
    }
  }
  else {
    success = false;
  }


  unsigned long runtime = scribe::clock::nowInMsec() - start;
  if (!success) {
    close();
    LOG_OPER("[%s] [Counter][mysql][WARNING] could not write messages! Re-queue <%i> messages",
          categoryHandled.c_str(), num_combined);
  }
  else {
    g_Handler->incCounterBy(categoryHandled, "combined into", num_written);
    LOG_OPER("[%s] [Counter][mysql] combined <%i> messages to <%i> queries in <%lu>",
          categoryHandled.c_str(), num_combined, num_written, runtime);
  }

  return success;
}

boost::shared_ptr<Store> CombineStore::copy(const std::string &category) {
  CombineStore *store = new CombineStore(storeQueue, category, multiCategory);
  shared_ptr<Store> copied = shared_ptr<Store>(store);

  store->remoteHost = remoteHost;
  store->remotePort = remotePort;
  store->database = database;
  store->username = username;
  store->password = password;
  store->connection = new MYSQL();
  store->mysql = new MYSQL();
  store->opened = false;

  //store->groupSeparator = groupSeparator;
  store->groupFields = groupFields;
  store->valueFields = valueFields;
  store->querieTemplates = querieTemplates;

  return copied;
}

#endif

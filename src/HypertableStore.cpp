/*
 * HypertableStore.cpp
 *
 * Created on: Aug 15, 2012
 * Author: goir
 *
 */

#ifdef USE_SCRIBE_HYPERTABLE

#include "HypertableStore.h"

using namespace std;
using namespace boost;
using namespace Hypertable;
using namespace Hypertable::ThriftGen;

HypertableStore::HypertableStore(StoreQueue* storeq, const string& category, bool multi_category) :
    Store(storeq, category, "hypertable", multi_category), opened(false) {
  // we can't open the connection until we get configured
}

HypertableStore::~HypertableStore() {
  close();
}

void HypertableStore::configure(pStoreConf configuration, pStoreConf parent) {
  Store::configure(configuration, parent);

  if (!configuration->getString("remote_host", remoteHost)) {
    LOG_OPER("[%s] Bad Config - remote_host not set", categoryHandled.c_str());
  }

  if (!configuration->getInt("remote_port", remotePort)) {
    remotePort = 38080;
  }
}

void HypertableStore::periodicCheck() {
  // nothing for now
}

bool HypertableStore::open() {
  if (isOpen()) {
    return (true);
  }
  opened = true;
  try {
    client = shared_ptr<Thrift::Client>(new Thrift::Client(remoteHost, remotePort));
  } catch (Thrift::TTransportException &e) {
    cout << "HypertableStore::open TTransportException" << e.what() << endl;
    opened = false;
  } catch (std::exception &e) {
    cout << "HypertableStore::open std::exception" << e.what() << endl;
    opened = false;
  }

  if (opened) {
    // clear status on success
    setStatus("");
  } else {
    setStatus("[Hypertable] Failed to connect");
  }
  return opened;
}

void HypertableStore::close() {
  if (opened) {
    LOG_OPER("[%s] [Hypertable] disconnected client", categoryHandled.c_str());
  }
  opened = false;
}

bool HypertableStore::isOpen() {
  return opened;
}

shared_ptr<Store> HypertableStore::copy(const std::string &category) {
  HypertableStore *store = new HypertableStore(storeQueue, category, multiCategory);
  shared_ptr<Store> copied = shared_ptr<Store>(store);

  store->remoteHost = remoteHost;
  store->remotePort = remotePort;
  return copied;
}

bool HypertableStore::handleMessages(boost::shared_ptr<logentry_vector_t> messages) {
  bool success = true;
  if (!isOpen()) {
    success = open();
    if (!success) {
      return false;
    }
  }

  map<string, map<string, vector<Cell> > > cells; // ns < table, cells >
  map<string, map<string, logentry_vector_t> > msgs; // ns < table, string >
  for (logentry_vector_t::iterator iter = messages->begin(); iter != messages->end(); ++iter) {
    string message;
    stringstream gzMessage;
    stringstream rawMessage;

    // detect if message is gzipped
    if ((unsigned int) (*iter)->message[0] == 0x1f && (unsigned int) (*iter)->message[1] == 0xffffff8b) {
      gzMessage << (*iter)->message;
      boost::iostreams::filtering_streambuf<boost::iostreams::input> gzFilter;
      gzFilter.push(boost::iostreams::gzip_decompressor());
      gzFilter.push(gzMessage);
      boost::iostreams::copy(gzFilter, rawMessage);
      message = rawMessage.str();
    } else {
      message = (*iter)->message;
    }

    HypertableDataStruct data = parseJsonMessage(message);

    cells[data.ns][data.table].insert(cells[data.ns][data.table].end(), data.cells.begin(), data.cells.end());
    msgs[data.ns][data.table].push_back(*iter); // keep messages in case the mutation fails so we can retry
  }

  unsigned int tableCount = 0;
  unsigned int cellCount = 0;
  boost::shared_ptr<logentry_vector_t> retryMsgs(new logentry_vector_t);
  if (cells.size() > 0) {
    unsigned long start = scribe::clock::nowInMsec();
    for (map<string, map<string, vector<Cell> > >::iterator nsIter = cells.begin(); nsIter != cells.end(); ++nsIter) {
      string nsName = nsIter->first;
      for (map<string, vector<Cell> >::iterator tableIter = nsIter->second.begin(); tableIter != nsIter->second.end();
          ++tableIter) {
        string tableName = tableIter->first;
        try {
          Namespace ns = client->namespace_open(nsName);
          Mutator m = client->mutator_open(ns, tableName, 0, 0);
          client->mutator_set_cells(m, tableIter->second);
          client->mutator_close(m);
          client->namespace_close(ns);

          tableCount++;
          cellCount += tableIter->second.size();
          for (vector<Cell>::iterator it3 = tableIter->second.begin(); it3 != tableIter->second.end(); ++it3) {
            cout << *it3 << endl;
          }
        } catch (ClientException &e) {
          cout << "HypertableStore::handleMessages ClientException " << e.message << endl;
          success = false;
        } catch (std::exception &e) {
          cout << "HypertableStore::handleMessages std::exception " << e.what() << endl;
          success = false;
        }
        if (!success) {
          retryMsgs->insert(retryMsgs->end(), msgs[nsName][tableName].begin(), msgs[nsName][tableName].end());
        }
      }
    }
    unsigned long runtime = scribe::clock::nowInMsec() - start;

    g_Handler->incCounterBy(categoryHandled, "cells written", cellCount);

    LOG_OPER("[%s] [Hypertable] wrote <%i> cells into <%i> tables in <%lu>ms",
        categoryHandled.c_str(), cellCount, tableCount, runtime);
  }

  if (!retryMsgs->empty()) {
    messages.swap(retryMsgs);
  }

  return retryMsgs->empty();
}

bool HypertableStore::getColumnStringValue(json_t* root, const string key, string &_return) {
  json_t* jObj = (key.empty()) ? root : json_object_get(root, key.c_str());
  if (jObj) {
    int type = json_typeof(jObj);
    stringstream stream;
    switch (type) {
    case JSON_STRING:
      _return = json_string_value(jObj);
      return true;
    case JSON_INTEGER:
      stream << (int64_t) json_integer_value(jObj);
      _return = stream.str().c_str();
      return true;
    case JSON_TRUE:
      _return = "true";
      return true;
    case JSON_FALSE:
      _return = "false";
      return true;
    case JSON_REAL:
      stream << (double) json_real_value(jObj);
      _return = stream.str().c_str();
      return true;
    default:
      LOG_OPER("[%s] [Hypertable][ERROR] value format not valid - contains NULL value ?", categoryHandled.c_str());
      return false;
    }
    return false;
  }
  return false;
}

HypertableStore::HypertableDataStruct HypertableStore::parseJsonMessage(string message) {
  HypertableDataStruct data;
  json_error_t error;
  json_t* jsonRoot = json_loads(message.c_str(), 0, &error);
  if (jsonRoot) {
    getColumnStringValue(jsonRoot, "namespace", data.ns);
    getColumnStringValue(jsonRoot, "table", data.table);

    // get rows
    json_t *jRowDataObj = json_object_get(jsonRoot, "rows");
    if (json_is_array(jRowDataObj)) {
      size_t arrayLength = json_array_size(jRowDataObj);
      for (size_t i = 0; i < arrayLength; i++) {
        data.cells = getCells(json_array_get(jRowDataObj, i), message);
      }
    } else {
      LOG_OPER("[%s] [Hypertable][ERROR] 'rows' is not a list! <%s>", categoryHandled.c_str(), message.c_str());
      json_decref(jsonRoot);
      return HypertableDataStruct();
    }
    json_decref(jsonRoot);
  } else {
    LOG_OPER("[%s] [Hypertable][ERROR] Not a valid JSON String '%s'", categoryHandled.c_str(), message.c_str());
    json_decref(jsonRoot);
    return HypertableDataStruct();
  }

  return data;
}

vector<Hypertable::ThriftGen::Cell> HypertableStore::getCells(json_t *jsonRoot, const string message) {
  vector<Hypertable::ThriftGen::Cell> cells;
  string rowKey;
  string timestamp_;
  string version_;
  const char* version;
  const char* timestamp;

  getColumnStringValue(jsonRoot, "key", rowKey);
  getColumnStringValue(jsonRoot, "timestamp", timestamp_);
  getColumnStringValue(jsonRoot, "version", version_);

  if (rowKey.empty()) {
    LOG_OPER("[%s] [Hypertable][ERROR] Namespace, Table and Key are required! <%s>",
        categoryHandled.c_str(), message.c_str());
    json_decref(jsonRoot);
    return vector<Hypertable::ThriftGen::Cell>();
  }

  // set version to AUTO_ASSIGN if not set or empty
  version = version_.c_str();
  if (version_.empty()) {
    version = NULL;
  }

  // set timestamp to AUTO_ASSIGN if not set or empty
  timestamp = timestamp_.c_str();
  if (timestamp_.empty()) {
    timestamp = NULL;
  }

  // get actual column data
  json_t *dataObj = json_object_get(jsonRoot, "data");
  if (json_is_object(dataObj)) {
    const char *columnKey;
    string columnFamily;
    string columnQualifier;
    json_t* jValueObj;
    json_object_foreach(dataObj, columnKey, jValueObj) {
      string elementValue;
      if (strcmp(columnKey, "") == 0) {
        LOG_OPER("[%s] [Hypertable][ERROR] columnKey <%s> invalid '%s'",
            categoryHandled.c_str(), columnKey, message.c_str());
        return vector<Hypertable::ThriftGen::Cell>();
      }

      if (!getColumnStringValue(jValueObj, "", elementValue)) {
        LOG_OPER("[%s] [Hypertable][ERROR] could not get value for %s <%s>",
            categoryHandled.c_str(), columnKey, message.c_str());
        return vector<Hypertable::ThriftGen::Cell>();
      }

      vector<string> cfSplit; // #2: Search for tokens
      boost::algorithm::split(cfSplit, columnKey, is_any_of(":"), token_compress_on);
      if (cfSplit.size() == 1) {
        columnFamily = cfSplit.at(0);
      } else if (cfSplit.size() == 2) {
        columnFamily = cfSplit.at(0);
        columnQualifier = cfSplit.at(1);
      } else {
        LOG_OPER("[%s] [Hypertable][ERROR] columnKey invalid <%s> - this should not happen <%s>",
            categoryHandled.c_str(), columnKey, message.c_str());
        return vector<Hypertable::ThriftGen::Cell>();
      }

      cells.push_back(
          make_cell(rowKey.c_str(), columnFamily.c_str(), columnQualifier.c_str(), elementValue.c_str(), timestamp,
              version));
    }
  } else {
    LOG_OPER("[%s] [Hypertable][ERROR] data not set - at least one value is required: %s",
        categoryHandled.c_str(), message.c_str());
    return vector<Hypertable::ThriftGen::Cell>();
  }

  return cells;
}

void HypertableStore::flush() {
  // Nothing to do
}

#endif

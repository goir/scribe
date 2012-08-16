/*
 * HypertableStore.cpp
 *
 * Created on: Aug 15, 2012
 * Author: goir
 *
 *      message syntax:
 *
 *      {"namespace": "/test", // defaults to first part of category
 *      "table": "thrift_test", // defaults to last part of category
 *      "key": "Key",
 *      "timestamp": "2012-08-15 00:00:00" // or timestamp in microseconds defaults to time on write
 *      "version": 0 // defaults to 0
 *      "values": {
 *                  "columnfamily:column1": "value1",
 *                  "columnfamily:column2": "value2",
 *                }
 *    // OR all values can be overwritten except namespace and table
 *      "values": {
 *                  {"key": "key", "timestamp": "2012-08-15 00:00:00", "version": "0",
 *                    "cf": "columnfamily:column1", "value": "value1"},
 *                  {"key": "key", "timestamp": "2012-08-15 00:00:00", "version": "0",
 *                    "cf": "columnfamily:column2", "value": "value2"},
 *                }
 *      }
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
  cout << "configure" << endl;
  Store::configure(configuration, parent);

  if (!configuration->getString("remote_host", remoteHost)) {
    LOG_OPER("[%s] Bad Config - remote_host not set", categoryHandled.c_str());
  }

  if (!configuration->getInt("remote_port", remotePort)) {
    remotePort = 38080;
  }
  cout << remoteHost << endl;
  cout << remotePort << endl;

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
    cout << remoteHost << endl;
    cout << remotePort << endl;
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

  vector<Cell> cells;
  for (logentry_vector_t::iterator iter = messages->begin(); iter != messages->end(); ++iter) {
    string message;
    stringstream gzMessage;
    stringstream rawMessage;

    // detect if message is gzipped
    if ((unsigned int) (*iter)->message[0] == 0x1f
        && (unsigned int) (*iter)->message[1] == 0xffffff8b) {
      cout << message << endl;
      gzMessage << (*iter)->message;
      boost::iostreams::filtering_streambuf<boost::iostreams::input> gzFilter;
      gzFilter.push(boost::iostreams::gzip_decompressor());
      gzFilter.push(gzMessage);
      boost::iostreams::copy(gzFilter, rawMessage);
      message = rawMessage.str();
    } else {
      message = (*iter)->message;
    }

    HypertableDataStruct *data = parseJsonMessage(message);

    cout << (*data) << endl;

    cells.insert(cells.end(), data->cells.begin(), data->cells.end());
//    delete[] data;
  }

  if (cells.size() > 0) {
    try {
      unsigned long start = scribe::clock::nowInMsec();
      Namespace ns = client->namespace_open("test");
      Mutator m = client->mutator_open(ns, "thrift_test", 0, 50);
      client->mutator_set_cells(m, cells);
      client->mutator_close(m);
      client->namespace_close(ns);
      unsigned long runtime = scribe::clock::nowInMsec() - start;

      LOG_OPER( "[%s] [Hypertable] wrote <%i> columns in <%lu>",
          categoryHandled.c_str(), cells.size(), runtime);
    } catch (ClientException &e) {
      cout << "HypertableStore::handleMessages ClientException " << e.message << endl;
      success = false;
    } catch (std::exception &e) {
      cout << "HypertableStore::handleMessages std::exception " << e.what() << endl;
      success = false;
    }
  }

  return success;
}

bool HypertableStore::getColumnStringValue(json_t* root, const string key, const string default_,
    string& _return) {
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
      LOG_OPER("[%s] [Hypertable][ERROR] value format not valid - contains NULL value ?",
          categoryHandled.c_str());
      return false;
    }
    return false;
  }
  else {
    if (!default_.empty()) {
      _return = default_;
      return true;
    }
  }
  return false;
}

HypertableStore::HypertableDataStruct* HypertableStore::parseJsonMessage(string message) {
  if (message.empty()) {
    LOG_OPER("[%s] [Hypertable][WARNING] empty Message", categoryHandled.c_str());
    return NULL;
  }

  HypertableDataStruct *data = new HypertableDataStruct();

  json_error_t error;
  json_t* jsonRoot = json_loads(message.c_str(), 0, &error);
  if (jsonRoot) {
    LOG_DBG("json parsed");
    getColumnStringValue(jsonRoot, "key", "", data->key);
    getColumnStringValue(jsonRoot, "timestamp", "", data->timestamp);
    getColumnStringValue(jsonRoot, "version", "0", data->version);

    // get actual column data
    json_t *dataObj = json_object_get(jsonRoot, "data");
    if (json_is_object(dataObj)) {
      if (data->key.empty()) {
        LOG_DBG("[%s] [Hypertable][ERROR] Key is empty <%s>",
            categoryHandled.c_str(), message.c_str());
        return NULL;
      }

      const char* key;
      json_t* jValueObj;
      json_object_foreach(dataObj, key, jValueObj) {
        string elementValue;

        if (!getColumnStringValue(jValueObj, "", "", elementValue)) {
          LOG_DBG("[%s] [Hypertable][ERROR] could not get value for %s",
              categoryHandled.c_str(), key);
          return NULL;
        }
        Hypertable::ThriftGen::Cell cell;
        data->cells.push_back(
            make_cell(data->key.c_str(), key, key, elementValue.c_str(), data->timestamp.c_str(),
                data->version.c_str()));
      }
    } else {
      LOG_OPER("[%s] [Hypertable][ERROR] data not set - at least one value is required: %s",
          categoryHandled.c_str(), message.c_str());
      return NULL;
    }

    json_decref(jsonRoot);
  } else {
    LOG_OPER("[%s] [Hypertable][ERROR] Not a valid JSON String '%s'",
        categoryHandled.c_str(), message.c_str());
    return NULL;
  }

  return data;
}

HypertableStore::HypertableElementStruct HypertableStore::getElement(json_t *jsonObj) {
  HypertableElementStruct element;

  return element;
}

void HypertableStore::flush() {
  // Nothing to do
}

#endif

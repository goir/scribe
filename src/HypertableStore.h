/*
 * HypertableStore.h
 *
 *  Created on: Aug 15, 2012
 *      Author: goir
 */

#ifndef HYPERTABLESTORE_H_
#define HYPERTABLESTORE_H_

#ifdef USE_SCRIBE_HYPERTABLE

#include "common.h"
#include "store.h"
//#include "Common/Compat.h"
//#include "Common/System.h"
#include <boost/foreach.hpp>
#include <jansson.h>
#include <boost/iostreams/filtering_streambuf.hpp>
#include <boost/iostreams/copy.hpp>
#include <boost/iostreams/filter/gzip.hpp>
#include "ThriftBroker/Client.h"
#include "ThriftBroker/ThriftHelper.h"

using namespace std;

/*
 * This store sends messages to a Cassandra Server.
 */
class HypertableStore: public Store {
public:
  HypertableStore(StoreQueue* storeq, const string& category, bool multi_category);
  ~HypertableStore();

  boost::shared_ptr<Store> copy(const string &category);
  bool handleMessages(boost::shared_ptr<logentry_vector_t> messages);
  bool open();
  bool isOpen();
  void configure(pStoreConf configuration, pStoreConf parent);
  void close();
  void flush();
  void periodicCheck();

protected:
  /*      {
   *      "key": "Key",
   *      "timestamp": "2012-08-15 00:00:00" // or timestamp in microseconds defaults to time on write
   *      "version": 0 // defaults to 0
   *      "data": {
   *                  "columnfamily:column1": "value1",
   *                  "columnfamily:column2": "value2",
   *                }
   *    // OR all values can be overwritten except namespace and table
   *      "data": {
   *                  {"key": "key", "timestamp": "2012-08-15 00:00:00", "version": "0",
   *                    "cf": "columnfamily:column1", "value": "value1"},
   *                  {"key": "key", "timestamp": "2012-08-15 00:00:00", "version": "0",
   *                    "cf": "columnfamily:column2", "value": "value2"},
   *                }
   *      }
   */
  struct HypertableDefaultStruct {
    string key;
    string timestamp; // TODO: convert to uint64_t
    string version;
  };

  struct HypertableElementStruct : HypertableDefaultStruct {
    string cf;
    string value;

//    friend ostream& operator<<(ostream& out, const vector<HypertableElementStruct> data) {
//      out << "[" << data.cf << ": " << data.value << "]";
//      return out;
//    }
  };

  struct HypertableDataStruct : HypertableDefaultStruct{
    vector<Hypertable::ThriftGen::Cell> cells;

    friend ostream& operator<<(ostream& out, const HypertableDataStruct data) {
        out << "(key: " << data.key
            << ", timestamp: " << data.timestamp
            << ", version: "
            << data.version
            << ", data: ";

            BOOST_FOREACH(Hypertable::ThriftGen::Cell cell, data.cells) {
              out << cell << endl;
            }
            out << ")";
        return out;
    }
  };

  // configuration
  long int remotePort;
  string remoteHost;
  string ns;
  string table;
  boost::shared_ptr<Hypertable::Thrift::Client> client;

  // state
  bool opened;

private:
  bool getColumnStringValue(json_t* root, const string key, const string default_, string& _return);
  HypertableDataStruct* parseJsonMessage(string message);
  HypertableElementStruct getElement(json_t *jsonObj);

  // disallow copy, assignment, and empty construction
  HypertableStore();
  HypertableStore(HypertableStore& rhs);
  HypertableStore& operator=(HypertableStore& rhs);
};

#endif /* USE_SCRIBE_HYPERTABLE */

#endif /* HYPERTABLESTORE_H_ */

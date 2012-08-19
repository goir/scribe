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
#include "scribe_server.h"
#include <boost/foreach.hpp>
#include <jansson.h>
#include <boost/iostreams/filtering_streambuf.hpp>
#include <boost/iostreams/copy.hpp>
#include <boost/iostreams/filter/gzip.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string.hpp>
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
  /*
{
    "namespace": "ns1",
    "table": "table1",
    "rows": [
        {
            "key": "Key",
            "timestamp": "2012-08-15 00:00:00",
            "version": 0,
            "data": {
                "columnfamily:column1": "value1",
                "columnfamily:column2": "value2"
            }
        }
    ]
}
   */
  struct HypertableDataStruct {
    string ns;
    string table;
    vector<string> hqlQueries; // TODO: support this!
    vector<Hypertable::ThriftGen::Cell> cells;

    bool empty() {
      return (ns.empty() || table.empty());
    }

    friend ostream& operator<<(ostream& out, const HypertableDataStruct data) {
        out << "(ns: " << data.ns
            << ", table: " << data.table
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
  bool getColumnStringValue(json_t* root, const string key, string &_return);
  HypertableDataStruct parseJsonMessage(string message);
  vector<Hypertable::ThriftGen::Cell> getCells(json_t *jsonRoot, const string message);

  // disallow copy, assignment, and empty construction
  HypertableStore();
  HypertableStore(HypertableStore& rhs);
  HypertableStore& operator=(HypertableStore& rhs);
};

#endif /* USE_SCRIBE_HYPERTABLE */

#endif /* HYPERTABLESTORE_H_ */

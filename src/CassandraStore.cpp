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

#ifdef USE_SCRIBE_CASSANDRA

#include "CassandraStore.h"

using namespace std;
using namespace boost;
using namespace boost::iostreams;
using namespace libcassandra;

CassandraStore::CassandraStore(StoreQueue* storeq, const string& category,
        bool multi_category) :
Store(storeq, category, "cassandra", multi_category),
categoryAsCfName(false), keyspace(""), columnFamily(""),
opened(false) {
    // we can't open the connection until we get configured
}

CassandraStore::~CassandraStore() {
    close();
}

void CassandraStore::configure(pStoreConf configuration, pStoreConf parent) {
    Store::configure(configuration, parent);
    // Error checking is done on open()
    if (!configuration->getString("remote_host", remoteHost)) {
        LOG_OPER("[%s] Bad Config - remote_host not set", categoryHandled.c_str());
    }

    if (!configuration->getInt("remote_port", remotePort)) {
        remotePort = DEFAULT_CASSANDRA_PORT;
    }

    if (!configuration->getInt("timeout", timeout)) {
        timeout = DEFAULT_SOCKET_TIMEOUT_MS;
    }

    configuration->getBool("category_as_cf_name", categoryAsCfName);

    if (!configuration->getString("keyspace", keyspace)) {
        LOG_OPER("[%s] Bad Config - Keyspace not set", categoryHandled.c_str());
    }

    if (configuration->getString("column_family", columnFamily)
            && categoryAsCfName) {
        LOG_OPER("[%s] Bad Config - category_is_cf_name = 'yes' and column_family set", categoryHandled.c_str());
    }

    string consistencyLevel_;
    if (configuration->getString("consistency_level", consistencyLevel_)) {
        if (0 == consistencyLevel_.compare("one")) {
            consistencyLevel = org::apache::cassandra::ConsistencyLevel::ONE;
        } else if (0 == consistencyLevel_.compare("quorum")) {
            consistencyLevel = org::apache::cassandra::ConsistencyLevel::QUORUM;
        } else if (0 == consistencyLevel_.compare("local_quorum")) {
            consistencyLevel = org::apache::cassandra::ConsistencyLevel::LOCAL_QUORUM;
        } else if (0 == consistencyLevel_.compare("each_quorum")) {
            consistencyLevel = org::apache::cassandra::ConsistencyLevel::EACH_QUORUM;
        } else if (0 == consistencyLevel_.compare("all")) {
            consistencyLevel = org::apache::cassandra::ConsistencyLevel::ALL;
        } else if (0 == consistencyLevel_.compare("any")) {
            consistencyLevel = org::apache::cassandra::ConsistencyLevel::ANY;
        } else {
            LOG_OPER("[%s] [cassandra] unknown Consistency Level <%s> assuming QUORUM", categoryHandled.c_str(), consistencyLevel_.c_str());
            consistencyLevel = org::apache::cassandra::ConsistencyLevel::QUORUM;
        }
    } else {
        LOG_OPER("[%s] [cassandra] unknown Consistency Level <%s> assuming QUORUM", categoryHandled.c_str(), consistencyLevel_.c_str());
        consistencyLevel = org::apache::cassandra::ConsistencyLevel::QUORUM;
    }
}

void CassandraStore::periodicCheck() {
    // nothing for now
}

bool CassandraStore::open() {
    if (isOpen()) {
        return (true);
    }
    opened = true;
    try {
        CassandraFactory factory(remoteHost, remotePort);
        tr1::shared_ptr<Cassandra> client_(factory.create());
        client = client_;
    } catch (apache::thrift::TException& e) {
        cout << e.what() << endl;
        opened = false;
    } catch (std::exception e) {
        cout << e.what() << endl;
        opened = false;
    }

    if (opened) {
        // clear status on success
        setStatus("");
    } else {
        setStatus("Failed to connect");
    }
    return opened;
}

void CassandraStore::close() {
    if (opened) {
        LOG_OPER("[%s] [cassandra] disconnected client", categoryHandled.c_str());
    }
    opened = false;
}

bool CassandraStore::isOpen() {
    return opened;
}

shared_ptr<Store> CassandraStore::copy(const std::string &category) {
    CassandraStore *store = new CassandraStore(storeQueue, category,
            multiCategory);
    shared_ptr<Store> copied = shared_ptr<Store> (store);
    store->timeout = timeout;
    store->remoteHost = remoteHost;
    store->remotePort = remotePort;
    store->categoryAsCfName = categoryAsCfName;
    store->keyspace = keyspace;
    store->columnFamily = columnFamily;
    store->consistencyLevel = consistencyLevel;

    return copied;
}

bool CassandraStore::handleMessages(
        boost::shared_ptr<logentry_vector_t> messages) {
    bool success = true;
    if (!isOpen()) {
        success = open();
        if (!success) {
            return false;
        }
    }

    vector<CassandraStore::CassandraDataStruct> data;
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
            gzFilter.push(gzip_decompressor());
            gzFilter.push(gzMessage);
            boost::iostreams::copy(gzFilter, rawMessage);
            message = rawMessage.str();
        } else {
            message = (*iter)->message;
        }

        vector<CassandraStore::CassandraDataStruct>* cassandraData = parseJsonMessage(message);
        if (cassandraData == NULL) {
            LOG_OPER("[%s] [Cassandra] could not create insert tuple for <%s>", categoryHandled.c_str(), message.c_str());
        } else {
        	data.insert(data.end(), (*cassandraData).begin(), (*cassandraData).end());
        }
        delete cassandraData;
    }

    vector<Cassandra::ColumnInsertTuple> cit;
    vector<Cassandra::SuperColumnInsertTuple> scit;
    vector<Cassandra::CounterColumnInsertTuple> countercit;
    vector<Cassandra::CounterSuperColumnInsertTuple> counterscit;
    if (data.size() > 0) {
        for (vector<CassandraDataStruct>::iterator iter = data.begin(); iter != data.end(); ++iter) {
            if (iter->superColumnFamily.empty()) {
                if (iter->counter) {
                    countercit.push_back(Cassandra::CounterColumnInsertTuple(iter->columnFamily,
                            iter->rowKey, iter->columnName, (int64_t) atoi(iter->value.c_str())));
                } else {
                    cit.push_back(Cassandra::ColumnInsertTuple(iter->columnFamily,
                                    iter->rowKey, iter->columnName, iter->value));
                }
            } else {
                if (iter->counter) {
                    counterscit.push_back(Cassandra::CounterSuperColumnInsertTuple(iter->columnFamily,
                                            iter->rowKey, iter->superColumnFamily,
                                            iter->columnName, (int64_t) atoi(iter->value.c_str())));
                } else {
                    scit.push_back(
                            Cassandra::SuperColumnInsertTuple(iter->columnFamily,
                                    iter->rowKey, iter->superColumnFamily,
                                    iter->columnName, iter->value));
                }
            }
        }
    }

    if (scit.size() > 0 || cit.size() > 0 || countercit.size() > 0 || counterscit.size() > 0) {
        try {
            client->setKeyspace(keyspace);

            unsigned long start = scribe::clock::nowInMsec();
            client->batchInsert(cit, scit, countercit, counterscit, consistencyLevel);
            unsigned long runtime = scribe::clock::nowInMsec() - start;
            LOG_OPER("[%s] [Cassandra] [%s] wrote <%i> columns and <%i> counter Columns in <%lu>",
                    categoryHandled.c_str(), client->getHost().c_str(),
                    scit.size() + cit.size(), counterscit.size() + countercit.size(), runtime);
        } catch (org::apache::cassandra::InvalidRequestException &ire) {
            cout << "InvalidRequestException" << endl;
            cout << ire.why << endl;
            success = false;
        } catch (apache::thrift::TException &tex) {
            cout << "TException" << endl;
            cout << tex.what() << endl;
            success = false;
        } catch (std::exception &e) {
            cout << "std::exception" << endl;
            cout << e.what() << endl;
            success = false;
        }
    }

    return success;
}

bool CassandraStore::getColumnStringValue(json_t* root, string key,
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
                stream << (int64_t)json_integer_value(jObj);
                _return = stream.str();
                return true;
            case JSON_TRUE:
                _return = "true";
                return true;
            case JSON_FALSE:
                _return = "false";
                return true;
            case JSON_REAL:
                stream << (double)json_real_value(jObj);
                _return = stream.str();
                return true;
            default:
                LOG_OPER("[%s] [Cassandra][ERROR] value format not valid - contains NULL value ?", categoryHandled.c_str());
                return false;
        }
        return false;
    }
    return false;
}

vector<CassandraStore::CassandraDataStruct>* CassandraStore::parseJsonMessage(string message) {
    if (message.empty()) {
        LOG_DBG("empty Message");
        return NULL;
    }

    string rowKey;
    string scName;
    vector<CassandraStore::CassandraDataStruct>* cassandraData = new vector<CassandraStore::CassandraDataStruct>();

    json_error_t error;
    json_t* jsonRoot = json_loads(message.c_str(), 0, &error);
    if (jsonRoot) {
        LOG_DBG("json parsed");
        // get rowKey which is required
        if (!getColumnStringValue(jsonRoot, "rowKey", rowKey)) {
            LOG_OPER("[cassandra][ERROR] rowKey not set %s", message.c_str());
            return NULL;
        }
        LOG_DBG("rowKey: %s", rowKey.c_str());

        // get optional super column name
        string scName;
        getColumnStringValue(jsonRoot, "scName", scName);
        LOG_DBG("scName: %s", scName.c_str());

        // get counter type
        string counter;
        getColumnStringValue(jsonRoot, "counter", counter);
        bool counterColumn = (0 == strcmp(counter.c_str(), "true")
                || 0 == strcmp(counter.c_str(), "yes")
                || 0 == strcmp(counter.c_str(), "1"));

        // get actual column data
        json_t *dataObj = json_object_get(jsonRoot, "data");
        if (json_is_object(dataObj)) {
            const char* key;
            json_t* jValueObj;
            json_object_foreach(dataObj, key, jValueObj) {
                string columnFamily_ = (categoryAsCfName) ? categoryHandled.c_str() : columnFamily;

                string columnValue;
                if (!getColumnStringValue(jValueObj, "", columnValue)) {
                    LOG_DBG("could not get value for %s", key);
                    return NULL;
                }

                CassandraDataStruct cd;

                cd.columnFamily = columnFamily_;
                cd.superColumnFamily = scName;
                cd.rowKey = rowKey;
                cd.columnName = key;
                cd.value = columnValue;
                cd.counter = counterColumn;
                cassandraData->push_back(cd);

                LOG_DBG("key %s", key);
                LOG_DBG("value %s", columnValue.c_str());
            }
        } else {
            LOG_OPER("[cassandra][ERROR] data not set - at least one value is required: %s", message.c_str());
            return NULL;
        }

        json_decref(jsonRoot);
    } else {
        LOG_OPER("[cassandra][ERROR] Not a valid JSON String '%s'", message.c_str());
        return NULL;
    }
    return cassandraData;
}

void CassandraStore::flush() {
    // Nothing to do
}

#endif

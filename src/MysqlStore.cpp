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

#include "MysqlStore.h"

using namespace std;
using namespace boost;

MysqlStore::MysqlStore(StoreQueue* storeq, const std::string& category,
    bool multi_category) :
  Store(storeq, category, "network", multi_category) {
  connection = new MYSQL();
  mysql = new MYSQL();
}

MysqlStore::~MysqlStore() {
}

void MysqlStore::configure(pStoreConf configuration, pStoreConf parent) {
  Store::configure(configuration, parent);
  // Error checking is done on open()
  if (!configuration->getString("remote_host", remoteHost)) {
    LOG_OPER("[%s] Bad Config - remote_host not set", categoryHandled.c_str());
  }

  if (!configuration->getInt("remote_port", remotePort)) {
    remotePort = DEFAULT_MYSQL_PORT;
  }

  if (!configuration->getString("username", username)) {
    LOG_OPER("[%s] Bad Config - username not set", categoryHandled.c_str());
  }

  configuration->getString("password", password);

  if (!configuration->getString("database", database)) {
    LOG_OPER("[%s] Bad Config - database not set", categoryHandled.c_str());
  }

  // start buffering if queries start to take over X seconds
  if (!configuration->getInt("slow_query_threshold", slowQueryThreshold)) {
    LOG_OPER("[%s] slow_query_threshold default to 30 seconds", categoryHandled.c_str());
    slowQueryThreshold = 30;
  }
}

bool MysqlStore::isOpen() {
  return opened;
}

bool MysqlStore::open() {
  connection = mysql_real_connect(mysql, remoteHost.c_str(), username.c_str(),
      password.c_str(), database.c_str(), remotePort, 0, 0);
  if (connection == NULL) {
    string msg = "Failed to Connect ";
    msg.append(mysql_error(mysql));
    cout << msg << endl;
    setStatus(msg);
    opened = false;
    return false;
  }
  setStatus(""); // clear status on success
  opened = true;
  return true;
}

void MysqlStore::close() {
  mysql_close(connection);
  opened = false;
}

bool MysqlStore::handleMessages(boost::shared_ptr<logentry_vector_t> messages) {
  if (!isOpen()) {
    if (!open()) {
      return false;
    }
  }
  bool success = true;
  int num_written = 0;

  unsigned long start = scribe::clock::nowInMsec();
  for (logentry_vector_t::iterator iter = messages->begin(); iter
      != messages->end(); ++iter) {
    int state;
    string message = (*iter)->message;

    // trim leading and trailing double quotes (")
    string::size_type pos1 = message.find_first_not_of('"');
    string::size_type pos2 = message.find_last_not_of('"');
    message = message.substr(pos1 == string::npos ? 0 : pos1, pos2 == string::npos ? message.length() - 1 : pos2 - pos1 + 1);

    state = mysql_query(connection, message.c_str());
    if (state != 0) {
      int errno;
      errno = mysql_errno(mysql);
      if (errno == 1064) {
        LOG_OPER("[%s] Mysql query syntax error: <%s> <%s>", categoryHandled.c_str(), message.c_str(), mysql_error(connection));
      } else {
        cout << "state: " << state << endl;
        cout << mysql_error(connection) << endl;
        cout << message << endl;
        success = false;
        break;
      }
    }
    num_written++;
  }
  long int runtime = scribe::clock::nowInMsec() - start;

  LOG_OPER("[%s] [mysql][INFO] wrote <%i> messages in <%lu>", categoryHandled.c_str(), num_written, runtime);

  if (runtime >= (slowQueryThreshold * 1000)) {
    LOG_OPER("[%s] [mysql][WARNING] last Query took <%lu> while the threshold is at <%li>! Starting buffering!", categoryHandled.c_str(), runtime, (slowQueryThreshold * 1000));
    success = false;
  }

  if (!success) {
    close();

    // update messages to include only the messages that were not handled
    if (num_written > 0) {
      messages->erase(messages->begin(), messages->begin() + num_written);
    }
  }

  return success;
}

void MysqlStore::flush() {
  // noop
}

void MysqlStore::periodicCheck() {
  // noop
}

shared_ptr<Store> MysqlStore::copy(const std::string &category) {
  MysqlStore *store = new MysqlStore(storeQueue, category, multiCategory);
  shared_ptr<Store> copied = shared_ptr<Store> (store);

  store->remoteHost = remoteHost;
  store->remotePort = remotePort;
  store->database = database;
  store->username = username;
  store->password = password;
  store->connection = new MYSQL();
  store->mysql = new MYSQL();
  store->opened = false;
  store->slowQueryThreshold = slowQueryThreshold;

  return copied;
}

#endif

// Copyright (c) 2018 The GAM Authors 


#include <thread>
#include <unistd.h>
#include <cstring>
#include "zmalloc.h"
#include "rdma.h"
#include "master.h"
#include "anet.h"
#include "log.h"
#include "ae.h"
#include "tcp.h"
#include "settings.h"

Master* MasterFactory::server = nullptr;

Master::Master(const Conf& conf)
    : st(nullptr),
      workers(),
      unsynced_workers() {

  this->conf = &conf;

  //get the RDMA resource
  resource = RdmaResourceFactory::getMasterRdmaResource();

  //create the event loop
  el = aeCreateEventLoop(
      conf.maxthreads + conf.maxclients + EVENTLOOP_FDSET_INCR);

  //open the socket for listening to the connections from workers to exch rdma resouces
  char neterr[ANET_ERR_LEN];
  char* bind_addr =
      conf.master_bindaddr.length() == 0 ?
          nullptr : const_cast<char*>(conf.master_bindaddr.c_str());
  sockfd = anetTcpServer(neterr, conf.master_port, bind_addr, conf.backlog);
  if (sockfd < 0) {
    epicLog(LOG_WARNING, "Opening port %s:%d (%s)",
            conf.master_bindaddr.c_str(), conf.master_port, neterr);
    exit(1);
  }

  //register tcp event for rdma parameter exchange
  if (sockfd
      > 0&& aeCreateFileEvent(el, sockfd, AE_READABLE, AcceptTcpClientHandle, this) == AE_ERR) {
    epicPanic("Unrecoverable error creating sockfd file event.");
  }

  //register rdma event
  if (resource->GetChannelFd()
      > 0 && aeCreateFileEvent(el, resource->GetChannelFd(), AE_READABLE, ProcessRdmaRequestHandle, this) == AE_ERR) {
    epicPanic("Unrecoverable error creating sockfd file event.");
  }

  epicLog(LOG_INFO, "start master eventloop\n");
  //create the Master thread to start service
  this->st = new thread(startEventLoop, el);
}

int Master::PostAcceptWorker(int fd, void* data) {
  if (worker_ips.length() == 0) {
    if (1 != write(fd, " ", 1)) {
      epicLog(LOG_WARNING, "Unable to send worker ip list\n");
      return -1;
    }
  } else {
    if (worker_ips.length()
        != write(fd, worker_ips.c_str(), worker_ips.length())) {
      epicLog(LOG_WARNING, "Unable to send worker ip list\n");
      return -1;
    }
    epicLog(LOG_DEBUG, "send: %s", worker_ips.c_str());

    worker_ips.append(",");
  }

  char msg[MAX_WORKERS_STRLEN + 1];
  int n = read(fd, msg, MAX_WORKERS_STRLEN);
  if (n <= 0) {
    epicLog(LOG_WARNING, "Unable to receive worker ip:port\n");
    return -2;
  }
  worker_ips.append(msg, n);
  msg[n] = '\0';

  epicLog(LOG_DEBUG, "received: %s, now worker list is %s (len=%d)\n", msg,
          worker_ips.c_str(), worker_ips.length());
  return 0;
}

Master::~Master() {
  aeDeleteEventLoop(el);
  delete st;
  st = nullptr;
}

void Master::ProcessRequest(Client* client, WorkRequest* wr) {
  epicAssert(wr->wid == client->GetWorkerId());
  switch (wr->op) {
    case UPDATE_MEM_STATS: {
      epicAssert(wr->size);
      Size curr_free = client->GetFreeMem();
      client->SetMemStat(wr->size, wr->free);
      unsynced_workers.push(client);

      widCliMapWorker[client->GetWorkerId()] = client;

      if (unsynced_workers.size() == conf->unsynced_th) {
        WorkRequest lwr { };
        lwr.op = BROADCAST_MEM_STATS;
        char buf[conf->unsynced_th * MAX_MEM_STATS_SIZE + 1];  //op + list + \0
        char send_buf[MAX_REQUEST_SIZE];

        int n = 0;
        while (!unsynced_workers.empty()) {
          Client* lc = unsynced_workers.front();
          n += sprintf(buf + n, "%u:%d:%ld:%ld", lc->GetQP(), lc->GetWorkerId(),
                       lc->GetTotalMem(), lc->GetFreeMem());
          unsynced_workers.pop();
        }
        lwr.size = conf->unsynced_th;
        lwr.ptr = buf;
        int len = 0;
        lwr.Ser(send_buf, len);
        Broadcast(send_buf, len);
      }
      delete wr;
      wr = nullptr;
      break;
    }
    case FETCH_MEM_STATS: {
      //UpdateWidMap();
      epicAssert(widCliMap.size() == qpCliMap.size());
//		if(widCliMapWorker.size() == 1) { //only have the info of the worker, who sends the request
//			break;
//		}
      WorkRequest lwr { };
      lwr.op = FETCH_MEM_STATS_REPLY;
      char buf[(widCliMapWorker.size()) * MAX_MEM_STATS_SIZE + 1];  //op + list + \0
      char* send_buf = client->GetFreeSlot();
      bool busy = false;
      if (send_buf == nullptr) {
        busy = true;
        send_buf = (char *) zmalloc(MAX_REQUEST_SIZE);
        epicLog(LOG_INFO,
                "We don't have enough slot buf, we use local buf instead");
      }

      int n = 0, i = 0;
      for (auto entry : widCliMapWorker) {
        //if(entry.first == client->GetWorkerId()) continue;
        Client* lc = entry.second;
        n += sprintf(buf + n, "%u:%d:%ld:%ld:", lc->GetQP(), lc->GetWorkerId(),
                     lc->GetTotalMem(), lc->GetFreeMem());
        i++;
      }
      lwr.size = i;  //widCliMapWorker.size()-1;
      epicAssert(widCliMapWorker.size() == i);
      lwr.ptr = buf;
      int len = 0, ret;
      lwr.Ser(send_buf, len);
      if ((ret = client->Send(send_buf, len)) != len) {
        epicAssert(ret == -1);
        epicLog(LOG_INFO, "slots are busy");
      }
      epicAssert((busy && ret == -1) || !busy);
      delete wr;
      wr = nullptr;
      break;
    }
    case PUT: {
      void* ptr = zmalloc(wr->size);
      memcpy(ptr, wr->ptr, wr->size);
      kvs[wr->key] = pair<void*, Size>(ptr, wr->size);
      if (to_serve_kv_request.count(wr->key)) {
        int size = to_serve_kv_request[wr->key].size();
        for (int i = 0; i < size; i++) {
          auto& to_serve = to_serve_kv_request[wr->key].front();
          epicAssert(to_serve.second->op == GET);
          epicLog(LOG_DEBUG, "processed to-serve remote kv request for key %ld",
                  to_serve.second->key);
          to_serve.second->flag |= TO_SERVE;
          to_serve_kv_request[wr->key].pop();
          ProcessRequest(to_serve.first, to_serve.second);
        }
        epicAssert(to_serve_kv_request[wr->key].size() == 0);
        to_serve_kv_request.erase(wr->key);
      }
      delete wr;
      wr = nullptr;
      break;
    }
    case GET: {
      if (kvs.count(wr->key)) {
        wr->ptr = kvs.at(wr->key).first;
        wr->size = kvs.at(wr->key).second;
        wr->op = GET_REPLY;
        char* send_buf = client->GetFreeSlot();
        bool busy = false;
        if (send_buf == nullptr) {
          busy = true;
          send_buf = (char *) zmalloc(MAX_REQUEST_SIZE);
          epicLog(LOG_INFO,
                  "We don't have enough slot buf, we use local buf instead");
        }

        int len = 0, ret;
        wr->Ser(send_buf, len);
        if ((ret = client->Send(send_buf, len)) != len) {
          epicAssert(ret == -1);
          epicLog(LOG_INFO, "slots are busy");
        }
        epicAssert((busy && ret == -1) || !busy);
        delete wr;
        wr = nullptr;
      } else {
        to_serve_kv_request[wr->key].push(
            pair<Client*, WorkRequest*>(client, wr));
      }

      break;
    }
    /* add xmx add */
    case CREATE_MUTEX:
    case MUTEX_LOCK:
    case MUTEX_TRY_LOCK:
    case MUTEX_UNLOCK: {
      processRemoteMutex(client, wr);
      break;
    }

    case CREATE_SEM:
    case SEM_POST:
    case SEM_WAIT: {
      processRemoteSem(client, wr);
      break;
    }
    /* add xmx add */
    default:
      epicLog(LOG_WARNING, "unrecognized work request %d", wr->op);
      break;
  }
}

/* add xmx add */
void Master::processRemoteMutex(Client* client, WorkRequest* wr) {
  std::unique_lock<std::mutex> lockGuard{masterMutexMtx};

  if (wr->op == CREATE_MUTEX) {
    uint64_t nameSize = wr->key;
    epicAssert(nameSize < BLOCK_SIZE);
    char strBuf[BLOCK_SIZE]{0};
    memcpy(strBuf, wr->ptr, nameSize);
    strBuf[nameSize] = 0;
    epicInfo("received CREATE_MUTEX. length=%lu, name=%s", nameSize, strBuf);

    std::string mutexName{strBuf};
    uint64_t key;
    if (mutexNameToKey.find(mutexName) != mutexNameToKey.end()) {
      key = mutexNameToKey[mutexName];
      epicInfo("mutex %s(%lu) already existed", strBuf, key);
    } else {
      mutexNameToKey[mutexName] = nextMutexKey++;
      key = mutexNameToKey[mutexName];
      mutexKeyToMutex.emplace(key, MasterMutex{});
      epicInfo("created mutex %s(%lu)", strBuf, key);
    }

    wr->op = MUTEX_REPLY;
    wr->key = key;
    wr->status = SUCCESS;
    sendRequest(client, wr);
    delete wr;
  } else if (wr->op == MUTEX_LOCK) {
    if (mutexKeyToMutex.find(wr->key) != mutexKeyToMutex.end()) {
      auto &mutex = mutexKeyToMutex[wr->key];
      if (!mutex.locked) {
        mutex.locked = true;
        wr->op = MUTEX_REPLY;
        wr->status = SUCCESS;
        sendRequest(client, wr);
      } else { // locked
        mutex.waitList.emplace_back(client->GetWorkerId(), wr->id);
      }
    } else { // no mutex
      wr->op = MUTEX_REPLY;
      wr->status = LOCK_FAILED;
      sendRequest(client, wr);
      epicWarning("lock mutex(%lu) failed: not existed", wr->key);
    }

    delete wr;
  } else if (wr->op == MUTEX_TRY_LOCK) {
    if (mutexKeyToMutex.find(wr->key) != mutexKeyToMutex.end()) {
      auto &mutex = mutexKeyToMutex[wr->key];
      wr->op = MUTEX_REPLY;
      if (!mutex.locked) {
        mutex.locked = true;
        wr->status = SUCCESS;
      } else { // locked
        wr->status = LOCK_FAILED;
      }

    } else { // no mutex
      wr->op = MUTEX_REPLY;
      wr->status = LOCK_FAILED;
      epicWarning("try lock mutex(%lu) failed: not existed", wr->key);
    }

    sendRequest(client, wr);
    delete wr;
  } else if (wr->op == MUTEX_UNLOCK) {
    if (mutexKeyToMutex.find(wr->key) != mutexKeyToMutex.end()) {
      auto &mutex = mutexKeyToMutex[wr->key];
      wr->op = MUTEX_REPLY;
      wr->status = SUCCESS;

      if (mutex.locked) {
        if (mutex.waitList.empty()) {
          mutex.locked = false;
        } else { // 唤醒下一个worker
          auto toBeNotified = mutex.waitList.front();
          mutex.waitList.pop_front();
          Client *notifiedClient = FindClientWid(toBeNotified.first);

          unsigned int currWRId = wr->id;
          wr->id = toBeNotified.second;
          sendRequest(notifiedClient, wr);

          wr->id = currWRId;
        } // waitQueue is not empty
      } else { // not locked
        epicWarning("mutex (%lu) is not locked", wr->key);
      }
    } else { // no mutex
      wr->op = MUTEX_REPLY;
      wr->status = ERROR;
      epicWarning("unlock mutex(%lu) failed: not existed", wr->key);
    }

    sendRequest(client, wr);
    delete wr;
  } else {
    epicFatal("unsupported operation: %s(%d)", ToCString(wr->op), wr->op);
    epicAssert(false);
  }
}

void Master::processRemoteSem(Client* client, WorkRequest* wr) {
  std::unique_lock<std::mutex> lockGuard{masterSemMtx};

  if (wr->op == CREATE_SEM) {
    uint64_t nameSize = wr->key;
    auto value = static_cast<decltype(MasterSem::value)>(wr->size);
    epicAssert(nameSize < BLOCK_SIZE);
    char strBuf[BLOCK_SIZE]{0};
    memcpy(strBuf, wr->ptr, nameSize);
    strBuf[nameSize] = 0;
    epicInfo("received CREATE_SEM. length=%lu, name=%s, value=%d", nameSize, strBuf, value);

    std::string semName{strBuf};
    uint64_t key;
    if (semNameToKey.find(semName) != semNameToKey.end()) {
      key = semNameToKey[semName];
      value = semKeyToSem[key].value;
      epicInfo("semaphore %s(%lu) already existed. value=%d", strBuf, key, value);
    } else { // sem不存在
      semNameToKey[semName] = nextSemKey++;
      key = semNameToKey[semName];
      semKeyToSem.emplace(key, MasterSem{value, {}});
      epicInfo("created semaphore %s(key=%lu, value=%d)", strBuf, key, value);
      if (value < 0) {
        epicWarning("initial semaphore value is negative(%d)", value);
      }
    }

    wr->op = SEM_REPLY;
    wr->key = key;
    wr->size = static_cast<decltype(wr->size)>(value);
    wr->status = SUCCESS;
    sendRequest(client, wr);
    delete wr;
  } else if (wr->op == SEM_WAIT) {
    if (semKeyToSem.find(wr->key) != semKeyToSem.end()) {
      auto &sem = semKeyToSem[wr->key];
      --sem.value;
      if (sem.value < 0) { // blocked
        sem.waitQueue.emplace(client->GetWorkerId(), wr->id);
      } else { // not blocked
        wr->op = SEM_REPLY;
        wr->size = static_cast<Size>(sem.value);
        wr->status = SUCCESS;
        sendRequest(client, wr);
      }
    } else { // no semaphore
      wr->op = SEM_REPLY;
      wr->status = LOCK_FAILED;
      sendRequest(client, wr);
      epicWarning("wait semaphore(%lu) failed: not existed", wr->key);
    }

    delete wr;
  } else if (wr->op == SEM_POST) {
    if (semKeyToSem.find(wr->key) != semKeyToSem.end()) {
      auto &sem = semKeyToSem[wr->key];
      wr->op = SEM_REPLY;
      wr->status = SUCCESS;

      wr->size = static_cast<decltype(wr->size)>(++sem.value);
      if (sem.value <= 0) {
        // 唤醒下一个worker
        auto toBeNotified = sem.waitQueue.front();
        sem.waitQueue.pop();
        Client *notifiedClient = FindClientWid(toBeNotified.first);

        unsigned int currWRId = wr->id;
        wr->id = toBeNotified.second;
        sendRequest(notifiedClient, wr);

        wr->id = currWRId;
        // waitQueue is not empty
      } else { // not blocked
        epicWarning("semaphore (%lu) is not blocked", wr->key);
      }
    } else { // no semaphore
      wr->op = SEM_REPLY;
      wr->status = ERROR;
      epicWarning("post semaphore(%lu) failed: not existed", wr->key);
    }

    sendRequest(client, wr);
    delete wr;
  } else {
    epicFatal("unsupported operation: %s(%d)", ToCString(wr->op), wr->op);
    epicAssert(false);
  }
}

void Master::sendRequest(Client *client, WorkRequest *wr) {
  wr->wid = GetWorkerId();
  char* sendBuf = client->GetFreeSlot();
  bool busy = false;
  if (sendBuf == nullptr) {
    busy = true;
    sendBuf = (char *) zmalloc(MAX_REQUEST_SIZE);
    epicWarning("We don't have enough slot buf, use local buf instead");
  }

  int len = 0;
  ssize_t ret;
  wr->Ser(sendBuf, len);
  if ((ret = client->Send(sendBuf, len)) != len) {
    epicAssert(ret == -1);
    epicWarning("slots are busy");
  }
  epicAssert((busy && ret == -1) || !busy);
}
/* add xmx add */

void Master::Broadcast(const char* buf, size_t len) {
  auto lt = qpCliMap.lock_table();
  for (auto entry : lt) {
    char* send_buf = entry.second->GetFreeSlot();
    bool busy = false;
    if (send_buf == nullptr) {
      busy = true;
      send_buf = (char *) zmalloc(MAX_REQUEST_SIZE);
      epicLog(LOG_INFO,
              "We don't have enough slot buf, we use local buf instead");
    }
    memcpy(send_buf, buf, len);

    size_t sent = entry.second->Send(send_buf, len);
    epicAssert((busy && sent == -1) || !busy);
    if (len != sent) {
      epicAssert(sent == -1);
      epicLog(LOG_INFO, "broadcast to %d failed (expected %d, but %d)\n", len,
              sent);
    }
  }
}

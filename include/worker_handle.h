// Copyright (c) 2018 The GAM Authors 


#ifndef INCLUDE_WORKER_HANDLE_H_
#define INCLUDE_WORKER_HANDLE_H_

#include <mutex>
#include <atomic>

#include "lockwrapper.h"
#include "worker.h"
#include "workrequest.h"

class WorkerHandle {
  boost::lockfree::queue<WorkRequest*>* wqueue;  //work queue used to communicate with worker
  Worker* worker;
  //app-side pipe fd
  int send_pipe[2];
  int recv_pipe[2];
  static LockWrapper lock;
#ifdef USE_PTHREAD_COND
  pthread_mutex_t cond_lock;
  pthread_cond_t cond;
#endif
#if !(defined(USE_PIPE_W_TO_H) && defined(USE_PIPE_H_TO_W))
  volatile int* notify_buf;
  int notify_buf_size;
#endif
  public:
  WorkerHandle(Worker* w);
  void RegisterThread();
  void DeRegisterThread();
  int SendRequest(WorkRequest* wr);
  inline int GetWorkerId() {
    return worker->GetWorkerId();
  }
  int GetWorkersSize() {
    return worker->GetWorkersSize();
  }
  inline void* GetLocal(GAddr addr) {
    return worker->ToLocal(addr);
  }

  /* add xmx add */
  uint64_t getTransferredBytes() const {
    return worker->getTransferredBytes();
  }

  Size getWorkerPendingWrites(WorkRequest *wr = nullptr) {
#ifdef SUB_BLOCK
    return worker->pendingWrites.load();
#else // !SUB_BLOCK
    if (!wr) {
      epicFatal("wr shouldn't be nullptr");
      epicAssert(false);
    }
    return worker->getFence(wr->fd)->pending_writes.load();
#endif // SUB_BLOCK
  }
  /* add xmx add */

  void ReportCacheStatistics();
  void ResetCacheStatistics();

  ~WorkerHandle();
};

#endif /* INCLUDE_WORKER_HANDLE_H_ */

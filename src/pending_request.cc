// Copyright (c) 2018 The GAM Authors 


#include <cstring>
#include <utility>
#include <queue>
#include "rdma.h"
#include "worker.h"
#include "anet.h"
#include "log.h"
#include "ae.h"
#include "client.h"
#include "util.h"
#include "structure.h"
#include "ae.h"
#include "tcp.h"
#include "slabs.h"
#include "zmalloc.h"
#include "kernel.h"
#include "chars.h"

void Worker::ProcessPendingRead(Client* cli, WorkRequest* wr) {
  /* add xmx add */
  if (wr->flag & Write_shared) {
    processPendingReadSubBlock(cli, wr);
    return;
  }
  /* add xmx add */
  epicAssert(wr->parent);
  epicAssert(
      (IsLocal(wr->addr) && wr->op == FETCH_AND_SHARED)
          || (!IsLocal(wr->addr) && wr->op == READ));
  //parent request is from local app or remote worker
  WorkRequest* parent = wr->parent;
  CacheLine* cline = nullptr;
  DirEntry* entry = nullptr;
  GAddr blk = TOBLOCK(wr->addr);
#ifndef SELECTIVE_CACHING
  epicAssert(blk == wr->addr);
#endif

  parent->lock();
  if (wr->flag & CACHED) {
    epicAssert(!IsLocal(wr->addr));
    cache.lock(blk);
    cline = cache.GetCLine(wr->addr);
    epicAssert(cline);
  } else if (IsLocal(wr->addr)) {
    directory.lock(ToLocal(wr->addr));
    entry = directory.GetEntry(ToLocal(wr->addr));
    epicAssert(entry);
  } else {
    epicLog(LOG_WARNING, "shouldn't happen");
    epicAssert(false);
  }

  if (!(wr->flag & LOCKED)) {
    GAddr pend = GADD(parent->addr, parent->size);
    GAddr end = GADD(wr->addr, wr->size);
    GAddr gs = wr->addr > parent->addr ? wr->addr : parent->addr;
    void* ls = (void*) ((ptr_t) parent->ptr + GMINUS(gs, parent->addr));
    void* cs = (void*) ((ptr_t) wr->ptr + GMINUS(gs, wr->addr));
    Size len = end > pend ? GMINUS(pend, gs) : GMINUS(end, gs);
    memcpy(ls, cs, len);
  }

  //update the cache or directory states
  if (!(wr->flag & REPEATED)) {
    if ((wr->flag & CACHED)) {  //read is issued by the cache (remote memory)
      epicAssert(wr->op == READ);
      epicAssert(!IsLocal(wr->addr));
#ifdef SELECTIVE_CACHING
      if(wr->flag & NOT_CACHE) {
        cache.ToNotCache(cline);
      } else {
        cache.ToShared(cline);
      }
#else
      cache.ToShared(cline);
#endif
    } else if (IsLocal(wr->addr)) {  //read is issued by local worker (local memory)
      epicAssert(wr->op == FETCH_AND_SHARED);
      directory.ToShared(entry, Gnullptr);
    } else {
      epicLog(LOG_WARNING, "unexpected!!!");
    }

    int ret = ErasePendingWork(wr->id);
    epicAssert(ret);
  }

  if (wr->flag & LOCKED) {  //RLOCK
    epicAssert(
        !(wr->flag & NOT_CACHE) && wr->addr == blk && wr->size == BLOCK_SIZE);
    epicAssert(
        RLOCK == parent->op && 1 == parent->counter && 0 == parent->size);
    if (wr->flag & CACHED) {  //RLOCK is issued by the cache (remote memory)
      epicAssert(wr->ptr == cline->line);
      epicAssert(!IsLocal(wr->addr));
      int ret = cache.RLock(cline, parent->addr);
      epicAssert(!ret);  //first rlock must be successful
    } else if (IsLocal(wr->addr)) {  //RLock is issued by local worker (local memory)
      epicAssert(ToLocal(wr->addr) == wr->ptr);
      int ret;
      if (entry) {
        ret = directory.RLock(entry, ToLocal(parent->addr));
      } else {
        ret = directory.RLock(ToLocal(parent->addr));  //the dir entry may be deleted
      }
      epicAssert(!ret);  //first rlock must be successful
    } else {
      epicLog(LOG_WARNING, "unexpected!!!");
    }
  }

  if (wr->flag & CACHED) {
    epicAssert(!IsLocal(wr->addr));
    cache.unlock(blk);
  } else if (IsLocal(wr->addr)) {
    directory.unlock(ToLocal(wr->addr));
  } else {
    epicLog(LOG_WARNING, "shouldn't happen");
    epicAssert(false);
  }

  if (--parent->counter == 0) {  //read all the data
    parent->unlock();
    Notify(parent);
  } else {
    parent->unlock();
  }

  ProcessToServeRequest(wr);
  delete wr;
  wr = nullptr;
}

void Worker::ProcessPendingReadForward(Client* cli, WorkRequest* wr) {
/* add xmx add */
  if (wr->flag & Write_shared) {
    processPendingReadForward(cli, wr);
    return;
  }
  /* add xmx add */
#ifdef SELECTIVE_CACHING
  epicAssert(!(wr->flag & NOT_CACHE));
#endif
  epicAssert(wr->parent);
  epicAssert(IsLocal(wr->addr));  //I'm the home node
  //parent request is from local node
  WorkRequest* parent = wr->parent;
  void* laddr = ToLocal(wr->addr);

  directory.lock(laddr);
  DirEntry* entry = directory.GetEntry(laddr);
  directory.ToShared(entry, Gnullptr);
  directory.ToShared(entry, FindClientWid(wr->pwid)->ToGlobal(parent->ptr));
  directory.unlock(laddr);
  //pending_works.erase(wr->id);
  int ret = ErasePendingWork(wr->id);
  epicAssert(ret);

  ProcessToServeRequest(wr);
  delete wr;
  delete parent;
  wr = nullptr;
  parent = nullptr;
}

void Worker::ProcessPendingWrite(Client* cli, WorkRequest* wr) {
/* add xmx add */
  if (wr->flag & Write_shared) {
    processPendingWriteSubBlock(cli, wr);
    return;
  }
  /* add xmx add */
#ifdef SELECTIVE_CACHING
  wr->addr = TOBLOCK(wr->addr);
#endif
  epicAssert(
      (wr->op == WRITE || wr->op == WRITE_PERMISSION_ONLY)
          xor IsLocal(wr->addr));
  WorkRequest* parent;
  CacheLine* cline = nullptr;
  DirEntry* entry = nullptr;
  parent = wr->parent;
  parent->lock();
  if (wr->flag & CACHED) {
    epicAssert(!IsLocal(wr->addr));
    cache.lock(wr->addr);
    cline = cache.GetCLine(wr->addr);
    epicAssert(cline);
  } else if (IsLocal(wr->addr)) {
    directory.lock(ToLocal(wr->addr));
    entry = directory.GetEntry(ToLocal(wr->addr));
    epicAssert(entry);
  } else {
    epicLog(LOG_WARNING, "shouldn't happen");
    epicAssert(false);
  }
  wr->lock();

  if (!(wr->flag & REPEATED) && !(wr->flag & REQUEST_DONE)) {
    if (WID(wr->addr) == cli->GetWorkerId()) {  //from home node, Case 4
      wr->counter++;
    } else {
      wr->counter--;
    }
  }

  //failed case
  if (wr->status) {  //failed from one of the responders
    epicLog(LOG_INFO, "failed case after-processing");
    epicAssert((wr->flag & LOCKED) && (wr->flag & TRY_LOCK));
    epicAssert(wr->status == LOCK_FAILED);
    epicAssert(wr->op != FETCH_AND_INVALIDATE);
    if ((wr->flag & CACHED)) {  //write is issued by the cache (remote memory)
      epicAssert(wr->op == WRITE || wr->op == WRITE_PERMISSION_ONLY);
      epicAssert(!IsLocal(wr->addr));
    } else if (IsLocal(wr->addr)) {  //write is issued by local worker (local memory)
      epicAssert(wr->op == INVALIDATE);
      epicAssert(wr->ptr == ToLocal(wr->addr));
      epicAssert(directory.GetState(entry) == DIR_TO_UNSHARED);
      directory.Remove(entry, cli->GetWorkerId());
      epicAssert(directory.GetState(entry) != DIR_UNSHARED);  //not possible to erase the entry
    } else {
      epicLog(LOG_WARNING, "unexpected");
      epicAssert(false);
    }

    if (wr->counter == 0) {
      epicLog(LOG_INFO, "failed case final-processing");
      //undo the directory/cache changes
      if (WRITE == wr->op) {
        cache.ToInvalid(cline);
      } else if (WRITE_PERMISSION_ONLY == wr->op) {
        cache.UndoShared(cline);
      } else if (FETCH_AND_INVALIDATE == wr->op) {
        epicAssert(wr->ptr == ToLocal(wr->addr));
        directory.UndoDirty(entry);
      } else {  //INVALIDATE
        epicAssert(wr->ptr == ToLocal(wr->addr));
        directory.UndoShared(entry);
      }

      wr->unlock();
      //unlock before process other requests
      if (wr->flag & CACHED) {
        epicAssert(!IsLocal(wr->addr));
        cache.unlock(wr->addr);
      } else if (IsLocal(wr->addr)) {
        directory.unlock(ToLocal(wr->addr));
      } else {
        epicLog(LOG_WARNING, "shouldn't happen");
        epicAssert(false);
      }

      --wr->parent->counter;
      epicAssert(wr->parent->counter == 0);  //lock is guaranteed to be only one block
      parent->unlock(); // unlock earlier
      // Notify() should be called in the very last after all usage of parent,
      // since the app thread may exit the function and release the memory of parent
      Notify(wr->parent);
      wr->parent = nullptr;

      epicAssert(!wr->next);
      ProcessToServeRequest(wr);
      //pending_works.erase(wr->id);
      int ret = ErasePendingWork(wr->id);
      epicAssert(ret);
      delete wr;
      wr = nullptr;
    } else {
      wr->unlock();
      parent->unlock(); // unlock earlier
      if (wr->flag & CACHED) {
        epicAssert(!IsLocal(wr->addr));
        cache.unlock(wr->addr);
      } else if (IsLocal(wr->addr)) {
        directory.unlock(ToLocal(wr->addr));
      } else {
        epicLog(LOG_WARNING, "shouldn't happen");
        epicAssert(false);
      }
    }
    // parent->unlock(); // @wentian: originally here
    return;
  }

  if ((wr->flag & CACHED)) {  //write is issued by the cache (remote memory)
    epicAssert(wr->op == WRITE || wr->op == WRITE_PERMISSION_ONLY);
    epicAssert(!IsLocal(wr->addr));
    epicAssert(!IsLocal(wr->addr));
  } else if (IsLocal(wr->addr)) {  //write is issued by local worker (local memory)
    epicAssert(wr->ptr == ToLocal(wr->addr));
    epicAssert(directory.GetState(entry) == DIR_TO_UNSHARED);
    directory.Remove(entry, cli->GetWorkerId());
  } else {
    epicLog(LOG_WARNING, "unexpected");
    epicAssert(false);
  }

  //normal process below
  epicLog(LOG_DEBUG, "wr->counter after = %d", wr->counter.load());
  epicAssert(parent);
  if (wr->counter == 0 || (wr->flag & REQUEST_DONE)) {

#ifdef SELECTIVE_CACHING
    if(!(wr->flag & NOT_CACHE)) {
#endif

    if (!(wr->flag & LOCKED)) {
      GAddr pend = GADD(parent->addr, parent->size);
      GAddr end = GADD(wr->addr, wr->size);
      GAddr gs = wr->addr > parent->addr ? wr->addr : parent->addr;
      void* ls = (void*) ((ptr_t) parent->ptr + GMINUS(gs, parent->addr));
      void* cs = (void*) ((ptr_t) wr->ptr + GMINUS(gs, wr->addr));
      Size len = end > pend ? GMINUS(pend, gs) : GMINUS(end, gs);
      //we blindly copy the data again
      //as while we are waiting for the reply,
      //there may be a race causes the current op to be canceled or renamed (WRITE_PERMISSION_ONLY to WRITE)
#ifdef GFUNC_SUPPORT
      if (wr->flag & GFUNC) {
        epicAssert(wr->gfunc);
        epicAssert(TOBLOCK(wr->addr) == TOBLOCK(GADD(wr->addr, wr->size-1)));
        void* laddr = cs;
        wr->gfunc(laddr, wr->arg);
      } else {
#endif
        memcpy(cs, ls, len);
#ifdef GFUNC_SUPPORT
      }
#endif
    }

#ifdef SELECTIVE_CACHING
  }
#endif

    if (!(wr->flag & REPEATED)) {
      if ((wr->flag & CACHED)) {  //write is issued by the cache (remote memory)
        epicAssert(wr->op == WRITE || wr->op == WRITE_PERMISSION_ONLY);
        epicAssert(!IsLocal(wr->addr));
#ifdef SELECTIVE_CACHING
        if(wr->flag & NOT_CACHE) {
          epicAssert(wr->op != WRITE_PERMISSION_ONLY);
          cache.ToNotCache(cline, true);
        } else {
          // do logging here
          // logWrite(cline->addr, BLOCK_SIZE, cline->line);
          cache.ToDirty(cline);
        }
#else
        // do logging here
        // logWrite(cline->addr, BLOCK_SIZE, cline->line);
        cache.ToDirty(cline);
#endif
      } else if (IsLocal(wr->addr)) {  //write is issued by local worker (local memory)
        directory.ToUnShared(entry);
      } else {
        epicLog(LOG_WARNING, "shouldn't happen");
        epicAssert(false);
      }

      //clear the pending structures
      int ret = ErasePendingWork(wr->id);
      epicAssert(ret);
    }

    if (wr->flag & LOCKED) {  //WLOCK
      epicLog(LOG_DEBUG, "parent->op = %d, parent->counter = %d", parent->op,
              parent->counter.load());
      epicAssert(WLOCK == parent->op && 1 == parent->counter);
    }

    //TODO: we should process the to_serve requests and pending requests first
    //then process the fenced requests
    //for now, it's ok since write will prevent fenced requests to be processed
    //and read is blocking
    bool notify = false;
    epicLog(LOG_DEBUG, "parent->counter = %d", parent->counter.load());
    if (--parent->counter == 0) {  //write all the blocks
      if (WLOCK == parent->op) {
        if (wr->flag & CACHED) {
          epicAssert(!IsLocal(wr->addr));
          epicAssert(wr->ptr == cline->line);
          if (cache.WLock(cline, parent->addr)) {  //lock failed
            epicAssert(
                wr->op == WRITE_PERMISSION_ONLY
                    && cache.IsRLocked(cline, parent->addr));  //must be shared locked before and now
            epicLog(LOG_INFO, "cannot lock addr %lx, will try later", wr->addr);
            AddToServeLocalRequest(wr->addr, parent);
          } else {
            notify = true;
          }
        } else if (IsLocal(wr->addr)) {
          epicAssert(ToLocal(wr->addr) == wr->ptr);
          int ret;
          if (entry) {
            ret = directory.WLock(entry, ToLocal(parent->addr));
          } else {
            ret = directory.WLock(ToLocal(parent->addr));
          }
          if (ret) {  //lock failed
            epicLog(LOG_INFO, "cannot lock addr %lx, will try later", wr->addr);
            AddToServeLocalRequest(wr->addr, parent);
          } else {
            notify = true;
          }
        } else {
          epicLog(LOG_WARNING, "unexpected!!!");
        }
      } else {
        notify = true;
      }
    }

    wr->unlock();
    if (wr->flag & CACHED) {
      epicAssert(!IsLocal(wr->addr));
      cache.unlock(wr->addr);
    } else if (IsLocal(wr->addr)) {
      directory.unlock(ToLocal(wr->addr));
    } else {
      epicLog(LOG_WARNING, "shouldn't happen");
      epicAssert(false);
    }
    parent->unlock();

    if (notify)
      Notify(parent);

    ProcessToServeRequest(wr);
    delete wr;
    wr = nullptr;
  } else {
    wr->unlock();
    //don't forget to unlock
    if (wr->flag & CACHED) {
      epicAssert(!IsLocal(wr->addr));
      cache.unlock(wr->addr);
    } else if (IsLocal(wr->addr)) {
      directory.unlock(ToLocal(wr->addr));
    } else {
      epicLog(LOG_WARNING, "shouldn't happen");
      epicAssert(false);
    }
    parent->unlock();
  }
}

void Worker::ProcessPendingWriteForward(Client* cli, WorkRequest* wr) {
/* add xmx add */
  if (wr->flag & Write_shared) {
    processPendingWriteForward(cli, wr);
    return;
  }
  /* add xmx add */
  epicAssert(wr->parent);
  epicAssert(IsLocal(wr->addr));  //I'm the home node
  WorkRequest* parent = wr->parent;
  epicAssert(wr->pid == parent->id);

  void* laddr = ToLocal(wr->addr);
  Client* lcli = FindClientWid(wr->pwid);
  epicAssert(BLOCK_ALIGNED(wr->addr));

#ifdef SELECTIVE_CACHING
  if(wr->flag & NOT_CACHE) {
    directory.lock(laddr);
#ifdef GFUNC_SUPPORT
    if(wr->flag & GFUNC) {
      epicAssert(parent->gfunc);
      epicAssert(TOBLOCK(parent->addr) == TOBLOCK(GADD(parent->addr, parent->size-1)));
      void* laddr = ToLocal(parent->addr);
      wr->gfunc(laddr, wr->arg);
    } else {
#endif
      memcpy(ToLocal(parent->addr), parent->ptr, parent->size);
#ifdef GFUNC_SUPPORT
    }
#endif
    directory.ToUnShared(laddr);
    directory.unlock(laddr);

    parent->op = WRITE_REPLY;
    parent->status = SUCCESS;
    SubmitRequest(lcli, parent);
  } else {
#endif

  directory.lock(laddr);
  logOwner(lcli->GetWorkerId(), wr->addr);
  directory.ToDirty(laddr, lcli->ToGlobal(parent->ptr));
  directory.unlock(laddr);

  //TOOD: add completion check
  lcli->WriteWithImm(nullptr, nullptr, 0, wr->pid);  //ack the ownership change

#ifdef SELECTIVE_CACHING
}
#endif

  int ret = ErasePendingWork(wr->id);
  epicAssert(ret);
  ProcessToServeRequest(wr);
  delete wr;
  delete parent;
  wr = nullptr;
  parent = nullptr;
}

void Worker::ProcessPendingEvictDirty(Client* cil, WorkRequest* wr) {
  cache.to_evicted--;
  cache.lock(wr->addr);
  cache.ToInvalid(wr->addr);
  cache.unlock(wr->addr);
  int ret = ErasePendingWork(wr->id);
  epicAssert(ret);
  ProcessToServeRequest(wr);
  delete wr;
  wr = nullptr;
}

void Worker::ProcessPendingInvalidateForward(Client* cli, WorkRequest* wr) {
  /* add xmx add */
  if (wr->flag & Write_shared) {
    processPendingInvalidateForward(cli, wr);
    return;
  }
  /* add xmx add */
  WorkRequest* parent = wr->parent;
  epicAssert(parent);
  epicAssert(TOBLOCK(parent->addr) == wr->addr);
  epicAssert(wr->size == BLOCK_SIZE);
  parent->lock();

  void* laddr = ToLocal(wr->addr);
  directory.lock(laddr);
  wr->lock();

  epicAssert(IsLocal(wr->addr));
  epicLog(LOG_DEBUG, "wr->counter before = %d", wr->counter.load());
  epicAssert(!(wr->flag & REPEATED) && !(wr->flag & REQUEST_DONE));
  wr->counter--;
  epicLog(LOG_DEBUG, "wr->counter after = %d", wr->counter.load());

  DirEntry* entry = directory.GetEntry(ToLocal(wr->addr));

  epicAssert(directory.GetState(entry) == DIR_TO_DIRTY);
  directory.Remove(entry, cli->GetWorkerId());

  //failed case
  if (wr->status) {  //failed from one of the responders
    epicLog(LOG_INFO, "INVALIDATE_FORWARD: failed case after-processing");
    epicAssert((wr->flag & LOCKED) && (wr->flag & TRY_LOCK));
    epicAssert(wr->status == LOCK_FAILED);
    epicAssert(directory.GetState(entry) != DIR_UNSHARED);  //not possible to erase the entry

    if (wr->counter == 0) {
      epicLog(LOG_INFO, "INVALIDATE_FORWARD: failed case final-processing");
      void* laddr = ToLocal(wr->addr);
      epicAssert(directory.GetState(entry) == DIR_TO_DIRTY);
      //undo the directory/cache changes
      directory.UndoShared(entry);

      wr->unlock();
      directory.unlock(ToLocal(wr->addr));

      Client* pcli = FindClientWid(wr->pwid);
      parent->status = wr->status;  //put the error status
      parent->op = WRITE_REPLY;
      //comment below as counter is not initialized and not used in the write/invalidforward
      //parent->counter--;
      //epicAssert(parent->counter == 0);
      SubmitRequest(pcli, parent);
      parent->unlock();
      delete parent;
      parent = nullptr;
      wr->parent = nullptr;

      ProcessToServeRequest(wr);
      int ret = ErasePendingWork(wr->id);
      epicAssert(ret);
      delete wr;
      wr = nullptr;
      //parent has been already deleted when we receive failed response the first time
    } else {
      wr->unlock();
      directory.unlock(ToLocal(wr->addr));
      parent->unlock();
    }
    return;
  }

  //normal process below
  if (wr->counter == 0) {
    Client* lcli = FindClientWid(wr->pwid);

#ifdef SELECTIVE_CACHING
    if(wr->flag & NOT_CACHE) {
#ifdef GFUNC_SUPPORT
      if(wr->flag & GFUNC) {
        epicAssert(parent->gfunc);
        epicAssert(TOBLOCK(parent->addr) == TOBLOCK(GADD(parent->addr, parent->size-1)));
        void* laddr = ToLocal(parent->addr);
        wr->gfunc(laddr, wr->arg);
      } else {
#endif
        memcpy(ToLocal(parent->addr), parent->ptr, parent->size);
#ifdef GFUNC_SUPPORT
      }
#endif
    } else {
#endif

    if (WRITE == parent->op) {
      lcli->Write(parent->ptr, laddr, parent->size);
      epicLog(LOG_DEBUG, "write the data (size = %ld) to destination",
              parent->size);
    } else {  //WRITE_PERMISSION_ONLY
      epicAssert(WRITE_PERMISSION_ONLY == parent->op);
      //deadlock: one node (Node A) wants to update its cache from shared to dirty,
      //but at the same time, the home nodes invalidates all its shared copy
      //(due to a local write, or remote write after local/remote read)
      //currently, dir_state == DIR_UNSHARED
      //which means that the shared list doesn't contain the requesting node A.
      //solution: Node A acts as it is still a shared copy so that the invalidation can completes,
      //after which, home node processes the pending list
      //and change the processing from WRITE_PERMISSION_ONLY to WRITE
      if (DIR_UNSHARED == directory.GetState(entry)) {
        lcli->Write(parent->ptr, laddr, parent->size);
        epicLog(LOG_INFO, "deadlock detected");
        epicLog(LOG_DEBUG, "write the data to destination");
      }
    }

    // logging a ownership
    logOwner(lcli->GetWorkerId(), wr->addr);

    if (entry) {
      directory.ToDirty(entry, lcli->ToGlobal(parent->ptr));  //entry should be null
    } else {
      directory.ToDirty(laddr, lcli->ToGlobal(parent->ptr));  //entry should be null
    }

#ifdef SELECTIVE_CACHING
  }
#endif

    wr->unlock();
    directory.unlock(laddr);

    parent->op = WRITE_REPLY;
    parent->status = SUCCESS;
    parent->counter = 0;
    SubmitRequest(lcli, parent);
    parent->unlock();

    //clear the pending structures
    int ret = ErasePendingWork(wr->id);
    epicAssert(ret);
    ProcessToServeRequest(wr);
    delete wr;
    delete parent;
    wr = nullptr;
    parent = nullptr;
  } else {
    wr->unlock();
    directory.unlock(laddr);
    parent->unlock();
  }
}

void Worker::ProcessPendingRequest(Client* cli, WorkRequest* wr) {
  epicLog(LOG_DEBUG, "process pending request %d from worker %d", wr->op,
          cli->GetWorkerId());
  switch (wr->op) {
    case READ:
    case FETCH_AND_SHARED: {
      ProcessPendingRead(cli, wr);
      break;
    }
    case READ_FORWARD: {
      ProcessPendingReadForward(cli, wr);
      break;
    }
    case FETCH_AND_INVALIDATE:
    case INVALIDATE:
    case WRITE:
    case WRITE_PERMISSION_ONLY: {
      ProcessPendingWrite(cli, wr);
      break;
    }
    case WRITE_FORWARD:  //Case 4 in home node
    {
      ProcessPendingWriteForward(cli, wr);
      break;
    }
    case WRITE_BACK: {
      ProcessPendingEvictDirty(cli, wr);
      break;
    }
    case INVALIDATE_FORWARD: {
      ProcessPendingInvalidateForward(cli, wr);
      break;
    }
    /* add ergeda add */
    case JUST_WRITE: {
      ProcessPendingPrivateWrite(cli, wr);
      break;
    }
    case JUST_READ: {
      ProcessPendingPrivateRead (cli, wr);
      break;
    }
    case RM_READ: {
      ProcessPendingRmRead (cli, wr);
      break;
    }
    case RM_WRITE: {
      ProcessPendingRmWrite (cli, wr);
      break;
    }
    case RM_FORWARD: {
      ProcessPendingRmForward (cli, wr);
      break;
    }
    case RM_Done: {
      ProcessPendingRmDone (cli, wr);
      break;
    }
    case WE_READ: {
      ProcessPendingWeRead (cli, wr);
      break;
    }
    case WE_WRITE: {
      ProcessPendingWeWrite (cli, wr);
      break;
    }
    case WE_INV: {
      ProcessPendingWeInv (cli, wr);
      break;
    }
    /* add ergeda add */

    /* add xmx add */
    case FETCH_SUB_BLOCK_META: {
      processPendingFetchSubBlockMeta(cli, wr);
      break;
    }
    /* add xmx add */
    default:
      epicLog(LOG_WARNING, "unrecognized work request %d", wr->op);
      exit(-1);
      break;
  }
}

/*
 * callback function for locally initiated asynchronous request
 */
void Worker::ProcessRequest(Client* cli, unsigned int work_id) {
#ifdef NOCACHE
  epicLog(LOG_WARNING, "shouldn't come here");
  return;
#endif
  epicLog(LOG_DEBUG, "callback function work_id = %u, reply from %d", work_id,
          cli->GetWorkerId());
  WorkRequest* wr = GetPendingWork(work_id);
  /* add ergeda add */
  if (wr->op == TEST_RDMA) {
    //epicLog(LOG_WARNING, "write_with_imm recv\n");
    return;
  }
  /* add ergeda add */
  epicAssert(wr);
  epicAssert(wr->id == work_id);
  ProcessPendingRequest(cli, wr);
}

/* add ergeda add */

void Worker::ProcessPendingPrivateRead(Client * client, WorkRequest * wr) {
  //Just_for_test("processpendingprivateread", wr);
  WorkRequest* parent = wr->parent;
  parent->lock();

  cache.lock(wr->addr);
  GAddr pend = GADD(parent->addr, parent->size);
  GAddr end = GADD(wr->addr, wr->size);
  GAddr gs = wr->addr > parent->addr ? wr->addr : parent->addr;
  void* ls = (void*) ((ptr_t) parent->ptr + GMINUS(gs, parent->addr));
  void* cs = (void*) ((ptr_t) wr->ptr + GMINUS(gs, wr->addr));
  Size len = end > pend ? GMINUS(pend, gs) : GMINUS(end, gs);
  memcpy(ls, cs, len);

  
  CacheLine * cline = cache.GetCLine(wr->addr); // delete temporary cache
  cache.DeleteCache(cline);
  cache.unlock(wr->addr);

  if ( (--parent->counter) == 0) {  //read all the data
    parent->status = SUCCESS;
    parent->unlock();
    Notify(parent);
  } else {
    parent->unlock();
  }

  int ret = ErasePendingWork(wr->id);
  delete wr;
  wr = nullptr;
}

void Worker::ProcessPendingRmRead(Client * client, WorkRequest * wr) {
  //Just_for_test("ProcessPendingRmRead", wr);
  WorkRequest * parent = wr->parent;
  parent->lock();

  cache.lock(wr->addr);
  GAddr pend = GADD(parent->addr, parent->size);
  GAddr end = GADD(wr->addr, wr->size);
  GAddr gs = wr->addr > parent->addr ? wr->addr : parent->addr;
  void* ls = (void*) ((ptr_t) parent->ptr + GMINUS(gs, parent->addr));
  void* cs = (void*) ((ptr_t) wr->ptr + GMINUS(gs, wr->addr));
  Size len = end > pend ? GMINUS(pend, gs) : GMINUS(end, gs);
  memcpy(ls, cs, len);

  
  CacheLine * cline = cache.GetCLine(wr->addr);
  if (cline == nullptr) {
    epicLog(LOG_WARNING, "rm_read pending_time no cache?");
  }
  cline->state = CACHE_SHARED;
  cache.unlock(wr->addr);

  if ( (-- parent->counter) == 0) {
    parent->status = SUCCESS;
    parent->unlock();
    Notify(parent);
  }
  else {
    parent->unlock();
  }
  
  ProcessToServeRequest(wr);
  int ret = ErasePendingWork(wr->id);
  delete wr;
  wr = nullptr;
}

void Worker::ProcessPendingPrivateWrite(Client * client, WorkRequest * wr) {
  //Just_for_test("ProcessPendingPrivateWrite", wr);
  WorkRequest* parent = wr->parent;
  parent->lock();

  if ( (-- parent->counter) == 0) {
    parent->status = SUCCESS;
    parent->unlock();
    Notify(parent);
  }
  else {
    parent->unlock();
  }
  int ret = ErasePendingWork(wr->id);
  delete wr;
  wr = nullptr;
}

void Worker::ProcessPendingRmWrite(Client * client, WorkRequest * wr) {
  //Just_for_test("ProcessPendingRmWrite", wr);
  GAddr blk = TOBLOCK(wr->addr); //wr->addr 不一定等于 blk
  cache.lock(blk);
  CacheLine * cline = nullptr;
  cline = cache.GetCLine(blk);
  if (cline == nullptr) {
    epicLog(LOG_WARNING, "rmwrite pending no cache");
  }
  cline->state = CACHE_SHARED;
  cache.unlock(blk);

  WorkRequest* parent = wr->parent;
  parent->lock();

  if ( (-- parent->counter) == 0) {
    parent->status = SUCCESS;
    parent->unlock();
    Notify(parent);
  }
  else {
    parent->unlock();
  }

  int ret = ErasePendingWork(wr->id);
  epicAssert(ret);
  ProcessToServeRequest(wr);
  delete wr;
  wr = nullptr;
}

void Worker::ProcessPendingRmForward(Client * client, WorkRequest * wr) {
  //Just_for_test("ProcessPendingRmForward", wr);
  WorkRequest * parent = wr->parent;
  parent->lock();
  wr->lock();
  if ( (--wr->counter) == 0) {
    if (parent->op == RM_WRITE) { //request_node != home_node
      void * laddr = ToLocal(wr->addr);
      directory.lock(laddr);
      DirEntry * entry = directory.GetEntry(laddr);
      entry->state = DIR_SHARED; // 所有副本都invalid了，可以修改状态
      list<GAddr>& shared = directory.GetSList(entry);
      parent->unlock();
      for (auto it = shared.begin(); it != shared.end(); it++) {
        Client* cli = GetClient(*it);
        if (WID(*it) == parent->wid) {
          cli->WriteWithImm(cli->ToLocal(*it), laddr, BLOCK_SIZE, parent->id); //通知request_node已完成
          continue;
        }
        else {
          cli->WriteWithImm(cli->ToLocal(*it), laddr, BLOCK_SIZE, -(wr->id));
        }
      }
      directory.unlock(laddr);
      int ret = ErasePendingWork(wr->id);
      wr->unlock();
      parent->unlock();
      
      ProcessToServeRequest(wr);
      delete wr;
      wr = nullptr;
      return;
    }
    else { //request_node == home_node
      void * laddr = ToLocal(wr->addr);
      directory.lock(laddr);
      DirEntry * entry = directory.GetEntry(laddr);
      entry->state = DIR_SHARED; // 所有副本都invalid了，可以修改状态
      list<GAddr>& shared = directory.GetSList(entry);
      for (auto it = shared.begin(); it != shared.end(); it++) {
        Client* cli = GetClient(*it);
        cli->WriteWithImm(cli->ToLocal(*it), laddr, BLOCK_SIZE, -(wr->id));
      }
      directory.unlock(laddr);

      if ( (--parent->counter) == 0) {
        parent->status = SUCCESS;
        parent->unlock();
        Notify(parent);
      }else {
        parent->unlock();
      }

      int ret = ErasePendingWork(wr->id);
      wr->unlock();
      ProcessToServeRequest(wr);
      delete wr;
      wr = nullptr;
      return;
    }
  }

  parent->unlock();
  wr->unlock();
}

void Worker::ProcessPendingRmDone(Client * client, WorkRequest * wr) {
  //Just_for_test("ProcessPendingRmDone", wr);
  cache.lock(wr->addr);
  CacheLine * cline = nullptr;
  cline = cache.GetCLine(wr->addr);
  if (cline->state != CACHE_TO_INVALID) cline->state = CACHE_SHARED;
  cache.unlock(wr->addr);

  int ret = ErasePendingWork(-(wr->id));
  ProcessToServeRequest(wr);
  delete wr;
  wr = nullptr;
}

void Worker::ProcessPendingWeRead(Client * client, WorkRequest * wr) {
  WorkRequest* parent = wr->parent;
  parent->lock();

  cache.lock(wr->addr);
  GAddr pend = GADD(parent->addr, parent->size);
  GAddr end = GADD(wr->addr, wr->size);
  GAddr gs = wr->addr > parent->addr ? wr->addr : parent->addr;
  void* ls = (void*) ((ptr_t) parent->ptr + GMINUS(gs, parent->addr));
  void* cs = (void*) ((ptr_t) wr->ptr + GMINUS(gs, wr->addr));
  Size len = end > pend ? GMINUS(pend, gs) : GMINUS(end, gs);
  memcpy(ls, cs, len);

  CacheLine * cline = cache.GetCLine(wr->addr);
  cline->state = CACHE_SHARED;
  cache.unlock(wr->addr);

  if ( (--parent->counter) == 0) {  //read all the data
    parent->status = SUCCESS;
    parent->unlock();
    Notify(parent);
  } else {
    parent->unlock();
  }

  int ret = ErasePendingWork(wr->id);
  ProcessToServeRequest(wr);
  delete wr;
  wr = nullptr;
}

void Worker::ProcessPendingWeWrite(Client * client, WorkRequest * wr) {
  //Just_for_test("Pending We_write", wr);
  WorkRequest* parent = wr->parent;
  parent->lock();

  if ( (-- parent->counter) == 0) {
    parent->status = SUCCESS;
    parent->unlock();
    Notify(parent);
  }
  else {
    parent->unlock();
  }
  int ret = ErasePendingWork(wr->id);
  ProcessToServeRequest(wr);
  delete wr;
  wr = nullptr;
}

void Worker::ProcessPendingWeInv(Client * client, WorkRequest * wr) { //和RMFORWARD那个太类似了，感觉可以合并到一个函数
  //Just_for_test("pending We_Inv", wr);
  WorkRequest * parent = wr->parent;
  parent->lock();
  wr->lock();

  GAddr blk = TOBLOCK(wr->addr);
  void * laddr;
  if (IsLocal(blk)) laddr = ToLocal(blk);
  else laddr = (void*)blk;

  if ( (--wr->counter) == 0) {
    if (parent->op == WE_WRITE) { //request_node != owner_node
      
      directory.lock(laddr);
      DirEntry * entry = directory.GetEntry(laddr);
      entry->state = DIR_SHARED; // 所有副本都invalid了，可以修改状态
      entry->shared.clear(); //忘了我去
      parent->unlock();
      
      Client * cli = GetClient( ( (1ll * (parent->wid)) << 48) );
      cli->WriteWithImm(nullptr, nullptr, 0, parent->id);
      directory.unlock(laddr);
      int ret = ErasePendingWork(wr->id);
      wr->unlock();
      parent->unlock();
      
      ProcessToServeRequest(wr);
      delete wr;
      wr = nullptr;
      return;
    }
    else { //request_node == home_node
      directory.lock(laddr);
      DirEntry * entry = directory.GetEntry(laddr);
      entry->state = DIR_SHARED; // 所有副本都invalid了，可以修改状态
      entry->shared.clear();
      directory.unlock(laddr);

      if ( (--parent->counter) == 0) {
        parent->status = SUCCESS;
        parent->unlock();
        Notify(parent);
      }else {
        parent->unlock();
      }

      int ret = ErasePendingWork(wr->id);
      wr->unlock();
      ProcessToServeRequest(wr);
      delete wr;
      wr = nullptr;
      return;
    }
  }

  parent->unlock();
  wr->unlock();
}
/* add ergeda add */

/* add xmx add */
void Worker::processPendingFetchSubBlockMeta(Client *client, WorkRequest *wr) {
  epicAssert(BLOCK_ALIGNED(wr->addr));
  epicInfo("received sub-block meta from worker %d", client->GetWorkerId());
  cache.lock(wr->addr);
  CacheLine *cacheLine = cache.GetCLine(wr->addr);
  WorkRequest *parent = wr->parent;
  epicAssert(!(parent->flag & SUB_READ));
  parent->lock();
  parent->counter--;

  if (cacheLine) {
    // 更新这一块的分块信息
    cacheLine->updateSubCachesMeta(wr->ptr);
    cacheLine->state = CACHE_SUB_VALID;
    // debug
    {
      char buf[4096] = {0};
      int offset = 0;
      offset += sprintf(buf, "sub-cache size = %lu, splits = [ ", cacheLine->subCaches.size());
      for (const auto &subCache: cacheLine->subCaches) {
        offset += sprintf(buf + offset, "%lu ", subCache.end - wr->addr);
      }
      offset -= 4;
      offset += sprintf(buf + offset, "]");
      (void) offset;
      epicInfo("sub-block meta for %lx from worker %d: %s", wr->addr, client->GetWorkerId(), buf);
    }
  } else { // no cache line
    epicFatal("cache line for %lx is not existed", wr->addr);
  }

  parent->unlock();
  cache.unlock(wr->addr);
  sb.sb_free(wr->ptr);

  ProcessToServeRequest(wr);

  delete wr;
}

void Worker::processPendingReadSubBlock(Client *client, WorkRequest *wr) {
  /**
   * READ:               sub-cache   invalid -> shared   counter = 0
   * FETCH_AND_SHARED:   sub-block   dirty   -> shared   counter = 0
   */
  epicInfo("process pending read from worker %d. wr=%s", client->GetWorkerId(), ToString(wr).c_str());
  epicAssert(wr->parent);
  epicAssert((IsLocal(wr->addr) && wr->op == FETCH_AND_SHARED) || (!IsLocal(wr->addr) && wr->op == READ));

  WorkRequest *parent = wr->parent;
  WorkRequest *wrOrigin = parent->parent ? parent->parent : parent;
  CacheLine *cacheLine = nullptr;
  DirEntry *entry = nullptr;
  GAddr block = TOBLOCK(wr->addr);
  void *localBlock = wr->op == READ ? nullptr : ToLocal(block);

  parent->lock();

  if (wr->op == READ) {
    cache.lock(block);
    cacheLine = cache.GetCLine(wr->addr);
    epicAssert(cacheLine);

    auto subCache = cacheLine->findSubCache(wr);
    if (subCache == cacheLine->subCaches.end()) {
      epicFatal("no sub-cache for %lx(size=%ld). wr=%s", wr->addr, wr->size, ToString(wr).c_str());
    } else {
      GAddr parentEnd = GADD(parent->addr, parent->size);
      GAddr end = GADD(wr->addr, wr->size);
      GAddr cpyStart = max(wr->addr, parent->addr);
      void *dstStart = static_cast<char *>(parent->ptr) + (cpyStart - parent->addr);
      void *srcStart = static_cast<char *>(wr->ptr) + (cpyStart - wr->addr);
      size_t len = min(end, parentEnd) - cpyStart;
      memcpy(dstStart, srcStart, len);
      subCache->toShared();
      epicInfo("copied %lu bytes from sub-cache(%s) to wr=%s",
               len, ToString(*subCache).c_str(), ToString(parent).c_str());
    }

    cache.unlock(block);
  } else if (wr->op == FETCH_AND_SHARED) {
    directory.lock(localBlock);
    entry = directory.GetEntry(localBlock);
    epicAssert(entry);

    auto subEntry = entry->findSubEntry(wr);
    if (subEntry == entry->subEntries.end()) {
      epicFatal("no sub-block for %lx(size=%ld). wr=%s", wr->addr, wr->size, ToString(wr).c_str());
    } else {
      GAddr parentEnd = GADD(parent->addr, parent->size);
      GAddr end = GADD(wr->addr, wr->size);
      GAddr cpyStart = max(wr->addr, parent->addr);
      void *dstStart = static_cast<char *>(parent->ptr) + (cpyStart - parent->addr);
      void *srcStart = static_cast<char *>(wr->ptr) + (cpyStart - wr->addr);
      size_t len = min(end, parentEnd) - cpyStart;
      memcpy(dstStart, srcStart, len);
      // 这里子块从dirty -> shared，原来的所有者的缓存子块也变为shared，所以sharer不需要更改
      subEntry->toShared();
      epicInfo("copied %lu bytes from sub-block(%s) to wr=%s",
               len, ToString(*subEntry).c_str(), ToString(parent).c_str());
    }

    directory.unlock(localBlock);
  } else {
    epicFatal("shouldn't happen");
    epicAssert(false);
  }

  int ret = ErasePendingWork(wr->id);
  epicAssert(ret);

  parent->counter--;
  if (parent->flag & SUB_READ) {
    epicWarning("parent-counter=%d", parent->counter.load());
    parent->unlock();
    if (parent->counter == 0) {
      wrOrigin->counter--;
      delete parent;
      if (wrOrigin->counter == 0) {
        Notify(wrOrigin);
      }
    }
  } else {
    epicAssert(parent == wrOrigin);
    //epicWarning("origin-counter=%d", parent->counter.load()); /*add xmx add */
    parent->unlock();
    if (parent->counter == 0) {
      Notify(wrOrigin);
    }
  }

  ProcessToServeRequest(wr);
  delete wr;
}

void Worker::processPendingReadForward(Client *client, WorkRequest *wr) {
  /**
   * READ_FORWARD
   *    parent = READ (Worker::processRemoteReadSubBlock)
   */
  epicInfo("process pending read forward from worker %d. wr=%s", client->GetWorkerId(), ToString(wr).c_str());
  epicAssert(wr->op == READ_FORWARD);
  epicAssert(wr->parent);
  epicAssert(IsLocal(wr->addr));
  //parent request is from local node
  WorkRequest *parent = wr->parent;
  void *localAddr = ToLocal(wr->addr);
  GAddr block = TOBLOCK(wr->addr);
  void *localBlock = ToLocal(block);

  directory.lock(localBlock);
  DirEntry *entry = directory.GetEntry(localBlock);
  auto subEntry = entry->findSubEntry(wr);
  if (subEntry == entry->subEntries.end()) {
    epicFatal("read error! no sub-block for %lx(length=%ld). wr=%s", wr->addr, wr->size, ToString(wr).c_str());
  }
  Client *remoteClient = FindClientWid(wr->pwid);
  remoteClient->WriteWithImm(parent->ptr, localAddr, parent->size, parent->id);
  subEntry->toShared();
  subEntry->sharers.clear();
  subEntry->addSharer(wr->pwid);
  epicInfo("sent sub-block%s to worker %d", ToString(*subEntry).c_str(), wr->pwid);
  directory.unlock(localBlock);

  int ret = ErasePendingWork(wr->id);
  epicAssert(ret);

  ProcessToServeRequest(wr);
  delete wr;
  delete parent;
}

void Worker::processPendingWriteSubBlock(Client *client, WorkRequest *wr) {
  /**
   * WRITE:                 sub-cache   invalid -> dirty      counter = 0
   * WRITE_PERMISSION_ONLY: sub-cache   shared  -> dirty      counter = 0
   * FETCH_AND_INVALIDATE:  sub-block   dirty   -> unshared   counter = sharers.size (=1)
   * INVALIDATE:            sub-block   shared  -> unshared   counter = sharers.size
   */
  epicInfo("process pending write from worker %d. wr=%s", client->GetWorkerId(), ToString(wr).c_str());
  epicAssert(((wr->op == WRITE || wr->op == WRITE_PERMISSION_ONLY) && wr->flag & CACHED)
             || ((wr->op == FETCH_AND_INVALIDATE || wr->op == INVALIDATE) && IsLocal(wr->addr)));
  WorkRequest *parent = wr->parent;
  WorkRequest *wrOrigin = parent->parent ? parent->parent : parent;
  CacheLine *cacheLine = nullptr;
  DirEntry *entry = nullptr;
  GAddr block = TOBLOCK(wr->addr);
  void *localBlock = (wr->op == FETCH_AND_INVALIDATE || wr->op == INVALIDATE) ? ToLocal(block) : nullptr;

  parent->lock();
  // counter = sharers.size
  // 当前节点是本地节点(home node)，请求是本地节点发起的
  if (wr->op == FETCH_AND_INVALIDATE || wr->op == INVALIDATE) {
    directory.lock(localBlock);
    entry = directory.GetEntry(localBlock);
    epicAssert(entry);

    wr->counter--;
    auto subEntry = entry->findSubEntry(wr);
    if (subEntry == entry->subEntries.end()) {
      epicFatal("no sub-block for %lx(size=%ld). wr=%s", wr->addr, wr->size, ToString(wr).c_str());
    } else {
      subEntry->removeSharer(client->GetWorkerId());
    }
    // 所有副本已无效化，可以写入
    if (wr->counter == 0) {
      GAddr parentEnd = GADD(parent->addr, parent->size);
      GAddr end = GADD(wr->addr, wr->size);
      GAddr cpyStart = max(wr->addr, parent->addr);
      void *srcStart = static_cast<char *>(parent->ptr) + (cpyStart - parent->addr);
      void *dstStart = static_cast<char *>(wr->ptr) + (cpyStart - wr->addr);
      size_t len = min(end, parentEnd) - cpyStart;
      memcpy(dstStart, srcStart, len);
      subEntry->toUnshared();
      epicInfo("copied %lu bytes from wr=%s to sub-block(%s)",
               len, ToString(parent).c_str()), ToString(*subEntry).c_str();

      int ret = ErasePendingWork(wr->id);
      epicAssert(ret);
      parent->counter--;
    }

    directory.unlock(localBlock);
  } else if (wr->op == WRITE || wr->op == WRITE_PERMISSION_ONLY) {
    // counter = 0
    // 当前节点是远程节点
    cache.lock(block);
    cacheLine = cache.GetCLine(block);
    epicAssert(cacheLine);

    auto subCache = cacheLine->findSubCache(wr);
    if (subCache == cacheLine->subCaches.end()) {
      epicFatal("no sub-cache for %lx(size=%ld). wr=%s", wr->addr, wr->size, ToString(wr).c_str());
    }

    GAddr parentEnd = GADD(parent->addr, parent->size);
    GAddr end = GADD(wr->addr, wr->size);
    GAddr cpyStart = max(wr->addr, parent->addr);
    void *srcStart = static_cast<char *>(parent->ptr) + (cpyStart - parent->addr);
    void *dstStart = static_cast<char *>(wr->ptr) + (cpyStart - wr->addr);
    size_t len = min(end, parentEnd) - cpyStart;
    memcpy(dstStart, srcStart, len);
    subCache->toDirty();
    epicInfo("copied %lu bytes from wr=%s to sub-cache(%s)",
             len, ToString(parent).c_str(), ToString(*subCache).c_str());

    int ret = ErasePendingWork(wr->id);
    epicAssert(ret);
    parent->counter--;

    cache.unlock(block);
  } else {
    epicFatal("shouldn't happen");
    epicAssert(false);
  }


  if (parent->flag & SUB_WRITE) {
    epicWarning("parent-counter=%d", parent->counter.load());
    parent->unlock();
    if (parent->counter == 0) {
      wrOrigin->counter--;
      delete parent;
      if (wrOrigin->counter == 0) {
        Notify(wrOrigin);
      }
    }
  } else {
    epicAssert(parent == wrOrigin);
    //epicWarning("origin-counter=%d", parent->counter.load()); /* add xmx add */
    parent->unlock();
    if (parent->counter == 0) {
      Notify(wrOrigin);
    }
  }

  if (wr->counter == 0) {
    ProcessToServeRequest(wr);
    delete wr;
  }
}

void Worker::processPendingInvalidateForward(Client *client, WorkRequest *wr) {
  /**
   * INVALIDATE_FORWARD
   *    parent = WRITE | WRITE_PERMISSION_ONLY (Worker::processRemoteWriteSubBlock)
   */
  epicInfo("process pending invalidate forward from worker %d. wr=%s", client->GetWorkerId(), ToString(wr).c_str());
  WorkRequest *parent = wr->parent;
  epicAssert(parent);
  parent->lock();

  void *localAddr = ToLocal(wr->addr);
  GAddr block = TOBLOCK(wr->addr);
  void *localBlock = ToLocal(block);
  directory.lock(localBlock);

  epicAssert(IsLocal(wr->addr));
  wr->counter--;

  DirEntry *entry = directory.GetEntry(localBlock);

  auto subEntry = entry->findSubEntry(parent);
  if (subEntry == entry->subEntries.end()) {
    epicFatal("no sub-block for %lx(size=%ld). wr=%s", wr->addr, wr->size, ToString(wr).c_str());
  }
  subEntry->removeSharer(client->GetWorkerId());

  //normal process below
  if (wr->counter == 0) {
    Client *remoteClient = FindClientWid(wr->pwid);
    if (WRITE == parent->op) {
      remoteClient->WriteWithImm(parent->ptr, localAddr, parent->size, parent->id);
      subEntry->toDirty();
      subEntry->sharers.clear();
      subEntry->addSharer(wr->pwid);
      epicInfo("sent sub-block%s to worker %d", ToString(*subEntry).c_str(), wr->pwid);
    } else if (WRITE_PERMISSION_ONLY == parent->op) {
      //deadlock: one node (Node A) wants to update its cache from shared to dirty,
      //but at the same time, the home nodes invalidates all its shared copy
      //(due to a local write, or remote write after local/remote read)
      //currently, dir_state == DIR_UNSHARED
      //which means that the shared list doesn't contain the requesting node A.
      //solution: Node A acts as it is still a shared copy so that the invalidation can completes,
      //after which, home node processes the pending list
      //and change the processing from WRITE_PERMISSION_ONLY to WRITE
      subEntry->toDirty();
      subEntry->sharers.clear();
      subEntry->addSharer(wr->pwid);
      if (DIR_UNSHARED == subEntry->state) {
        remoteClient->WriteWithImm(parent->ptr, localAddr, parent->size, parent->id);
        epicWarning("deadlock detected");
        epicInfo("sent sub-block%s to worker %d", ToString(*subEntry).c_str(), wr->pwid);
      } else {
        remoteClient->WriteWithImm(nullptr, nullptr, 0, parent->id);
        epicInfo("transferred ownership of sub-block%s to worker %d", ToString(*subEntry).c_str(), wr->pwid);
      }
    }

    directory.unlock(localBlock);
    parent->unlock();

    //clear the pending structures
    int ret = ErasePendingWork(wr->id);
    epicAssert(ret);
    ProcessToServeRequest(wr);
    delete wr;
    delete parent;
  } else { // 还需要继续等待处理响应
    directory.unlock(localBlock);
    parent->unlock();
  }
}

void Worker::processPendingWriteForward(Client *client, WorkRequest *wr) {
  /**
   * WRITE_FORWARD
   *    parent = WRITE | WRITE_PERMISSION_ONLY (Worker::processRemoteWriteSubBlock)
   */
  epicInfo("process pending write forward from worker %d. wr=%s", client->GetWorkerId(), ToString(wr).c_str());
  epicAssert(wr->op == WRITE_FORWARD);
  epicAssert(wr->parent);
  epicAssert(IsLocal(wr->addr));

  void *localAddr = ToLocal(wr->addr);
  GAddr block = TOBLOCK(wr->addr);
  void *localBlock = ToLocal(block);

  WorkRequest *parent = wr->parent;
  DirEntry *entry = directory.GetEntry(localBlock);
  epicAssert(wr->pid == parent->id);

  Client *remoteClient = FindClientWid(wr->pwid);

  directory.lock(localBlock);
  auto subEntry = entry->findSubEntry(parent);
  if (subEntry == entry->subEntries.end()) {
    epicFatal("no sub-block for %lx(size=%ld). wr=%s", wr->addr, wr->size, ToString(wr).c_str());
  }
  subEntry->toDirty();
  subEntry->sharers.clear();
  subEntry->addSharer(wr->pwid);
  directory.unlock(localBlock);

  // TODO@xmx: 这里需要检查是否达到划分子块的阈值
  remoteClient->WriteWithImm(parent->ptr, localAddr, parent->size, parent->id);
  epicInfo("sent sub-block%s to worker %d", ToString(*subEntry).c_str(), wr->pwid);

  int ret = ErasePendingWork(wr->id);
  epicAssert(ret);
  ProcessToServeRequest(wr);
  delete wr;
  delete parent;
}

/* add xmx add */
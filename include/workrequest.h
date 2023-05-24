// Copyright (c) 2018 The GAM Authors 


#ifndef INCLUDE_WORKREQUEST_H_
#define INCLUDE_WORKREQUEST_H_

#include <type_traits>
#include <cstring>
#include <atomic>
#include <mutex>
#include <unistd.h>
#include <syscall.h>

#include "lockwrapper.h"
#include "structure.h"
#include "zmalloc.h"
#ifdef GFUNC_SUPPORT
#include "gfunc.h"
#endif

enum Work {
  MALLOC = 1,
  READ,
  FETCH_AND_SHARED,
  READ_FORWARD,
  WRITE,
  WRITE_PERMISSION_ONLY,
  FETCH_AND_INVALIDATE,
  INVALIDATE,
  INVALIDATE_FORWARD,
  WRITE_FORWARD,
  WRITE_PERMISSION_ONLY_FORWARD,
  ATOMIC,
  UPDATE_MEM_STATS,
  FETCH_MEM_STATS,
  BROADCAST_MEM_STATS,
  MFENCE,
  SFENCE,
  RLOCK,
  RLOCK_LEN,
  WLOCK,
  WLOCK_LEN,
  UNLOCK,
  UNLOCK_LEN,
  FREE,
  ACTIVE_INVALIDATE,
  WRITE_BACK,
  PENDING_INVALIDATE,
  PUT,
  GET,
  /* add ergeda add */
  JUST_WRITE, // access_exclusive write
  SET_CACHE, // create cache when malloc
  JUST_READ, //access_exclusive read
  READ_TYPE,
  RM_WRITE, //read_mostly write
  RM_READ, // read_mostly read
  RM_FORWARD, //read_mostly forward_write
  RM_Done, //read_mostly forward_write_done
  TEST_RDMA, //used to learn rdma workflow
  WE_READ, //write exclusive read
  WE_WRITE, //write exclusive write(!owner_node)
  WE_INV, //write_exclusive invalidate
  /* add ergeda add */

  /* add xmx add */
  // 本地没有缓存元信息或者版本号不对
  FETCH_SUB_BLOCK_META,
  CREATE_MUTEX,
  MUTEX_LOCK,
  MUTEX_TRY_LOCK,
  MUTEX_UNLOCK,
  CREATE_SEM,
  SEM_POST,
  SEM_WAIT,
  /* add xmx add */
#ifdef DHT
  GET_HTABLE,
#endif
  //set the value of REPLY so that we can test op & REPLY
  //to check whether it is a reply workrequest or not
  REPLY = 1 << 16,
#ifdef NOCACHE
  RLOCK_REPLY,
  WLOCK_REPLY,
#ifndef ASYNC_UNLOCK
  UNLOCK_REPLY,
#endif
#endif
  MALLOC_REPLY,
  FETCH_MEM_STATS_REPLY,
  READ_REPLY,
  WRITE_REPLY,
  LOCK_REPLY,
  FREE_REPLY,
#ifdef DHT
  GET_HTABLE_REPLY,
#endif
  GET_REPLY,
  /* add ergeda add */
  TYPE_REPLY,
  JUST_READ_REPLY,
  SET_CACHE_REPLY,
  /* add ergeda add */

  /* add xmx add */
  MUTEX_REPLY,
  SEM_REPLY,
  /* add xmx add */
};

enum Status {
  SUCCESS = 0,
  REMOTE_REQUEST,
  IN_TRANSITION,
  FENCE_PENDING,
  READ_SUCCESS,
  ERROR = 1 << 8,
  ALLOC_ERROR,
  READ_ERROR,
  WRITE_ERROR,
  UNRECOGNIZED_OP,
  LOCK_FAILED,
  /* add xmx add */
  // 需要获取新的子块信息，再进行远程数据读写
  META_VERSION_REQUIRED,
  /* add xmx add */
  NOT_EXIST
};

typedef std::underlying_type<Work>::type wtype;
typedef std::underlying_type<Status>::type stype;

typedef int Flag;

#define REMOTE 1
#define RANDOM (1 << 1)
#define CACHED (1 << 2)
#define ASYNC (1 << 3)
#define REPEATED (1 << 4)
#define REQUEST_DONE (1 << 5)
#define LOCKED (1 << 6)
#define TRY_LOCK (1 << 7)
#define TO_SERVE (1 << 8)
#define ALIGNED (1 << 9)
#define COPY (1 << 10)
#define LOCAL_REQUEST (1 << 11)
#define FENCE (1 << 12)
#define NOT_CACHE (1 << 13)
#define GFUNC (1 << 14)
/* add ergeda add */
#define Msi (1 << 15)
#define Read_only (1 << 16)
#define Read_mostly (1 << 17)
#define Access_exclusive (1 << 18)
#define Write_exclusive (1 << 19)
#define Write_shared (1 << 20)
#define Add_list (1 << 21) //表示是第一次访问，需要加入shared_list(read_mostly)
/* add ergeda add */

/* add xmx add */
# define SUB_READ (1 << 22)
# define SUB_WRITE (1 << 23)
/* add xmx add */

#define MASK_ID 1
#define MASK_OP 1 << 1
#define MASK_ADDR 1 << 2
#define MASK_FREE 1 << 3
#define MASK_SIZE 1 << 4
#define MASK_STATUS 1 << 5
#define MASK_FLAG 1 << 6
#define MASK_PTR 1 << 7
#define MASK_FD 1 << 8
#define MASK_WID 1 << 9
#define MASK_COUNTER 1 << 10

/*
 * TODO: try to shrink the size of WorkRequest structure
 * use union?
 */
struct WorkRequest {
  unsigned int id;  //identifier of the work request
  unsigned int pid;  //identifier of the parent work request (used for FORWARD request)
  int pwid;  //identifier of the parent worker
  enum Work op;

  union {
    uint64_t key;
    GAddr addr;
    Size free;
  };
  Size size;
  int status;

  Flag flag = 0;
  void* ptr;

  int fd;
#if	!defined(USE_PIPE_W_TO_H) || !defined(USE_PIPE_H_TO_W)
  volatile int* notify_buf;
#endif
#ifdef USE_PTHREAD_COND
  pthread_mutex_t* cond_lock;
  pthread_cond_t* cond;
#endif

  int wid;

  atomic<int> counter;  //maybe negative in Write Case 4

  WorkRequest* parent;
  WorkRequest* next; //
  WorkRequest* dup;

  /* add xmx add */
  int metaVersion = -1;
  /* add xmx add */

  LockWrapper lock_;

  bool is_cache_hit_ = true;

#ifdef GFUNC_SUPPORT
  GFunc* gfunc = nullptr;
  uint64_t arg = 0;
#endif
  WorkRequest()
      : fd(),
        id(),
        pid(),
        pwid(),
        op(),
        addr(),
        size(),
        status(),
        flag(),
        ptr(),
        wid(),
        counter(),
        parent(),
        next(),
        dup() {
#if	!defined(USE_PIPE_W_TO_H) || !defined(USE_PIPE_H_TO_W)
    notify_buf = nullptr;
#endif
  }
  ;
  WorkRequest(WorkRequest& wr);bool operator==(const WorkRequest& wr);
  int Ser(char* buf, int& len);
  int Deser(const char* buf, int& len);

  //we only allow one-times copy of the original workrequest
  //second call will return the previous duplicated copy
  //NOTE: if you want multiple copies,
  //use the WorkRequest(WorkRequest&) constructor
  WorkRequest* Copy() {
    if (flag & COPY) {
      epicLog(LOG_DEBUG, "already copied before");
      if (dup) {
        return dup;
      } else {
        return this;  //this is a copied version
      }
    } else {
      WorkRequest* nw = new WorkRequest(*this);
      if (ptr && size) {
        nw->ptr = zmalloc(size);
        memcpy(nw->ptr, ptr, size);
      }
      nw->flag |= COPY;
      //update the original version
      flag |= COPY;
      dup = nw;
      return nw;
    }
  }

  bool IsACopy() {
    return (flag & COPY) && (dup == nullptr);
  }

  void Reset() {
    lock();
    //memset(this, 0, sizeof(WorkRequest));
    id = 0;  //identifier of the work request

    pid = 0;  //identifier of the parent work request (used for FORWARD request)
    pwid = 0;  //identifier of the parent worker
    op = static_cast<Work>(0);

    key = 0;
    addr = 0;
    free = 0;
    size = 0;
    status = 0;

    flag = 0;
    ptr = 0;
    fd = 0;
#if	!defined(USE_PIPE_W_TO_H) || !defined(USE_PIPE_H_TO_W)
    notify_buf = 0;
#endif
    wid = 0;
    counter.store(0);
    parent = nullptr;
    next = nullptr;
    dup = nullptr;
#ifdef GFUNC_SUPPORT
    gfunc = nullptr;
    arg = 0;
#endif

    /* add xmx add */
    metaVersion = -1;
    /* add xmx add */
    is_cache_hit_ = true;
    unlock();
  }

  inline void lock() {
    lock_.lock();
  }

  inline void unlock() {
    lock_.unlock();
  }

  ~WorkRequest();
};

/* add xmx add */
inline const char *ToCString(Work work) {
  switch (work) {
    case MALLOC:
      return "MALLOC";
    case READ:
      return "READ";
    case FETCH_AND_SHARED:
      return "FETCH_AND_SHARED";
    case READ_FORWARD:
      return "READ_FORWARD";
    case WRITE:
      return "WRITE";
    case WRITE_PERMISSION_ONLY:
      return "WRITE_PERMISSION_ONLY";
    case FETCH_AND_INVALIDATE:
      return "FETCH_AND_INVALIDATE";
    case INVALIDATE:
      return "INVALIDATE";
    case INVALIDATE_FORWARD:
      return "INVALIDATE_FORWARD";
    case WRITE_FORWARD:
      return "WRITE_FORWARD";
    case WRITE_PERMISSION_ONLY_FORWARD:
      return "WRITE_PERMISSION_ONLY_FORWARD";
    case ATOMIC:
      return "ATOMIC";
    case UPDATE_MEM_STATS:
      return "UPDATE_MEM_STATS";
    case FETCH_MEM_STATS:
      return "FETCH_MEM_STATS";
    case BROADCAST_MEM_STATS:
      return "BROADCAST_MEM_STATS";
    case MFENCE:
      return "MFENCE";
    case SFENCE:
      return "SFENCE";
    case RLOCK:
      return "RLOCK";
    case RLOCK_LEN:
      return "RLOCK_LEN";
    case WLOCK:
      return "WLOCK";
    case WLOCK_LEN:
      return "WLOCK_LEN";
    case UNLOCK:
      return "UNLOCK";
    case UNLOCK_LEN:
      return "UNLOCK_LEN";
    case FREE:
      return "FREE";
    case ACTIVE_INVALIDATE:
      return "ACTIVE_INVALIDATE";
    case WRITE_BACK:
      return "WRITE_BACK";
    case PENDING_INVALIDATE:
      return "PENDING_INVALIDATE";
    case PUT:
      return "PUT";
    case GET:
      return "GET";
    case FETCH_SUB_BLOCK_META:
      return "FETCH_SUB_BLOCK_META";
    case CREATE_MUTEX:
      return "CREATE_MUTEX";
    case MUTEX_LOCK:
      return "MUTEX_LOCK";
    case MUTEX_TRY_LOCK:
      return "MUTEX_TRY_LOCK";
    case MUTEX_UNLOCK:
      return "MUTEX_UNLOCK";
    case CREATE_SEM:
      return "CREATE_MUTEX";
    case SEM_WAIT:
      return "SEM_WAIT";
    case SEM_POST:
      return "SEM_POST";
    case REPLY:
      return "REPLY";
    case MALLOC_REPLY:
      return "MALLOC_REPLY";
    case FETCH_MEM_STATS_REPLY:
      return "FETCH_MEM_STATS_REPLY";
    case READ_REPLY:
      return "READ_REPLY";
    case WRITE_REPLY:
      return "WRITE_REPLY";
    case LOCK_REPLY:
      return "LOCK_REPLY";
    case FREE_REPLY:
      return "FREE_REPLY";
    case GET_REPLY:
      return "GET_REPLY";
    case MUTEX_REPLY:
      return "MUTEX_REPLY";
    case SEM_REPLY:
      return "SEM_REPLY";
    default:
      return "Unknown work";
  }
}

inline const char *ToCString(int status) {
  switch (static_cast<Status>(status)) {
    case SUCCESS:
      return "SUCCESS";
    case REMOTE_REQUEST:
      return "REMOTE_REQUEST";
    case IN_TRANSITION:
      return "IN_TRANSITION";
    case FENCE_PENDING:
      return "FENCE_PENDING";
    case READ_SUCCESS:
      return "READ_SUCCESS";
    case ERROR:
      return "ERROR";
    case ALLOC_ERROR:
      return "ALLOC_ERROR";
    case READ_ERROR:
      return "READ_ERROR";
    case WRITE_ERROR:
      return "WRITE_ERROR";
    case UNRECOGNIZED_OP:
      return "UNRECOGNIZED_OP";
    case LOCK_FAILED:
      return "LOCK_FAILED";
    case META_VERSION_REQUIRED:
      return "META_VERSION_REQUIRED";
    case NOT_EXIST:
      return "NOT_EXIST";
    default:
      return "Unknown status";
  }
}

inline string ToString(const WorkRequest *wr) {
  char buf[BLOCK_SIZE * 2] = {0};
  sprintf(buf, "{%p, id=%u, addr=%lx, size=%ld, ptr=%p, parent=%p, op=%s, counter=%d, status=%s, meta=%d, fd=%d, wid=%d, flag=%d}",
          wr, wr->id, wr->addr, wr->size,
          wr->ptr, wr->parent,
          ToCString(wr->op), wr->counter.load(), ToCString(wr->status),
          wr->metaVersion, wr->fd, wr->wid, wr->flag);
  return string{buf};
}
/* add xmx add */

#endif /* INCLUDE_WORKREQUEST_H_ */

// Copyright (c) 2018 The GAM Authors 


#ifndef INCLUDE_GALLOCATOR_H_
#define INCLUDE_GALLOCATOR_H_

#include <cstring>
#include "lockwrapper.h"
#include "structure.h"
#include "worker.h"
#include "settings.h"
#include "worker_handle.h"
#include "master.h"
#ifdef GFUNC_SUPPORT
#include "gfunc.h"
#endif
/* add xmx add */
#include <utility>
/* add xmx add */

class GAlloc {
  WorkerHandle* wh;  //handle to communicate with local worker

  int Lock(Work op, const GAddr addr, const Size count, Flag flag = 0);
 public:
  GAlloc(Worker* worker);

  /**
   * malloc in the global address
   * @param size: the size of memory to be malloced
   * @param flag: some special flags
   * @param base: used to guarantee the affinity,
   * 				trying to put the newly-allocated addr to be in the same node of the base addr
   */
  //GAddr Malloc(const Size size, Flag flag = 0);
  //GAddr Malloc(const Size size, GAddr base, Flag flag = 0);
  /* add ergeda add */
  GAddr Malloc(const Size size, Flag flag = 0, int Owner=0); //为flag增加表示datastate的标志位，Malloc时候有可能需要指定owner。
  GAddr Malloc(const Size size, GAddr base, Flag flag = 0, int Owner=0);
  /* add ergeda add */
  GAddr AlignedMalloc(const Size size, GAddr base, Flag flag = 0);
  GAddr AlignedMalloc(const Size size, Flag flag = 0);
  GAddr Calloc(Size nmemb, Size size, Flag flag, GAddr base);
  GAddr Realloc(GAddr ptr, Size size, Flag flag);

  void Free(const GAddr addr);

  /*
   * read is blocking until buf is filled with requested data
   */
  int Read(const GAddr addr, void* buf, const Size count, Flag flag = 0);
  int Read(const GAddr addr, const Size offset, void* buf, const Size count,
           Flag flag = 0);

  /*
   * Generally, write is non-blocking as we're using release memory consistency model
   */
  int Write(const GAddr addr, void* buf, const Size count, Flag flag = 0);
#ifdef GFUNC_SUPPORT
  int Write(const GAddr addr, const Size offset, void* buf, const Size count,
            Flag flag = 0, GFunc* func = nullptr, uint64_t arg = 0);
  int Write(const GAddr addr, void* buf, const Size count, GFunc* func,
            uint64_t arg = 0, Flag flag = 0);
#else
  int Write(const GAddr addr, const Size offset, void* buf, const Size count, Flag flag = 0);
#endif

  void MFence();
  void SFence();

  void RLock(const GAddr addr, const Size count);
  void WLock(const GAddr addr, const Size count);

  void UnLock(const GAddr addr, const Size count);

  int Try_RLock(const GAddr addr, const Size count);
  int Try_WLock(const GAddr addr, const Size count);

  Size Put(uint64_t key, const void* value, Size count);
  Size Get(uint64_t key, void* value);

#ifdef DHT
  int HTable(void*);
#endif

  inline int GetID() {
    return wh->GetWorkerId();
  }
  int GetWorkersSize() {
    return wh->GetWorkersSize();
  }

  /*
   * check whether addr is a local addr or not
   */
  inline bool IsLocal(GAddr addr) {
    return WID(addr) == GetID();
  }

  /*
   * return the local pointer of the global address addr
   * if not local, return nullptr
   */
  inline void* GetLocal(GAddr addr) {
    if (!IsLocal(addr)) {
      return nullptr;
    } else {
      return wh->GetLocal(addr);
    }
  }

  void ReportCacheStatistics() {
    wh->ReportCacheStatistics();
  }
  /* add xmx add */
  uint64_t getTransferredBytes() const {
    return wh->getTransferredBytes();
  }
  /* add xmx add */

  void ResetCacheStatistics() {
    wh->ResetCacheStatistics();
  }

  ~GAlloc();
};

class GAllocFactory {
  static const Conf* conf;
  static Worker* worker;
  static Master* master;
  static LockWrapper lock;
 public:
#ifdef GFUNC_SUPPORT
#define MAX_GFUNCS 100
  static GFunc* gfuncs[MAX_GFUNCS];
#endif
  /*
   * this function should be call in every thread
   * in order to init some per-thread data
   */
  static GAlloc* CreateAllocator(const std::string& conf_file) {
    return CreateAllocator(ParseConf(conf_file));
  }

  static const Conf* InitConf() {
    lock.lock();
    conf = new Conf();
    const Conf* ret = conf;
    lock.unlock();
    return ret;
  }

  //need to call for every thread
  static GAlloc* CreateAllocator(const Conf* c = nullptr) {
    lock.lock();
    if (c) {
      if (!conf) {
        conf = c;
      } else {
        epicLog(LOG_INFO, "NOTICE: Conf already exist %lx", conf);
      }
    } else {
      if (!conf) {
        epicLog(LOG_FATAL, "Must provide conf for the first time");
      }
    }

    if (conf->is_master) {
      if (!master)
        master = MasterFactory::CreateServer(*conf);
    }
    if (!worker) {
      worker = WorkerFactory::CreateServer(*conf);
    }
    GAlloc* ret = new GAlloc(worker);
    lock.unlock();
    return ret;
  }

  //need to call for every thread
  static GAlloc* CreateAllocator(const Conf& c) {
    lock.lock();
    if (!conf) {
      Conf* lc = new Conf();
      *lc = c;
      conf = lc;
    } else {
      epicLog(LOG_INFO, "Conf already exist %lx", conf);
    }
    if (conf->is_master) {
      if (!master)
        master = MasterFactory::CreateServer(*conf);
    }
    if (!worker) {
      worker = WorkerFactory::CreateServer(*conf);
    }
    GAlloc* ret = new GAlloc(worker);
    lock.unlock();
    return ret;
  }

  static void SetConf(Conf* c) {
    lock.lock();
    conf = c;
    lock.unlock();
  }

  static void FreeResouce() {
    lock.lock();
    delete conf;
    delete worker;
    delete master;
    lock.unlock();
  }

  /*
   * TODO: fake parseconf function
   */
  static const Conf* ParseConf(const std::string& conf_file) {
    Conf* c = new Conf();
    return c;
  }

  static int LogLevel() {
    int ret = conf->loglevel;
    return ret;
  }

  static string* LogFile() {
    string* ret = conf->logfile;
    return ret;
  }
};

/* add xmx add */
template<typename T>
class GVar;


template<typename T>
class GLazyArray;


class GMemoryManager;


class GMutex {
  friend class GMemoryManager;

public:
  using Key = uint64_t;
  using MemoryManager = std::weak_ptr<GMemoryManager>;


  GMutex(Key key, const std::shared_ptr<GMemoryManager> &manager)
      : key(key), manager(manager) {}

  void lock();

  bool tryLock();

  void unlock();

  bool isValid() const {
    return key != 0;
  }

  Key getKey() const {
    return key;
  }

private:
  Key key;
  MemoryManager manager;
};


class GSemaphore {
  friend class GMemoryManager;

public:
  using Key = uint64_t;
  using Value = int;
  using MemoryManager = std::weak_ptr<GMemoryManager>;


  GSemaphore(Key key, Value value, const std::shared_ptr<GMemoryManager> &manager)
      : key(key), value(value), manager(manager) {}

  Value wait();

  Value post();

  bool isValid() const {
    return key != 0;
  }

  Key getKey() const {
    return key;
  }

  Value getValue() const {
    return value;
  }

private:
  Key key;
  Value value;
  MemoryManager manager;
};


class GMemoryManager : public std::enable_shared_from_this<GMemoryManager> {
public:
  explicit GMemoryManager(Worker *worker) : handle(new WorkerHandle{worker}) {}

  static std::shared_ptr<GMemoryManager> build(Worker *worker) {
    return make_shared<GMemoryManager>(worker);
  }

  std::shared_ptr<GMemoryManager> getPointer() {
    return shared_from_this();
  }

  Size getPendingWrites() const {
    return handle->getWorkerPendingWrites();
  }

  void printCacheStatistics() const {
    handle->ReportCacheStatistics();
  }

  uint64_t getTransferredBytes() const {
    return handle->getTransferredBytes();
  }

  void mfence() const {
    while (getPendingWrites());
  }

  template<typename T, typename ...Args>
  GVar<T> create(Args &&...args) {
    GAddr addr = malloc(sizeof(T), false);
    if (addr == Gnullptr) {
      epicFatal("create object failed");
      return GVar<T>{Gnullptr, getPointer()};
    }

    GVar<T> obj{addr, getPointer()};
    new(obj.value) T{std::forward<Args>(args)...};

    if (!write(obj)) {
      epicFatal("init object failed");
      return GVar<T>{Gnullptr, getPointer()};
    }

    return obj;
  }


  template<typename T>
  GVar<T> getVar(GAddr addr) {
    GVar<T> var{addr, getPointer()};
    if (!read(var)) {
      epicFatal("get object failed");
      return GVar<T>{Gnullptr, getPointer()};
    }

    return var;
  }

  template<typename T>
  GVar<T> getLazyVar(GAddr addr) {
    return {addr, getPointer()};
  }

  template<typename T>
  GLazyArray<T> getLazyArray(GAddr addr, size_t length) {
    return {addr, length, getPointer()};
  }


  GMutex createMutex(const char *name) {
    assert(name);
    WorkRequest wr;
    wr.op = CREATE_MUTEX;
    wr.ptr = const_cast<char *>(name);
    if (handle->SendRequest(&wr)) {
      epicFatal("created mutex %s failed", name);
      return {0, getPointer()};
    }
    return {wr.key, getPointer()};
  }

  GSemaphore createSemaphore(const char *name, GSemaphore::Value initVal) {
    assert(name);
    if (initVal < 0) {
      epicWarning("initial semaphore value is negative");
    }
    WorkRequest wr;
    wr.op = CREATE_SEM;
    wr.size = static_cast<decltype(wr.size)>(initVal);
    wr.ptr = const_cast<char *>(name);
    if (handle->SendRequest(&wr)) {
      epicFatal("created semaphore %s failed", name);
      return {0, 0, getPointer()};
    }
    return {wr.key, static_cast<GSemaphore::Value>(wr.size), getPointer()};
  }


  GAddr malloc(size_t size, bool aligned = false) {
    WorkRequest wr;
    wr.op = MALLOC;
    wr.size = size;
    if (aligned) {
      wr.flag |= ALIGNED;
    }
    if (handle->SendRequest(&wr)) {
      epicFatal("malloc failed");
      return Gnullptr;
    }
    return wr.addr;
  }

  template<typename T>
  GAddr allocate(size_t count, bool aligned = false) {
    WorkRequest wr;
    wr.op = MALLOC;
    wr.size = sizeof(T) * count;
//    epicFatal("allocate %lu bytes, aligned=%d", wr.size, (int)aligned);
    if (aligned) {
      wr.flag |= ALIGNED;
    }
    if (handle->SendRequest(&wr)) {
      epicFatal("allocate failed");
      return Gnullptr;
    }
    return wr.addr;
  }

  template<typename T>
  bool read(const GVar<T> &var) {
    WorkRequest wr;
    wr.op = READ;
    wr.addr = var.addr;
    wr.size = var.getSize();
    wr.ptr = const_cast<char *>(var.value);
    if (handle->SendRequest(&wr)) {
      epicFatal("read failed");
      return false;
    }
    return true;
  }

  template<typename T>
  bool write(const GVar<T> &var) {
    WorkRequest wr;
    wr.op = WRITE;
    wr.addr = var.addr;
    wr.size = var.getSize();
    wr.ptr = const_cast<char *>(var.value);
    wr.flag = ASYNC;
    if (handle->SendRequest(&wr)) {
      epicFatal("write failed");
      return false;
    }
    return true;
  }


  void lock(const GMutex &mutex) {
    WorkRequest wr;
    wr.op = MUTEX_LOCK;
    wr.key = mutex.key;
    if (handle->SendRequest(&wr)) {
      epicFatal("lock(%lu) failed. status=[%d]", wr.key, wr.status);
    }
  }

  bool tryLock(const GMutex &mutex) {
    WorkRequest wr;
    wr.op = MUTEX_TRY_LOCK;
    wr.key = mutex.key;
    if (handle->SendRequest(&wr)) {
      epicFatal("try lock(%lu) failed. status=[%d]", wr.key, wr.status);
    }
    return wr.status == SUCCESS;
  }

  void unlock(const GMutex &mutex) {
    WorkRequest wr;
    wr.op = MUTEX_UNLOCK;
    wr.key = mutex.key;
    if (handle->SendRequest(&wr)) {
      epicFatal("unlock(%lu) failed. status=[%d]", wr.key, wr.status);
    }
  }


  GSemaphore::Value wait(const GSemaphore &semaphore) {
    WorkRequest wr;
    wr.op = SEM_WAIT;
    wr.key = semaphore.key;
    if (handle->SendRequest(&wr)) {
      epicFatal("wait(%lu) failed. status=[%d]", wr.key, wr.status);
    }
    return static_cast<GSemaphore::Value>(wr.size);
  }

  GSemaphore::Value post(const GSemaphore &semaphore) {
    WorkRequest wr;
    wr.op = SEM_POST;
    wr.key = semaphore.key;
    if (handle->SendRequest(&wr)) {
      epicFatal("post(%lu) failed. status=[%d]", wr.key, wr.status);
    }
    return static_cast<GSemaphore::Value>(wr.size);
  }


private:
  std::unique_ptr<WorkerHandle> handle;
};


template<typename T>
class GVar {
  static_assert(std::is_trivial<T>::value, "object type should be trivial");
  friend class GMemoryManager;

public:
  using MemoryManager = std::weak_ptr<GMemoryManager>;

  GVar(GAddr addr, const std::shared_ptr<GMemoryManager> &manager)
      : addr(addr), manager(manager) {}

  GVar &operator=(const T &val) {
    *reinterpret_cast<T *>(const_cast<char *>(value)) = val;
    return *this;
  }

  T getValue() const {
    return *reinterpret_cast<T *>(const_cast<char *>(value));
  }

  T &getReference() {
    return *reinterpret_cast<T *>(const_cast<char *>(value));
  }

  const T &getConstReference() const {
    return *reinterpret_cast<T *>(const_cast<char *>(value));
  }

  GAddr getGlobalAddress() const {
    return addr;
  }

  T *operator->() const {
    return reinterpret_cast<T *>(const_cast<char *>(value));
  }

  explicit operator bool() const {
    return addr != Gnullptr && bool(getValue());
  }

  bool store(bool sync = false) {
    if (manager.expired()) {
      epicFatal("store object failed: invalid GMemoryManager");
      return false;
    }

    if (!manager.lock()->write(*this)) {
      epicFatal("store object failed");
      return false;
    }

    if (sync) {
      while (manager.lock()->getPendingWrites());
    }
    return true;
  }

  T load() {
    if (manager.expired()) {
      epicFatal("load object failed: invalid GMemoryManager");
      return {};
    }

    if (!manager.lock()->read(*this)) {
      epicFatal("load object failed");
      return {};
    }
    return *reinterpret_cast<T *>(const_cast<char *>(value));
  }

  constexpr size_t getSize() const {
    return sizeof(T);
  }

private:
  GAddr addr;
  MemoryManager manager;
  char value[sizeof(T)]{0};
};


template<typename T>
class GLazyArray {
  static_assert(std::is_trivial<T>::value, "object type should be trivial");
  friend class GMemoryManager;

public:
  using MemoryManager = std::weak_ptr<GMemoryManager>;

  GLazyArray(GAddr addr, size_t length, const std::shared_ptr<GMemoryManager> &manager)
      : addr(addr), length(length), manager(manager) {}

  constexpr size_t getElementSize() const {
    return sizeof(T);
  }

  GVar<T> operator[](size_t index) const {
    if (manager.expired()) {
      epicFatal("read element of lazy array failed: invalid GMemoryManager");
      return {addr + index * getElementSize(), manager.lock()};
    }
    GVar<T> var{addr + index * getElementSize(), manager.lock()};
    if (!manager.lock()->read(var)) {
      epicFatal("read element of lazy array failed");
    }
    return var;
  }

  GVar<T> at(size_t index) const noexcept(false) {
    if (manager.expired()) {
      epicFatal("read element of lazy array failed: invalid GMemoryManager");
      return {addr + index * getElementSize(), manager.lock()};
    }

    if (index >= length) {
      throw std::out_of_range{"GLazyArray: index out of range"};
    }

    GVar<T> var{addr + index * getElementSize(), manager.lock()};
    if (!manager.lock()->read(var)) {
      epicFatal("read element of lazy array failed");
    }
    return var;
  }

  GVar<T> getLazyVar(size_t index) const noexcept(false) {
    if (manager.expired()) {
      epicFatal("read element of lazy array failed: invalid GMemoryManager");
      return {addr + index * getElementSize(), manager.lock()};
    }

    if (index >= length) {
      throw std::out_of_range{"GLazyArray: index out of range"};
    }

    return {addr + index * getElementSize(), manager.lock()};
  }

  explicit operator bool() const {
    return addr != Gnullptr;
  }

  GAddr getGlobalAddress() const {
    return addr;
  }

  size_t getLength() const {
    return length;
  }

private:
  GAddr addr;
  size_t length;
  MemoryManager manager;
};
/* add xmx add */

#endif /* INCLUDE_GALLOCATOR_H_ */

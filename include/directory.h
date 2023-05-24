// Copyright (c) 2018 The GAM Authors 


#ifndef INCLUDE_DIRECTORY_H_
#define INCLUDE_DIRECTORY_H_

#include <list>
#include <unordered_map>
#include "settings.h"
#include "hashtable.h"
#include "map.h"
/* add xmx add */
#include "workrequest.h"
/* add xmx add */

enum DirState {
  DIR_UNSHARED,
  DIR_DIRTY,
  DIR_SHARED,
  DIR_TO_DIRTY,
  DIR_TO_SHARED,
  DIR_TO_UNSHARED
};

/* add xmx add */
inline const char *ToCString(DirState dirState) {
  switch (dirState) {
    case DIR_UNSHARED:
      return "DIR_UNSHARED";
    case DIR_DIRTY:
      return "DIR_DIRTY";
    case DIR_SHARED:
      return "DIR_SHARED";
    case DIR_TO_DIRTY:
      return "DIR_TO_DIRTY";
    case DIR_TO_SHARED:
      return "DIR_TO_SHARED";
    case DIR_TO_UNSHARED:
      return "DIR_TO_UNSHARED";
    default:
      return "Unknown directory state";
  }
}

struct SubDirEntry {
  SubDirEntry(GAddr start, GAddr end): start(start), end(end) {}
  // 子块的本地开始的地址
  ptr_t start;
  // 子块的本地结束的地址（不包括）
  ptr_t end;
  // 子块的状态
  DirState state = DIR_UNSHARED;
  // 子块的共享者（worker id）
  list<int> sharers;

  auto size() const -> decltype(end - start) {
    return end - start;
  }

  void removeSharer(int wid) {
    sharers.erase(remove_if(sharers.begin(), sharers.end(), [wid] (const int workerId) { return workerId == wid;}), sharers.end());
  }

  void addSharer(int wid) {
    sharers.push_back(wid);
  }

  void toToShared() {
    state = DIR_TO_SHARED;
  }

  void toShared() {
    state = DIR_SHARED;
  }

  void toToDirty() {
    state = DIR_TO_DIRTY;
  }

  void toDirty() {
    state = DIR_DIRTY;
  }

  void toToUnshared() {
    state = DIR_TO_UNSHARED;
  }

  void toUnshared() {
    state = DIR_UNSHARED;
  }
};

inline string ToString(const SubDirEntry &subDirEntry) {
  char buf[BLOCK_SIZE] = {0};
  int offset = 0;
  offset += sprintf(buf, "{addr=%lx:%lx, size=%lu, state=%s, sharers=[",
                    subDirEntry.start, subDirEntry.end,
                    subDirEntry.end - subDirEntry.start,
                    ToCString(subDirEntry.state));
  for (auto sharer: subDirEntry.sharers) {
    offset += sprintf(buf + offset, "%d, ", sharer);
  }
  offset -= 2;
  offset += sprintf(buf + offset, "]}");
  return string{buf};
}
/* add xmx add */

struct DirEntry {
  DirState state = DIR_UNSHARED;
  list<GAddr> shared;
  ptr_t addr;
  /* add ergeda add */
  DataState Dstate = DataState::MSI;
  GAddr owner;
  /* add ergeda add */
  //if lock == 0, no one is holding the lock. otherwise, there are #lock ones holding the lock
  //but if lock = EXCLUSIVE_LOCK_TAG, it is a exclusive lock
  //int lock = 0;
  unordered_map<ptr_t, int> locks;

  /* add xmx add */
  int metaVersion = 0;
  // 内存块的子块
  list<SubDirEntry> subEntries;

  decltype(subEntries.end()) findSubEntry(WorkRequest *wr) {
    return std::find_if(subEntries.begin(), subEntries.end(), [wr] (const SubDirEntry &subDirEntry) {
      return subDirEntry.start == wr->addr && subDirEntry.end == GADD(wr->addr, wr->size);
    });
  }
  /* add xmx add */
};

class Directory {
  /*
   * the directory structure
   * key: BLOCK-aligned address
   * value: directory entry
   */
  //unordered_map<ptr_t, DirEntry> dir;
#ifdef USE_SIMPLE_MAP
  Map<ptr_t, DirEntry*> dir { "directory" };
#else
  HashTable<ptr_t, DirEntry*> dir {"directory"};
#endif

  DirState GetState(ptr_t ptr);bool InTransitionState(ptr_t ptr);
  int RLock(ptr_t ptr);
  int RLock(DirEntry* entry, ptr_t ptr);bool IsRLocked(ptr_t ptr);bool IsRLocked(
      DirEntry* entry, ptr_t ptr);
  int WLock(ptr_t ptr);
  int WLock(DirEntry* entry, ptr_t ptr);bool IsWLocked(ptr_t ptr);bool IsWLocked(
      DirEntry* entry, ptr_t ptr);

  DirEntry* GetEntry(ptr_t ptr) {
    if (dir.count(TOBLOCK(ptr))) {
      return dir.at(TOBLOCK(ptr));
    } else {
      return nullptr;
    }
  }

  void UnLock(ptr_t ptr);
  void UnLock(DirEntry*& entry, ptr_t ptr);bool IsBlockWLocked(ptr_t block);bool IsBlockLocked(
      ptr_t block);
  void Clear(ptr_t ptr, GAddr addr);
  inline list<GAddr>& GetSList(ptr_t ptr) {
    return dir.at(TOBLOCK(ptr))->shared;
  }
  inline void lock(ptr_t ptr) {
    epicAssert(BLOCK_ALIGNED(ptr));
    LOCK_MICRO(dir, ptr);
  }
  inline void unlock(ptr_t ptr) {
    epicAssert(BLOCK_ALIGNED(ptr));
    UNLOCK_MICRO(dir, ptr);
  }
 public:
  /*
   * @ptr is the local virtual address
   */

  /* add xmx add */
  DirEntry *newEntry(GAddr block, void *localBlock) {
    auto entry = new DirEntry();
    entry->addr = block;
    dir[reinterpret_cast<ptr_t>(localBlock)] = entry;

    int nBlocks = BLOCK_SIZE / 64;
    list<SubDirEntry> subBlocks;
    for (int i = 0; i < nBlocks; ++i) {
      subBlocks.emplace_back(BLOCK_SIZE / nBlocks * i + block, BLOCK_SIZE / nBlocks * (i + 1) + block);
    }
    entry->subEntries.swap(subBlocks);
    return entry;
  }
  /* add xmx add */
  inline DirState GetState(void* ptr) {
    return GetState((ptr_t) ptr);
  }
  inline DirState GetState(DirEntry* entry) {
    if (!entry) {
      return DIR_UNSHARED;
    } else {
      return entry->state;
    }
  }
  inline bool InTransitionState(void* ptr) {
    return InTransitionState((ptr_t) ptr);
  }
  inline bool InTransitionState(DirState s) {
    return s == DIR_TO_DIRTY || s == DIR_TO_SHARED || s == DIR_TO_UNSHARED;
  }
  inline bool InTransitionState(DirEntry* entry) {
    if (!entry) {
      return false;
    } else {
      return InTransitionState(entry->state);
    }
  }
  DirEntry* GetEntry(void* ptr) {
    return GetEntry((ptr_t) ptr);
  }

  /* add ergeda add */

/*  MetaEntry* GetMeta(GAddr addr) {
    if (Meta.count(addr)) {
      return Meta.at(addr);
    }
    else return nullptr;
  }
*/
  DataState GetDataState (DirEntry* Entry) {
    if (Entry == nullptr) {
      //epicLog(LOG_WARNING, "MetaEntry == nullptr\n");
      return DataState::MSI;
    }
    return Entry->Dstate;
  }

  DataState GetDataState (GAddr addr) {
    return GetDataState (GetEntry(addr));
  }

  GAddr GetOwner (DirEntry * Entry) {
    return Entry->owner;
  }

  GAddr GetOwner (GAddr addr) {
    return GetOwner (GetEntry(addr));
  }/*
  void CreateMetaEntry(GAddr addr, DataState Dstate=DataState::MSI, GAddr Owner=1) {
    MetaEntry * Entry = GetMeta(addr);
    if (Entry == nullptr) {
      Entry = new MetaEntry();
      Entry->Dstate = Dstate;
      Entry->Last_def = 0;
      Entry->owner = Owner;
      Meta[addr] = Entry;
    }
    else {
      epicLog(LOG_WARNING, "meta already exist\n");
    }
  }
*/
  void SetDataState(DirEntry * Entry, DataState Dstate) {
    Entry->Dstate = Dstate;
  }

  void SetMetaOwner(DirEntry * Entry, GAddr Owner) {
    Entry->owner = Owner;
  }

  void CreateEntry (GAddr plus_block, void* ptr, DataState Cur_state=DataState::MSI, GAddr Owner=1) {
    ptr_t block = TOBLOCK(ptr);
    DirEntry* entry = GetEntry(ptr);
    if (entry == nullptr) {
      entry = new DirEntry();
      entry->state = DIR_UNSHARED;
      entry->Dstate = Cur_state;
      // maybe problem
      //entry->addr = plus_block;
      entry->addr = block; 
      // maybe problem
      entry->owner = Owner;
      dir[block] = entry;

      /* add xmx add */
      if (Cur_state == WRITE_SHARED) {
        int nBlocks = BLOCK_SIZE / 64;
        list<SubDirEntry> subBlocks;
        for (int i = 0; i < nBlocks; ++i) {
          subBlocks.emplace_back(BLOCK_SIZE / nBlocks * i + plus_block, BLOCK_SIZE / nBlocks * (i + 1) + plus_block);
        }
        entry->subEntries.swap(subBlocks);
      }
      /* add xmx add */
    }
  }

  void SetShared(DirEntry * Entry) {
    Entry->state = DIR_SHARED;
  }
  
  void SetDirty(DirEntry * Entry) {
    Entry->state = DIR_DIRTY;
  }

  void SetUnshared(DirEntry * Entry) {
    Entry->state = DIR_UNSHARED;
  }
  /* add ergeda add */

  DirEntry* ToShared(void* ptr, GAddr addr);
  void ToShared(DirEntry* entry, GAddr addr);
  DirEntry* ToDirty(void* ptr, GAddr addr);
  void ToDirty(DirEntry* entry, GAddr addr);
  void ToUnShared(void* ptr);
  void ToUnShared(DirEntry*& entry);
  void ToToShared(void* ptr, GAddr addr = Gnullptr);
  void ToToShared(DirEntry* entry, GAddr addr = Gnullptr);
  DirEntry* ToToDirty(void* ptr, GAddr addr = Gnullptr);
  inline void ToToDirty(DirEntry* entry, GAddr = Gnullptr) {
    epicAssert(entry);
    entry->state = DIR_TO_DIRTY;
  }
  void ToToUnShared(void* ptr);
  void ToToUnShared(DirEntry* entry);
  void UndoDirty(void* ptr);
  void UndoDirty(DirEntry* entry);
  void UndoShared(void* ptr);
  void UndoShared(DirEntry* entry);
  void Remove(void* ptr, int wid);
  void Remove(DirEntry*& entry, int wid);

  inline int RLock(void* ptr) {
    return RLock((ptr_t) ptr);
  }
  inline int RLock(DirEntry* entry, void* ptr) {
    return RLock(entry, (ptr_t) ptr);
  }
  int RLock(DirEntry* entry) = delete;

  inline bool IsRLocked(void* ptr) {
    return IsRLocked((ptr_t) ptr);
  }
  inline bool IsRLocked(DirEntry* entry, void* ptr) {
    return IsRLocked(entry, (ptr_t) ptr);
  }
  bool IsRLocked(DirEntry* entry) = delete;

  inline int WLock(void* ptr) {
    return WLock((ptr_t) ptr);
  }
  inline int WLock(DirEntry* entry, void* ptr) {
    return WLock(entry, (ptr_t) ptr);
  }
  int WLock(DirEntry* entry) = delete;

  inline bool IsWLocked(void* ptr) {
    return IsWLocked((ptr_t) ptr);
  }
  inline bool IsWLocked(DirEntry* entry, void* ptr) {
    return IsWLocked(entry, (ptr_t) ptr);
  }
  bool IsWLocked(DirEntry* entry) = delete;

  inline void UnLock(void* ptr) {
    UnLock((ptr_t) ptr);
  }
  inline void UnLock(DirEntry*& entry, void* ptr) {
    UnLock(entry, (ptr_t) ptr);
  }
  void UnLock(DirEntry* entry) = delete;

  inline bool IsBlockWLocked(void* block) {
    return IsBlockWLocked((ptr_t) block);
  }
  bool IsBlockWLocked(DirEntry* entry);
  inline bool IsBlockLocked(void* block) {
    return IsBlockLocked((ptr_t) block);
  }
  bool IsBlockLocked(DirEntry* entry);

  inline void Clear(void* ptr, GAddr addr) {
    Clear((ptr_t) ptr, addr);
  }
  void Clear(DirEntry*& entry, GAddr addr);

  inline list<GAddr>& GetSList(void* ptr) {
    return GetSList((ptr_t) ptr);
  }
  list<GAddr>& GetSList(DirEntry* entry) {
    return entry->shared;
  }

  //below are used for multithread programming
  inline void lock(void* ptr) {
    lock((ptr_t) ptr);
  }
  inline void unlock(void* ptr) {
    unlock((ptr_t) ptr);
  }
  void unlock(DirEntry* entry) = delete;
  void lock(DirEntry* entry) = delete;
};

#endif /* INCLUDE_DIRECTORY_H_ */

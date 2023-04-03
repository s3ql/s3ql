/*
 * Based heavily on https://www.sqlite.org/src/file?name=src/test_vfstrace.c&ci=trunk
 */
#include <sqlite3ext.h>
#include <unordered_set>
#include <unordered_map>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <iostream>
#include <string>

 // Set of dirty blocks in a file
typedef std::unordered_set<size_t> block_map_t;

// Maps filenames to sets of dirty blocks
static std::unordered_map<std::string, block_map_t> file_block_map;

// Block size to use
static size_t blocksize;

static std::string vfsname = "s3ql_tracewrites";


SQLITE_EXTENSION_INIT1;

/*
** An instance of this structure is attached to the each trace VFS to
** provide auxiliary information.
*/
typedef struct vfstrace_info vfstrace_info;
struct vfstrace_info {
    sqlite3_vfs* pRootVfs;  /* The underlying real VFS */
    sqlite3_vfs* pTraceVfs; /* Pointer back to the trace VFS */
};

/*
** The sqlite3_file object for the trace VFS
*/
typedef struct vfstrace_file vfstrace_file;
struct vfstrace_file {
    sqlite3_file base;    /* Base class.  Must be first */
    sqlite3_file* pReal;  /* The real underlying file */
    block_map_t* block_map;
};

/*
** Method declarations for vfstrace_file.
*/
static int vfstraceClose(sqlite3_file*);
static int vfstraceRead(sqlite3_file*, void*, int iAmt, sqlite3_int64 iOfst);
static int vfstraceWrite(sqlite3_file*, const void*, int iAmt, sqlite3_int64);
static int vfstraceTruncate(sqlite3_file*, sqlite3_int64 size);
static int vfstraceSync(sqlite3_file*, int flags);
static int vfstraceFileSize(sqlite3_file*, sqlite3_int64* pSize);
static int vfstraceLock(sqlite3_file*, int);
static int vfstraceUnlock(sqlite3_file*, int);
static int vfstraceCheckReservedLock(sqlite3_file*, int*);
static int vfstraceFileControl(sqlite3_file*, int op, void* pArg);
static int vfstraceSectorSize(sqlite3_file*);
static int vfstraceDeviceCharacteristics(sqlite3_file*);
static int vfstraceShmLock(sqlite3_file*, int, int, int);
static int vfstraceShmMap(sqlite3_file*, int, int, int, void volatile**);
static void vfstraceShmBarrier(sqlite3_file*);
static int vfstraceShmUnmap(sqlite3_file*, int);

/*
** Method declarations for vfstrace_vfs.
*/
static int vfstraceOpen(sqlite3_vfs*, const char*, sqlite3_file*, int, int*);
static int vfstraceDelete(sqlite3_vfs*, const char* zName, int syncDir);
static int vfstraceAccess(sqlite3_vfs*, const char* zName, int flags, int*);
static int vfstraceFullPathname(sqlite3_vfs*, const char* zName, int, char*);
static void* vfstraceDlOpen(sqlite3_vfs*, const char* zFilename);
static void vfstraceDlError(sqlite3_vfs*, int nByte, char* zErrMsg);
static void (*vfstraceDlSym(sqlite3_vfs*, void*, const char* zSymbol))(void);
static void vfstraceDlClose(sqlite3_vfs*, void*);
static int vfstraceRandomness(sqlite3_vfs*, int nByte, char* zOut);
static int vfstraceSleep(sqlite3_vfs*, int microseconds);
static int vfstraceCurrentTime(sqlite3_vfs*, double*);
static int vfstraceGetLastError(sqlite3_vfs*, int, char*);
static int vfstraceCurrentTimeInt64(sqlite3_vfs*, sqlite3_int64*);
static int vfstraceSetSystemCall(sqlite3_vfs*, const char*, sqlite3_syscall_ptr);
static sqlite3_syscall_ptr vfstraceGetSystemCall(sqlite3_vfs*, const char*);
static const char* vfstraceNextSystemCall(sqlite3_vfs*, const char* zName);

/*
** Close an vfstrace-file.
*/
static int vfstraceClose(sqlite3_file* pFile) {
    vfstrace_file* p = (vfstrace_file*)pFile;
    int rc;
    rc = p->pReal->pMethods->xClose(p->pReal);
    if (rc == SQLITE_OK) {
        sqlite3_free((void*)p->base.pMethods);
        p->base.pMethods = 0;
    }
    return rc;
}

/*
** Read data from an vfstrace-file.
*/
static int vfstraceRead(
    sqlite3_file* pFile,
    void* zBuf,
    int iAmt,
    sqlite_int64 iOfst) {
    vfstrace_file* p = (vfstrace_file*)pFile;
    int rc;
    rc = p->pReal->pMethods->xRead(p->pReal, zBuf, iAmt, iOfst);
    return rc;
}

/*
** Write data to an vfstrace-file.
*/
static int vfstraceWrite(
    sqlite3_file* pFile,
    const void* zBuf,
    int iAmt,
    sqlite_int64 iOfst) {
    vfstrace_file* p = (vfstrace_file*)pFile;
    int rc;
    rc = p->pReal->pMethods->xWrite(p->pReal, zBuf, iAmt, iOfst);
    if (rc != SQLITE_OK) {
        return rc;
    }
    if (!p->block_map) {
        return rc;
    }
    for (size_t blockno = iOfst / blocksize;
        blockno <= (iOfst + iAmt) / blocksize;
        blockno++) {
        p->block_map->insert(blockno);
    }
    return rc;
}

/*
** Truncate an vfstrace-file.
*/
static int vfstraceTruncate(sqlite3_file* pFile, sqlite_int64 size) {
    vfstrace_file* p = (vfstrace_file*)pFile;
    int rc;
    rc = p->pReal->pMethods->xTruncate(p->pReal, size);
    if (rc != SQLITE_OK) {
        return rc;
    }
    if (!p->block_map) {
        return rc;
    }
    auto it = p->block_map->begin();
    auto blockno = size / blocksize;
    int dropped = 0;
    while (it != p->block_map->end()) {
        if (*it <= blockno) {
            it++;
            continue;
        }
        p->block_map->erase(it++);
        dropped++;
    }
    sqlite3_log(SQLITE_NOTICE, "Dropped %d dirty blocks after truncation", dropped);
    return rc;
}

/*
** Sync an vfstrace-file.
*/
static int vfstraceSync(sqlite3_file* pFile, int flags) {
    vfstrace_file* p = (vfstrace_file*)pFile;
    int rc;
    rc = p->pReal->pMethods->xSync(p->pReal, flags);
    return rc;
}

/*
** Return the current file-size of an vfstrace-file.
*/
static int vfstraceFileSize(sqlite3_file* pFile, sqlite_int64* pSize) {
    vfstrace_file* p = (vfstrace_file*)pFile;
    int rc;
    rc = p->pReal->pMethods->xFileSize(p->pReal, pSize);
    return rc;
}

/*
** Lock an vfstrace-file.
*/
static int vfstraceLock(sqlite3_file* pFile, int eLock) {
    vfstrace_file* p = (vfstrace_file*)pFile;
    int rc;
    rc = p->pReal->pMethods->xLock(p->pReal, eLock);
    return rc;
}

/*
** Unlock an vfstrace-file.
*/
static int vfstraceUnlock(sqlite3_file* pFile, int eLock) {
    vfstrace_file* p = (vfstrace_file*)pFile;
    int rc;
    rc = p->pReal->pMethods->xUnlock(p->pReal, eLock);
    return rc;
}

/*
** Check if another file-handle holds a RESERVED lock on an vfstrace-file.
*/
static int vfstraceCheckReservedLock(sqlite3_file* pFile, int* pResOut) {
    vfstrace_file* p = (vfstrace_file*)pFile;
    int rc;
    rc = p->pReal->pMethods->xCheckReservedLock(p->pReal, pResOut);
    return rc;
}

/*
** File control method. For custom operations on an vfstrace-file.
*/
static int vfstraceFileControl(sqlite3_file* pFile, int op, void* pArg) {
    vfstrace_file* p = (vfstrace_file*)pFile;
    int rc;
    rc = p->pReal->pMethods->xFileControl(p->pReal, op, pArg);
    return rc;
}

/*
** Return the sector-size in bytes for an vfstrace-file.
*/
static int vfstraceSectorSize(sqlite3_file* pFile) {
    vfstrace_file* p = (vfstrace_file*)pFile;
    int rc;
    rc = p->pReal->pMethods->xSectorSize(p->pReal);
    return rc;
}

/*
** Return the device characteristic flags supported by an vfstrace-file.
*/
static int vfstraceDeviceCharacteristics(sqlite3_file* pFile) {
    vfstrace_file* p = (vfstrace_file*)pFile;
    int rc;
    rc = p->pReal->pMethods->xDeviceCharacteristics(p->pReal);
    return rc;
}

/*
** Shared-memory operations.
*/
static int vfstraceShmLock(sqlite3_file* pFile, int ofst, int n, int flags) {
    vfstrace_file* p = (vfstrace_file*)pFile;
    if (p->block_map) {
        sqlite3_log(SQLITE_IOERR_SHMLOCK, "Can't use shm when write tracking is enabled");
        return SQLITE_IOERR_SHMLOCK;
    }
    return p->pReal->pMethods->xShmLock(p->pReal, ofst, n, flags);
}
static int vfstraceShmMap(
    sqlite3_file* pFile,
    int iRegion,
    int szRegion,
    int isWrite,
    void volatile** pp) {
    vfstrace_file* p = (vfstrace_file*)pFile;
    if (p->block_map) {
        sqlite3_log(SQLITE_IOERR_SHMMAP, "Can't use shm when write tracking is enabled");
        return SQLITE_IOERR_SHMMAP;
    }
    return p->pReal->pMethods->xShmMap(p->pReal, iRegion, szRegion, isWrite, pp);
}
static void vfstraceShmBarrier(sqlite3_file* pFile) {
    vfstrace_file* p = (vfstrace_file*)pFile;
    if (p->block_map) {
        sqlite3_log(SQLITE_IOERR, "Can't use shm when write tracking is enabled");
    }
    p->pReal->pMethods->xShmBarrier(p->pReal);
}
static int vfstraceShmUnmap(sqlite3_file* pFile, int delFlag) {
    vfstrace_file* p = (vfstrace_file*)pFile;
    if (p->block_map) {
        sqlite3_log(SQLITE_IOERR, "Can't use shm when write tracking is enabled");
        return SQLITE_IOERR;
    }
    return p->pReal->pMethods->xShmUnmap(p->pReal, delFlag);
}

/*
** Open an vfstrace file handle.
*/
static int vfstraceOpen(
    sqlite3_vfs* pVfs,
    const char* zName,
    sqlite3_file* pFile,
    int flags,
    int* pOutFlags) {
    int rc;
    vfstrace_file* p = (vfstrace_file*)pFile;
    vfstrace_info* pInfo = (vfstrace_info*)pVfs->pAppData;
    sqlite3_vfs* pRoot = pInfo->pRootVfs;
    p->pReal = (sqlite3_file*)&p[1];
    rc = pRoot->xOpen(pRoot, zName, p->pReal, flags, pOutFlags);

    auto it = file_block_map.find(zName ? zName : "<temp>");
    if (it != file_block_map.end()) {
        p->block_map = &(it->second);
        sqlite3_log(SQLITE_NOTICE, "%s opened with write-tracking enabled", zName);
    } else {
        sqlite3_log(SQLITE_NOTICE, "%s opened with write-tracking disabled", zName);
        p->block_map = nullptr;
    }

    if (p->pReal->pMethods) {
        sqlite3_io_methods* pNew = static_cast<sqlite3_io_methods*>(sqlite3_malloc(sizeof(*pNew)));
        const sqlite3_io_methods* pSub = p->pReal->pMethods;
        memset(pNew, 0, sizeof(*pNew));
        pNew->iVersion = pSub->iVersion;
        pNew->xClose = vfstraceClose;
        pNew->xRead = vfstraceRead;
        pNew->xWrite = vfstraceWrite;
        pNew->xTruncate = vfstraceTruncate;
        pNew->xSync = vfstraceSync;
        pNew->xFileSize = vfstraceFileSize;
        pNew->xLock = vfstraceLock;
        pNew->xUnlock = vfstraceUnlock;
        pNew->xCheckReservedLock = vfstraceCheckReservedLock;
        pNew->xFileControl = vfstraceFileControl;
        pNew->xSectorSize = vfstraceSectorSize;
        pNew->xDeviceCharacteristics = vfstraceDeviceCharacteristics;
        if (pNew->iVersion >= 2) {
            /* We can't set these to nullptr for the tracked databases, because SQLite
             * then assumes that they will not be available for e.g. the associated
             * journals either */
            pNew->xShmMap = pSub->xShmMap ? vfstraceShmMap : 0;
            pNew->xShmLock = pSub->xShmLock ? vfstraceShmLock : 0;
            pNew->xShmBarrier = pSub->xShmBarrier ? vfstraceShmBarrier : 0;
            pNew->xShmUnmap = pSub->xShmUnmap ? vfstraceShmUnmap : 0;
        }

        pFile->pMethods = pNew;
    }
    return rc;
}

/*
** Delete the file located at zPath. If the dirSync argument is true,
** ensure the file-system modifications are synced to disk before
** returning.
*/
static int vfstraceDelete(sqlite3_vfs* pVfs, const char* zPath, int dirSync) {
    vfstrace_info* pInfo = (vfstrace_info*)pVfs->pAppData;
    sqlite3_vfs* pRoot = pInfo->pRootVfs;
    int rc;
    rc = pRoot->xDelete(pRoot, zPath, dirSync);
    return rc;
}

/*
** Test for access permissions. Return true if the requested permission
** is available, or false otherwise.
*/
static int vfstraceAccess(
    sqlite3_vfs* pVfs,
    const char* zPath,
    int flags,
    int* pResOut) {
    vfstrace_info* pInfo = (vfstrace_info*)pVfs->pAppData;
    sqlite3_vfs* pRoot = pInfo->pRootVfs;
    int rc;
    rc = pRoot->xAccess(pRoot, zPath, flags, pResOut);
    return rc;
}

/*
** Populate buffer zOut with the full canonical pathname corresponding
** to the pathname in zPath. zOut is guaranteed to point to a buffer
** of at least (DEVSYM_MAX_PATHNAME+1) bytes.
*/
static int vfstraceFullPathname(
    sqlite3_vfs* pVfs,
    const char* zPath,
    int nOut,
    char* zOut) {
    vfstrace_info* pInfo = (vfstrace_info*)pVfs->pAppData;
    sqlite3_vfs* pRoot = pInfo->pRootVfs;
    int rc;
    rc = pRoot->xFullPathname(pRoot, zPath, nOut, zOut);
    return rc;
}

/*
** Open the dynamic library located at zPath and return a handle.
*/
static void* vfstraceDlOpen(sqlite3_vfs* pVfs, const char* zPath) {
    vfstrace_info* pInfo = (vfstrace_info*)pVfs->pAppData;
    sqlite3_vfs* pRoot = pInfo->pRootVfs;
    return pRoot->xDlOpen(pRoot, zPath);
}

/*
** Populate the buffer zErrMsg (size nByte bytes) with a human readable
** utf-8 string describing the most recent error encountered associated
** with dynamic libraries.
*/
static void vfstraceDlError(sqlite3_vfs* pVfs, int nByte, char* zErrMsg) {
    vfstrace_info* pInfo = (vfstrace_info*)pVfs->pAppData;
    sqlite3_vfs* pRoot = pInfo->pRootVfs;
    pRoot->xDlError(pRoot, nByte, zErrMsg);
}

/*
** Return a pointer to the symbol zSymbol in the dynamic library pHandle.
*/
static void (*vfstraceDlSym(sqlite3_vfs* pVfs, void* p, const char* zSym))(void) {
    vfstrace_info* pInfo = (vfstrace_info*)pVfs->pAppData;
    sqlite3_vfs* pRoot = pInfo->pRootVfs;
    return pRoot->xDlSym(pRoot, p, zSym);
}

/*
** Close the dynamic library handle pHandle.
*/
static void vfstraceDlClose(sqlite3_vfs* pVfs, void* pHandle) {
    vfstrace_info* pInfo = (vfstrace_info*)pVfs->pAppData;
    sqlite3_vfs* pRoot = pInfo->pRootVfs;
    pRoot->xDlClose(pRoot, pHandle);
}

/*
** Populate the buffer pointed to by zBufOut with nByte bytes of
** random data.
*/
static int vfstraceRandomness(sqlite3_vfs* pVfs, int nByte, char* zBufOut) {
    vfstrace_info* pInfo = (vfstrace_info*)pVfs->pAppData;
    sqlite3_vfs* pRoot = pInfo->pRootVfs;
    return pRoot->xRandomness(pRoot, nByte, zBufOut);
}

/*
** Sleep for nMicro microseconds. Return the number of microseconds
** actually slept.
*/
static int vfstraceSleep(sqlite3_vfs* pVfs, int nMicro) {
    vfstrace_info* pInfo = (vfstrace_info*)pVfs->pAppData;
    sqlite3_vfs* pRoot = pInfo->pRootVfs;
    return pRoot->xSleep(pRoot, nMicro);
}

/*
** Return the current time as a Julian Day number in *pTimeOut.
*/
static int vfstraceCurrentTime(sqlite3_vfs* pVfs, double* pTimeOut) {
    vfstrace_info* pInfo = (vfstrace_info*)pVfs->pAppData;
    sqlite3_vfs* pRoot = pInfo->pRootVfs;
    return pRoot->xCurrentTime(pRoot, pTimeOut);
}
static int vfstraceCurrentTimeInt64(sqlite3_vfs* pVfs, sqlite3_int64* pTimeOut) {
    vfstrace_info* pInfo = (vfstrace_info*)pVfs->pAppData;
    sqlite3_vfs* pRoot = pInfo->pRootVfs;
    return pRoot->xCurrentTimeInt64(pRoot, pTimeOut);
}

/*
** Return th3 emost recent error code and message
*/
static int vfstraceGetLastError(sqlite3_vfs* pVfs, int iErr, char* zErr) {
    vfstrace_info* pInfo = (vfstrace_info*)pVfs->pAppData;
    sqlite3_vfs* pRoot = pInfo->pRootVfs;
    return pRoot->xGetLastError(pRoot, iErr, zErr);
}

/*
** Override system calls.
*/
static int vfstraceSetSystemCall(
    sqlite3_vfs* pVfs,
    const char* zName,
    sqlite3_syscall_ptr pFunc) {
    vfstrace_info* pInfo = (vfstrace_info*)pVfs->pAppData;
    sqlite3_vfs* pRoot = pInfo->pRootVfs;
    return pRoot->xSetSystemCall(pRoot, zName, pFunc);
}
static sqlite3_syscall_ptr vfstraceGetSystemCall(
    sqlite3_vfs* pVfs,
    const char* zName) {
    vfstrace_info* pInfo = (vfstrace_info*)pVfs->pAppData;
    sqlite3_vfs* pRoot = pInfo->pRootVfs;
    return pRoot->xGetSystemCall(pRoot, zName);
}
static const char* vfstraceNextSystemCall(sqlite3_vfs* pVfs, const char* zName) {
    vfstrace_info* pInfo = (vfstrace_info*)pVfs->pAppData;
    sqlite3_vfs* pRoot = pInfo->pRootVfs;
    return pRoot->xNextSystemCall(pRoot, zName);
}

extern "C"
{
    int sqlite3_extension_init(
        sqlite3* db,
        char** pzErrMsg,
        const sqlite3_api_routines* pApi) {
        (void)db;
        (void)pzErrMsg;

        SQLITE_EXTENSION_INIT2(pApi);

        sqlite3_vfs* pNew;
        sqlite3_vfs* pRoot;
        vfstrace_info* pInfo;
        int nByte;

        pRoot = sqlite3_vfs_find(nullptr);
        if (pRoot == 0)
            return SQLITE_NOTFOUND;
        nByte = sizeof(*pNew) + sizeof(*pInfo);
        pNew = static_cast<sqlite3_vfs*>(sqlite3_malloc(nByte));
        if (pNew == 0)
            return SQLITE_NOMEM;
        memset(pNew, 0, nByte);
        pInfo = (vfstrace_info*)&pNew[1];
        pNew->iVersion = pRoot->iVersion;
        pNew->szOsFile = pRoot->szOsFile + static_cast<int>(sizeof(vfstrace_file));
        pNew->mxPathname = pRoot->mxPathname;
        pNew->zName = vfsname.c_str();
        pNew->pAppData = pInfo;
        pNew->xOpen = vfstraceOpen;
        pNew->xDelete = vfstraceDelete;
        pNew->xAccess = vfstraceAccess;
        pNew->xFullPathname = vfstraceFullPathname;
        pNew->xDlOpen = pRoot->xDlOpen == 0 ? 0 : vfstraceDlOpen;
        pNew->xDlError = pRoot->xDlError == 0 ? 0 : vfstraceDlError;
        pNew->xDlSym = pRoot->xDlSym == 0 ? 0 : vfstraceDlSym;
        pNew->xDlClose = pRoot->xDlClose == 0 ? 0 : vfstraceDlClose;
        pNew->xRandomness = vfstraceRandomness;
        pNew->xSleep = vfstraceSleep;
        pNew->xCurrentTime = vfstraceCurrentTime;
        pNew->xGetLastError = pRoot->xGetLastError == 0 ? 0 : vfstraceGetLastError;
        if (pNew->iVersion >= 2) {
            pNew->xCurrentTimeInt64 = pRoot->xCurrentTimeInt64 == 0 ? 0 : vfstraceCurrentTimeInt64;
            if (pNew->iVersion >= 3) {
                pNew->xSetSystemCall = pRoot->xSetSystemCall == 0 ? 0 : vfstraceSetSystemCall;
                pNew->xGetSystemCall = pRoot->xGetSystemCall == 0 ? 0 : vfstraceGetSystemCall;
                pNew->xNextSystemCall = pRoot->xNextSystemCall == 0 ? 0 : vfstraceNextSystemCall;
            }
        }
        pInfo->pRootVfs = pRoot;
        pInfo->pTraceVfs = pNew;
        int ret = sqlite3_vfs_register(pNew, 0);

        if (ret == SQLITE_OK) {
            return SQLITE_OK_LOAD_PERMANENTLY;
        } else {
            return ret;
        }
    }
}

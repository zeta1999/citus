/*-------------------------------------------------------------------------
 *
 * locally_reserved_shared_connection_stats.h
 *   Management of connection reservations in shard memory pool
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef LOCALLY_RESERVED_SHARED_CONNECTION_STATS_H_
#define LOCALLY_RESERVED_SHARED_CONNECTION_STATS_H_

#include "distributed/connection_management.h"


#define DEALLOCATE_ALL -1

extern void InitializeLocallyReservedSharedConnectionStats(void);
extern bool HasAlreadyReservedConnection(const char *hostName, int nodePort,
										 Oid databaseOid);
extern void DecrementReservedConnection(const char *hostName, int nodePort,
										Oid databaseOid);
extern void DeallocateReservedConnections(int count);
extern void ReserveSharedConnectionCounterForAllPrimaryNodesIfNeeded(int count);


#endif /* LOCALLY_RESERVED_SHARED_CONNECTION_STATS_H_ */

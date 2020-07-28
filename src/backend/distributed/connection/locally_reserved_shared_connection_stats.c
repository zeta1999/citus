/*-------------------------------------------------------------------------
 *
 * locally_reserved_shared_connection_stats.c
 *
 *   Keeps track of the number of reserved connections to remote nodes
 *   for this backend. The primary goal is to complement the logic
 *   implemented shared_connection_stats.c which aims to prevent excessive
 *   number of connections (typically > max_connections) to any worker node.
 *   With this locally reserved connection stats, we enforce the same
 *   constraints considering these locally reserved shared connections.
 *
 *   To be more precise, shared connection stats are incremented only with two
 *   operations: (a) Establishing a connection to a remote node
 *               (b) Reserving connections, the logic that this
 *                   file implements.
 *
 *   Finally, as the name already implies, once a node has a reserved a  shared
 *   connection, it is guaranteed to have the right to establish a connection
 *   to the given remote node when needed.
 *
 *   For COPY command, we use this fact to reserve connections to the remote nodes
 *   in the same order as the adaptive executor in order to prevent any resource
 *   starvations. We need to do this because COPY establishes connections when it
 *   recieves a tuple that targets a remote node. This is a valuable optimization
 *   to prevent unnecessary connection establishments, which are pretty expensive.
 *   Instead, COPY command can reserve connections upfront, and utilize them when
 *   they are actually needed.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "miscadmin.h"

#include "access/hash.h"
#include "commands/dbcommands.h"
#include "distributed/listutils.h"
#include "distributed/locally_reserved_shared_connection_stats.h"
#include "distributed/metadata_cache.h"
#include "distributed/shared_connection_stats.h"
#include "distributed/tuplestore.h"
#include "distributed/worker_manager.h"
#include "utils/hashutils.h"
#include "utils/builtins.h"


#define RESERVED_CONNECTION_STATS_COLUMNS 4


/* session specific hash map*/
static HTAB *SessionLocalReservedConnectionCounters = NULL;


/*
 * Hash key for connection reservations
 */
typedef struct ReservedConnectionCounterHashKey
{
	char hostname[MAX_NODE_LENGTH];
	int32 port;
	Oid databaseOid;
} ReservedConnectionCounterHashKey;

/* hash entry for per worker stats */
typedef struct ReservedConnectionCounterHashEntry
{
	ReservedConnectionCounterHashKey key;

	int reservedConnectionCount;
} ReservedConnectionCounterHashEntry;


static void StoreAllReservedConnectionStats(Tuplestorestate *tupleStore,
											TupleDesc tupleDescriptor);
static ReservedConnectionCounterHashEntry * AllocateOrGetReservedConectionEntry(
	char *hostName, int nodePort, Oid databaseOid);
static void ReserveSharedConnectionCounterForNodeListIfNeeded(List *nodeList);
static uint32 LocalConnectionReserveHashHash(const void *key, Size keysize);
static int LocalConnectionReserveHashCompare(const void *a, const void *b, Size keysize);


PG_FUNCTION_INFO_V1(citus_reserved_connection_stats);

/*
 * citus_reserved_connection_stats returns all the avaliable information about all
 * the reserved connections. This function is used mostly for testing.
 */
Datum
citus_reserved_connection_stats(PG_FUNCTION_ARGS)
{
	TupleDesc tupleDescriptor = NULL;

	CheckCitusVersion(ERROR);
	Tuplestorestate *tupleStore = SetupTuplestore(fcinfo, &tupleDescriptor);

	StoreAllReservedConnectionStats(tupleStore, tupleDescriptor);

	/* clean up and return the tuplestore */
	tuplestore_donestoring(tupleStore);

	PG_RETURN_VOID();
}


/*
 * StoreAllRemoteConnectionStats gets connections established from the current node
 * and inserts them into the given tuplestore.
 *
 * We don't need to enforce any access privileges as the number of backends
 * on any node is already visible on pg_stat_activity to all users.
 */
static void
StoreAllReservedConnectionStats(Tuplestorestate *tupleStore, TupleDesc tupleDescriptor)
{
	Datum values[RESERVED_CONNECTION_STATS_COLUMNS];
	bool isNulls[RESERVED_CONNECTION_STATS_COLUMNS];

	HASH_SEQ_STATUS status;
	ReservedConnectionCounterHashEntry *connectionEntry = NULL;

	hash_seq_init(&status, SessionLocalReservedConnectionCounters);
	while ((connectionEntry = (ReservedConnectionCounterHashEntry *) hash_seq_search(
				&status)) != 0)
	{
		/* get ready for the next tuple */
		memset(values, 0, sizeof(values));
		memset(isNulls, false, sizeof(isNulls));

		char *databaseName = get_database_name(connectionEntry->key.databaseOid);
		if (databaseName == NULL)
		{
			/* database might have been dropped */
			continue;
		}

		values[0] = PointerGetDatum(cstring_to_text(connectionEntry->key.hostname));
		values[1] = Int32GetDatum(connectionEntry->key.port);
		values[2] = PointerGetDatum(cstring_to_text(databaseName));
		values[3] = Int32GetDatum(connectionEntry->reservedConnectionCount);

		tuplestore_putvalues(tupleStore, tupleDescriptor, values, isNulls);
	}
}


/*
 * InitializeLocallyReservedSharedConnectionStats initializes the hashmap in
 * ConnectionContext.
 */
void
InitializeLocallyReservedSharedConnectionStats(void)
{
	HASHCTL reservedCounterInfo;

	memset(&reservedCounterInfo, 0, sizeof(reservedCounterInfo));
	reservedCounterInfo.keysize = sizeof(ReservedConnectionCounterHashKey);
	reservedCounterInfo.entrysize = sizeof(ReservedConnectionCounterHashEntry);

	/*
	 * ConnectionContext is the session local memory context that is used for
	 * tracking remote connections.
	 */
	reservedCounterInfo.hcxt = ConnectionContext;

	reservedCounterInfo.hash = LocalConnectionReserveHashHash;
	reservedCounterInfo.match = LocalConnectionReserveHashCompare;

	uint32 hashFlags = (HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT | HASH_COMPARE);

	SessionLocalReservedConnectionCounters =
		hash_create("citus session level reserved counters (host,port,database)",
					64, &reservedCounterInfo, hashFlags);
}


/*
 *  HasAlreadyReservedConnection returns true if we have already reserved at least
 *  one shared connection in this session.
 */
bool
HasAlreadyReservedConnection(const char *hostName, int nodePort, Oid databaseOid)
{
	ReservedConnectionCounterHashKey key;

	strlcpy(key.hostname, hostName, MAX_NODE_LENGTH);
	key.port = nodePort;
	key.databaseOid = databaseOid;

	bool found = false;
	ReservedConnectionCounterHashEntry *entry =
		(ReservedConnectionCounterHashEntry *)
		hash_search(SessionLocalReservedConnectionCounters, &key, HASH_FIND, &found);

	if (!found || !entry)
	{
		return false;
	}

	return entry->reservedConnectionCount > 0;
}


/*
 * DeallocateReservedConnections is intended to be called when the operation
 * has reserved connection(s) but not used them.
 */
void
DeallocateReservedConnections(int count)
{
	HASH_SEQ_STATUS status;
	ReservedConnectionCounterHashEntry *entry;

	hash_seq_init(&status, SessionLocalReservedConnectionCounters);
	while ((entry = (ReservedConnectionCounterHashEntry *) hash_seq_search(&status)) != 0)
	{
		int deallocateCountForEntry = count;
		if (deallocateCountForEntry == DEALLOCATE_ALL)
		{
			/*
			 * The caller asked to deallocate all the reservation for all
			 * the nodes. Each node might have different number of reserved
			 * connections.
			 */
			deallocateCountForEntry = entry->reservedConnectionCount;
		}
		else
		{
			/* at most deallocate reservedConnectionCount */
			deallocateCountForEntry = Min(count, entry->reservedConnectionCount);
		}

		Assert(deallocateCountForEntry >= 0);
		DecrementSharedConnectionCounter(deallocateCountForEntry, entry->key.hostname,
										 entry->key.port);

		entry->reservedConnectionCount -= deallocateCountForEntry;
		Assert(entry->reservedConnectionCount >= 0);

		ereport(DEBUG5, (errmsg("Decrement shared reserved connection to node "
								"%s:%d to %d", entry->key.hostname, entry->key.port,
								entry->reservedConnectionCount)));

		bool found = false;

		/*
		 * We cleaned up all the entries because we may not need reserved connections
		 * in the next iteration.
		 */
		hash_search(SessionLocalReservedConnectionCounters, entry, HASH_REMOVE, &found);
		Assert(found);
	}
}


/*
 * DecrementReservedConnection decrements the reserved counter for the given host.
 */
void
DecrementReservedConnection(const char *hostName, int nodePort, Oid databaseOid)
{
	ReservedConnectionCounterHashKey key;

	strlcpy(key.hostname, hostName, MAX_NODE_LENGTH);
	key.port = nodePort;
	key.databaseOid = databaseOid;

	bool found = false;
	ReservedConnectionCounterHashEntry *entry =
		(ReservedConnectionCounterHashEntry *) hash_search(
			SessionLocalReservedConnectionCounters, &key, HASH_FIND, &found);

	if (!found)
	{
		ereport(ERROR, (errmsg("BUG: untracked reserved connection"),
						errhint("Set citus.max_shared_pool_size TO -1 to "
								"disable reserved connection counters")));
	}

	entry->reservedConnectionCount--;

	ereport(DEBUG5, (errmsg("Decrement shared reserved connection to node %s:%d to %d",
							hostName, nodePort, entry->reservedConnectionCount)));
}


/*
 * ReserveSharedConnectionCounterForAllPrimaryNodesIfNeeded is a wrapper around
 * ReserveSharedConnectionCounterForNodeListIfNeeded.
 */
void
ReserveSharedConnectionCounterForAllPrimaryNodesIfNeeded(int count)
{
	List *primaryNodeList = ActivePrimaryNonCoordinatorNodeList(NoLock);

	/*
	 * We need to reserve connection one by one because the underlying
	 * functions (e.g., WaitLoopForSharedConnection) do not implement
	 * reserving multiple connections.
	 */
	int reserveIndex = 0;
	for (; reserveIndex < count; ++reserveIndex)
	{
		ReserveSharedConnectionCounterForNodeListIfNeeded(primaryNodeList);
	}
}


/*
 * ReserveSharedConnectionCounterForNodeListIfNeeded reserves a shared connection
 * counter per node in the nodeList unless:
 *  - there is at least one connection to the node
 *  - has already reserved a connection to the node
 */
static void
ReserveSharedConnectionCounterForNodeListIfNeeded(List *nodeList)
{
	if (GetMaxSharedPoolSize() == DISABLE_CONNECTION_THROTTLING)
	{
		/* connection throttling disabled */
		return;
	}

	if (SessionLocalReservedConnectionCounters == NULL)
	{
		/*
		 * This is unexpected as SessionLocalReservedConnectionCounters hash table is
		 * created at startup. Still, let's be defensive.
		 */
		ereport(ERROR, (errmsg("unexpected state for session level reserved "
							   "connection counter hash")));
	}

	/*
	 * We sort the workerList because adaptive connection management
	 * (e.g., OPTIONAL_CONNECTION) requires any concurrent executions
	 * to wait for the connections in the same order to prevent any
	 * starvation. If we don't sort, we might end up with:
	 *      Execution 1: Get connection for worker 1, wait for worker 2
	 *      Execution 2: Get connection for worker 2, wait for worker 1
	 *
	 *  and, none could proceed. Instead, we enforce every execution establish
	 *  the required connections to workers in the same order.
	 */
	nodeList = SortList(nodeList, CompareWorkerNodes);

	char *databaseName = get_database_name(MyDatabaseId);

	WorkerNode *workerNode = NULL;
	foreach_ptr(workerNode, nodeList)
	{
		if (ConnectionAvailableToNode(workerNode->workerName, workerNode->workerPort,
									  CurrentUserName(), databaseName))
		{
			/*
			 * The same user has already an active connection for the node. It
			 * means that the execution can use the same connection, so reservation
			 * is not necessary.
			 */
			continue;
		}

		if (HasAlreadyReservedConnection(workerNode->workerName, workerNode->workerPort,
										 MyDatabaseId))
		{
			/*
			 * We already locally reserved a connection for this node. So, it is not
			 * necessary to reserve again.
			 *
			 * One caveat here is that we are ignoring for which user we reserved the
			 * connection. It is not important as Citus doesn't support switching
			 * between users (https://github.com/citusdata/citus/pull/3869) and
			 * we're clearing the reserved connections at the end of the transaction.
			 */
			continue;
		}

		/*
		 * We are trying to be defensive here by ensuring that the required hash
		 * table entry can be allocated. The main goal is that we don't want to be
		 * in a situation where shared connection counter is incremented but not
		 * the local reserved counter due to out-of-memory.
		 *
		 * Note that shared connection stats operate on the shared memory, and we
		 * pre-allocate all the necessary memory. In other words, it would never
		 * throw out of memory error.
		 */
		ReservedConnectionCounterHashEntry *hashEntry =
			AllocateOrGetReservedConectionEntry(workerNode->workerName,
												workerNode->workerPort,
												MyDatabaseId);

		/*
		 * Increment the shared counter, we may need to wait if there are
		 * no space left.
		 */
		WaitLoopForSharedConnection(workerNode->workerName, workerNode->workerPort);

		/* locally mark that we have one connection reserved */
		hashEntry->reservedConnectionCount++;
	}
}


/*
 * AllocateReservedConectionEntry allocated the required entry in the hash
 * map by HASH_ENTER. The function throws an error if it cannot allocate
 * the entry.
 */
static ReservedConnectionCounterHashEntry *
AllocateOrGetReservedConectionEntry(char *hostName, int nodePort, Oid databaseOid)
{
	ReservedConnectionCounterHashKey key;

	strlcpy(key.hostname, hostName, MAX_NODE_LENGTH);
	key.port = nodePort;
	key.databaseOid = databaseOid;

	/*
	 * Entering a new entry with HASH_ENTER flag is enough as it would
	 * throw out-of-memory error as it internally does palloc.
	 */
	bool found = false;
	ReservedConnectionCounterHashEntry *entry =
		(ReservedConnectionCounterHashEntry *) hash_search(
			SessionLocalReservedConnectionCounters, &key, HASH_ENTER, &found);

	if (!found)
	{
		entry->reservedConnectionCount = 0;

		ereport(DEBUG5, (errmsg("Allocated shared reserved connection to node "
								"%s:%d to %d", entry->key.hostname, entry->key.port,
								entry->reservedConnectionCount)));
	}

	return entry;
}


/*
 * LocalConnectionReserveHashHash is a utilty function to calculate hash of
 * ReservedConnectionCounterHashKey.
 */
static uint32
LocalConnectionReserveHashHash(const void *key, Size keysize)
{
	ReservedConnectionCounterHashKey *entry = (ReservedConnectionCounterHashKey *) key;

	uint32 hash = string_hash(entry->hostname, NAMEDATALEN);
	hash = hash_combine(hash, hash_uint32(entry->port));
	hash = hash_combine(hash, hash_uint32(entry->databaseOid));

	return hash;
}


/*
 * LocalConnectionReserveHashCompare is a utilty function to compare
 * ReservedConnectionCounterHashKeys.
 */
static int
LocalConnectionReserveHashCompare(const void *a, const void *b, Size keysize)
{
	ReservedConnectionCounterHashKey *ca = (ReservedConnectionCounterHashKey *) a;
	ReservedConnectionCounterHashKey *cb = (ReservedConnectionCounterHashKey *) b;

	if (strncmp(ca->hostname, cb->hostname, MAX_NODE_LENGTH) != 0 ||
		ca->port != cb->port ||
		ca->databaseOid != cb->databaseOid)
	{
		return 1;
	}
	else
	{
		return 0;
	}
}

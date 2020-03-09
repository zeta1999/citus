
#ifndef LOCAL_MULTI_COPY
#define LOCAL_MULTI_COPY

extern void ProcessLocalCopy(TupleTableSlot *slot, CitusCopyDestReceiver *copyDest, int64
							 shardId,
							 StringInfo buffer, bool isEndOfCopy);


extern void InsertSlot(CitusCopyDestReceiver *copyDest, TupleTableSlot *slot, Relation shard);

#endif /* LOCAL_MULTI_COPY */

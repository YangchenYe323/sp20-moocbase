package edu.berkeley.cs186.database.concurrency;
// If you see this line, you have successfully pulled the latest changes from the skeleton for proj4!
import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.Pair;

import java.util.*;

/**
 * LockManager maintains the bookkeeping for what transactions have
 * what locks on what resources. The lock manager should generally **not**
 * be used directly: instead, code should call methods of LockContext to
 * acquire/release/promote/escalate locks.
 *
 * The LockManager is primarily concerned with the mappings between
 * transactions, resources, and locks, and does not concern itself with
 * multiple levels of granularity (you can and should treat ResourceName
 * as a generic Object, rather than as an object encapsulating levels of
 * granularity, in this class).
 *
 * It follows that LockManager should allow **all**
 * requests that are valid from the perspective of treating every resource
 * as independent objects, even if they would be invalid from a
 * multigranularity locking perspective. For example, if LockManager#acquire
 * is called asking for an X lock on Table A, and the transaction has no
 * locks at the time, the request is considered valid (because the only problem
 * with such a request would be that the transaction does not have the appropriate
 * intent locks, but that is a multigranularity concern).
 *
 * Each resource the lock manager manages has its own queue of LockRequest objects
 * representing a request to acquire (or promote/acquire-and-release) a lock that
 * could not be satisfied at the time. This queue should be processed every time
 * a lock on that resource gets released, starting from the first request, and going
 * in order until a request cannot be satisfied. Requests taken off the queue should
 * be treated as if that transaction had made the request right after the resource was
 * released in absence of a queue (i.e. removing a request by T1 to acquire X(db) should
 * be treated as if T1 had just requested X(db) and there were no queue on db: T1 should
 * be given the X lock on db, and put in an unblocked state via Transaction#unblock).
 *
 * This does mean that in the case of:
 *    queue: S(A) X(A) S(A)
 * only the first request should be removed from the queue when the queue is processed.
 */
public class LockManager {
    // transactionLocks is a mapping from transaction number to a list of lock
    // objects held by that transaction.
    private Map<Long, List<Lock>> transactionLocks = new HashMap<>();
    // resourceEntries is a mapping from resource names to a ResourceEntry
    // object, which contains a list of Locks on the object, as well as a
    // queue for requests on that resource.
    private Map<ResourceName, ResourceEntry> resourceEntries = new HashMap<>();

    // A ResourceEntry contains the list of locks on a resource, as well as
    // the queue for requests for locks on the resource.
    private class ResourceEntry {
        // List of currently granted locks on the resource.
        List<Lock> locks = new ArrayList<>();
        // Queue for yet-to-be-satisfied lock requests on this resource.
        Deque<LockRequest> waitingQueue = new ArrayDeque<>();

        // Below are a list of helper methods you should implement!
        // Make sure to use these helper methods to abstract your code and
        // avoid re-implementing every time!

        /**
         * check if the queue is empty: acquire method needs to query this method
         * @return
         */
        boolean checkQueueEmpty(){
            return waitingQueue.isEmpty();
        }

        /**
         * Check if a LOCKTYPE lock is compatible with preexisting locks.
         * Allows conflicts for locks held by transaction id EXCEPT.
         */
        boolean checkCompatible(LockType lockType, long except) {

            for (Lock l: locks){
                if (l.transactionNum != except){
                    //found an incompatible lock
                    if (!LockType.compatible(lockType, l.lockType)) return false;
                }
            }

            //compatible with all existing locks
            return true;
        }

        /**
         * Gives the transaction the lock LOCK. Assumes that the lock is compatible.
         * Updates lock on resource if the transaction already has a lock.
         */
        void grantOrUpdateLock(Lock lock) {

            Lock oldLock = getTransactionLock(lock.transactionNum);
            //if has an old lock, replace that with the new lock
            if (oldLock != null){
                //don't allow duplicate lock
                if (oldLock.lockType == lock.lockType) {
                    System.out.println(locks);
                    throw new DuplicateLockRequestException("Lock Already Held");
                }

                /*Replace the old lock with a new one in the same place*/
                int index = locks.indexOf(oldLock);
                locks.add(index+1, lock);
            } else{
                locks.add(lock);
            }

            //get the transaction a new lock
            LockManager.this.transactionLocks.get(lock.transactionNum).add(lock);

        }

        /**
         * Releases the lock LOCK and processes the queue. Assumes it had been granted before.
         */
        void releaseLock(Lock lock) {
            locks.remove(lock);
            LockManager.this.transactionLocks.get(lock.transactionNum).remove(lock);
            processQueue();
        }

        /**
         * Adds a request for LOCK by the transaction to the queue and puts the transaction
         * in a blocked state.
         */
        void addToQueue(LockRequest request, boolean addFront) {
            if (addFront) waitingQueue.addFirst(request);
            else waitingQueue.addLast(request);

            //add the transaction to blocked state
            request.transaction.prepareBlock();
        }

        /**
         * Grant locks to requests from front to back of the queue, stopping
         * when the next lock cannot be granted.
         */
        private void processQueue() {
            while (!waitingQueue.isEmpty()){
                LockRequest r = waitingQueue.getFirst();
                //found a compatible waiting lock request
                if (checkCompatible(r.lock.lockType, r.transaction.getTransNum())){
                    waitingQueue.removeFirst();

                    grantOrUpdateLock(r.lock);

                    //release any locks specified in the request
                    //NOTE: the other locks are on different resources
                    for (Lock l: r.releasedLocks){
                        ResourceEntry correspondingEntry = LockManager.this.resourceEntries.get(l.name);
                        correspondingEntry.releaseLock(l);
                    }

                    //unblock the transaction that made this request
                    if (r.transaction.getBlocked()) r.transaction.unblock();
                } else break;
            }
        }

        /**
         * Gets the Lock transaction has on this resource
         * if the transaction does not have a lock, return null
         * @param transaction
         * @return
         */
        Lock getTransactionLock(long transaction){
           for (Lock l: locks){
               if (l.transactionNum == transaction) return l;
            }
            //the transaction does not have a lock on this resource
            return null;
        }

        @Override
        public String toString() {
            return "Active Locks: " + Arrays.toString(this.locks.toArray()) +
                   ", Queue: " + Arrays.toString(this.waitingQueue.toArray());
        }
    }

    // You should not modify or use this directly.
    private Map<Long, LockContext> contexts = new HashMap<>();

    /**
     * Helper method to fetch the resourceEntry corresponding to NAME.
     * Inserts a new (empty) resourceEntry into the map if no entry exists yet.
     */
    private ResourceEntry getResourceEntry(ResourceName name) {
        resourceEntries.putIfAbsent(name, new ResourceEntry());
        return resourceEntries.get(name);
    }

    // TODO(proj4_part1): You may add helper methods here if you wish

    /**
     * whenever a transaction comes first, the lock manager needs to
     * initialize an empty List of Locks for it
     * @param transaction
     */
    private void getTransactionLocks(long transaction){
        transactionLocks.computeIfAbsent(transaction, k -> new ArrayList<>());
    }

    /**
     * Acquire a LOCKTYPE lock on NAME, for transaction TRANSACTION, and releases all locks
     * in RELEASELOCKS after acquiring the lock, in one atomic action.
     *
     * Error checking must be done before any locks are acquired or released. If the new lock
     * is not compatible with another transaction's lock on the resource, the transaction is
     * blocked and the request is placed at the **front** of ITEM's queue.
     *
     * Locks in RELEASELOCKS should be released only after the requested lock has been acquired.
     * The corresponding queues should be processed.
     *
     * An acquire-and-release that releases an old lock on NAME **should not** change the
     * acquisition time of the lock on NAME, i.e.
     * if a transaction acquired locks in the order: S(A), X(B), acquire X(A) and release S(A), the
     * lock on A is considered to have been acquired before the lock on B.
     *
     * @throws DuplicateLockRequestException if a lock on NAME is held by TRANSACTION and
     * isn't being released
     * @throws NoLockHeldException if no lock on a name in RELEASELOCKS is held by TRANSACTION
     */
    public void acquireAndRelease(TransactionContext transaction, ResourceName name,
                                  LockType lockType, List<ResourceName> releaseLocks)
    throws DuplicateLockRequestException, NoLockHeldException {

        // You may modify any part of this method. You are not required to keep all your
        // code within the given synchronized block -- in fact,
        // you will have to write some code outside the synchronized block to avoid locking up
        // the entire lock manager when a transaction is blocked. You are also allowed to
        // move the synchronized block elsewhere if you wish.
        boolean shouldBlock = false;
        synchronized (this) {
            //this is to check if this transaction comes to this
            //lock manager for the first time. If so, allocate
            //an empty list for it in the map
            getTransactionLocks(transaction.getTransNum());
            //we are working on this resource entry
            ResourceEntry entry = getResourceEntry(name);

            //in this case the acquirement can proceed, no block
            if (entry.checkCompatible(lockType, transaction.getTransNum())){
                //acquire the lock
                entry.grantOrUpdateLock(new Lock(name, lockType, transaction.getTransNum()));
                //release other specified lock
                for (ResourceName n: releaseLocks){
                    release(transaction, n);
                }
            } else{

                //in this case we should block
                shouldBlock = true;
                //make an request
                //extract a list of locks to release
                List<Lock> releasedLock = new ArrayList<>();
                for (ResourceName n: releaseLocks){
                    releasedLock.add(resourceEntries.get(n).getTransactionLock(transaction.getTransNum()));
                }

                LockRequest request = new LockRequest(transaction, new Lock(name, lockType, transaction.getTransNum()), releasedLock);
                entry.addToQueue(request, true);
            }
        }

        if (shouldBlock) transaction.block();
    }

    /**
     * Acquire a LOCKTYPE lock on NAME, for transaction TRANSACTION.
     *
     * Error checking must be done before the lock is acquired. If the new lock
     * is not compatible with another transaction's lock on the resource, or if there are
     * other transaction in queue for the resource, the transaction is
     * blocked and the request is placed at the **back** of NAME's queue.
     *
     * @throws DuplicateLockRequestException if a lock on NAME is held by
     * TRANSACTION
     */
    public void acquire(TransactionContext transaction, ResourceName name,
                        LockType lockType) throws DuplicateLockRequestException {
        // You may modify any part of this method. You are not required to keep all your
        // code within the given synchronized block -- in fact,
        // you will have to write some code outside the synchronized block to avoid locking up
        // the entire lock manager when a transaction is blocked. You are also allowed to
        // move the synchronized block elsewhere if you wish.
        boolean shouldBlock = false;
        synchronized (this) {
            getTransactionLocks(transaction.getTransNum());
            ResourceEntry entry = getResourceEntry(name);
            if (entry.checkCompatible(lockType, transaction.getTransNum()) && entry.checkQueueEmpty()){
                entry.grantOrUpdateLock(new Lock(name, lockType, transaction.getTransNum()));
            } else{
                //make an request
                //extract a list of locks to release
                List<Lock> releasedLock = Collections.emptyList();

                LockRequest request = new LockRequest(transaction, new Lock(name, lockType, transaction.getTransNum()), releasedLock);
                entry.addToQueue(request, false);
                shouldBlock = true;
            }
        }

        if (shouldBlock) transaction.block();
    }

    /**
     * Release TRANSACTION's lock on NAME.
     *
     * Error checking must be done before the lock is released.
     *
     * NAME's queue should be processed after this call. If any requests in
     * the queue have locks to be released, those should be released, and the
     * corresponding queues also processed.
     *
     * @throws NoLockHeldException if no lock on NAME is held by TRANSACTION
     */
    public void release(TransactionContext transaction, ResourceName name)
    throws NoLockHeldException {
        // You may modify any part of this method.
        synchronized (this) {
            getTransactionLocks(transaction.getTransNum());
            ResourceEntry entry = getResourceEntry(name);
            Lock lock = entry.getTransactionLock(transaction.getTransNum());
            //if the transaction has no lock on this resource
            if (lock == null)
                throw new NoLockHeldException("No Lock is held");
            entry.releaseLock(lock);
        }
    }

    /**
     * Promote TRANSACTION's lock on NAME to NEWLOCKTYPE (i.e. change TRANSACTION's lock
     * on NAME from the current lock type to NEWLOCKTYPE, which must be strictly more
     * permissive).
     *
     * Error checking must be done before any locks are changed. If the new lock
     * is not compatible with another transaction's lock on the resource, the transaction is
     * blocked and the request is placed at the **front** of ITEM's queue.
     *
     * A lock promotion **should not** change the acquisition time of the lock, i.e.
     * if a transaction acquired locks in the order: S(A), X(B), promote X(A), the
     * lock on A is considered to have been acquired before the lock on B.
     *
     * @throws DuplicateLockRequestException if TRANSACTION already has a
     * NEWLOCKTYPE lock on NAME
     * @throws NoLockHeldException if TRANSACTION has no lock on NAME
     * @throws InvalidLockException if the requested lock type is not a promotion. A promotion
     * from lock type A to lock type B is valid if and only if B is substitutable
     * for A, and B is not equal to A.
     */
    public void promote(TransactionContext transaction, ResourceName name,
                        LockType newLockType)
    throws DuplicateLockRequestException, NoLockHeldException, InvalidLockException {

        // You may modify any part of this method.
        boolean shouldBlock = false;
        synchronized (this) {
            getTransactionLocks(transaction.getTransNum());

            ResourceEntry entry = getResourceEntry(name);
            Lock oldLock = entry.getTransactionLock(transaction.getTransNum());
            //check for invalid situations
            if (oldLock == null) throw new NoLockHeldException("No Lock to promote");
            if (oldLock.lockType == newLockType) throw new DuplicateLockRequestException("Already Hold this lock");
            if (!LockType.substitutable(newLockType, oldLock.lockType))
                throw new InvalidLockException("New Lock Not More Permissive");

            //now valid promotion request, process it
            if (entry.checkCompatible(newLockType, transaction.getTransNum())){
                entry.grantOrUpdateLock(new Lock(name, newLockType, transaction.getTransNum()));
                entry.releaseLock(oldLock);
            } else{
                shouldBlock = true;
                List<Lock> release = Collections.singletonList(oldLock);
                LockRequest request = new LockRequest(transaction, new Lock(name, newLockType, transaction.getTransNum()), release);
                entry.addToQueue(request, true);
            }
        }

        if (shouldBlock) transaction.block();
    }

    /**
     * Return the type of lock TRANSACTION has on NAME (return NL if no lock is held).
     */
    public synchronized LockType getLockType(TransactionContext transaction, ResourceName name) {

        if (transactionLocks.containsKey(transaction.getTransNum())) {
            for (Lock l : transactionLocks.get(transaction.getTransNum())) {
                if (l.name == name) return l.lockType;
            }
        }

        return LockType.NL;
    }

    /**
     * Returns the list of locks held on NAME, in order of acquisition.
     * A promotion or acquire-and-release should count as acquired
     * at the original time.
     */
    public synchronized List<Lock> getLocks(ResourceName name) {
        return new ArrayList<>(resourceEntries.getOrDefault(name, new ResourceEntry()).locks);
    }

    /**
     * Returns the list of locks locks held by
     * TRANSACTION, in order of acquisition. A promotion or
     * acquire-and-release should count as acquired at the original time.
     */
    public synchronized List<Lock> getLocks(TransactionContext transaction) {
        return new ArrayList<>(transactionLocks.getOrDefault(transaction.getTransNum(),
                               Collections.emptyList()));
    }

    /**
     * Creates a lock context. See comments at
     * he top of this file and the top of LockContext.java for more information.
     */
    public synchronized LockContext context(String readable, long name) {
        if (!contexts.containsKey(name)) {
            contexts.put(name, new LockContext(this, null, new Pair<>(readable, name)));
        }
        return contexts.get(name);
    }

    /**
     * Create a lock context for the database. See comments at
     * the top of this file and the top of LockContext.java for more information.
     */
    public synchronized LockContext databaseContext() {
        return context("database", 0L);
    }
}

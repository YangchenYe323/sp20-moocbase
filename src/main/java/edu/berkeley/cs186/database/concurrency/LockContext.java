package edu.berkeley.cs186.database.concurrency;
// If you see this line, you have successfully pulled the latest changes from the skeleton for proj4!
import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.Pair;

import javax.lang.model.type.ArrayType;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * LockContext wraps around LockManager to provide the hierarchical structure
 * of multigranularity locking. Calls to acquire/release/etc. locks should
 * be mostly done through a LockContext, which provides access to locking
 * methods at a certain point in the hierarchy (database, table X, etc.)
 */
public class LockContext {
    // You should not remove any of these fields. You may add additional fields/methods as you see fit.

    // The underlying lock manager.
    protected final LockManager lockman;
    // The parent LockContext object, or null if this LockContext is at the top of the hierarchy.
    protected final LockContext parent;
    // The name of the resource this LockContext represents.
    protected ResourceName name;
    // Whether this LockContext is readonly. If a LockContext is readonly, acquire/release/promote/escalate should
    // throw an UnsupportedOperationException.
    protected boolean readonly;
    // A mapping between transaction numbers, and the number of locks on children of this LockContext
    // that the transaction holds.
    protected final Map<Long, Integer> numChildLocks;
    // The number of children that this LockContext has, if it differs from the number of times
    // LockContext#childContext was called with unique parameters: for a table, we do not
    // explicitly create a LockContext for every page (we create them as needed), but
    // the capacity should be the number of pages in the table, so we use this
    // field to override the return value for capacity().
    protected int capacity;

    // You should not modify or use this directly.
    protected final Map<Long, LockContext> children;

    // Whether or not any new child LockContexts should be marked readonly.
    protected boolean childLocksDisabled;

    public LockContext(LockManager lockman, LockContext parent, Pair<String, Long> name) {
        this(lockman, parent, name, false);
    }

    protected LockContext(LockManager lockman, LockContext parent, Pair<String, Long> name,
                          boolean readonly) {
        this.lockman = lockman;
        this.parent = parent;
        if (parent == null) {
            this.name = new ResourceName(name);
        } else {
            this.name = new ResourceName(parent.getResourceName(), name);
        }
        this.readonly = readonly;
        this.numChildLocks = new ConcurrentHashMap<>();
        this.capacity = -1;
        this.children = new ConcurrentHashMap<>();
        this.childLocksDisabled = readonly;
    }

    /**
     * Gets a lock context corresponding to NAME from a lock manager.
     */
    public static LockContext fromResourceName(LockManager lockman, ResourceName name) {
        Iterator<Pair<String, Long>> names = name.getNames().iterator();
        LockContext ctx;
        Pair<String, Long> n1 = names.next();
        ctx = lockman.context(n1.getFirst(), n1.getSecond());
        while (names.hasNext()) {
            Pair<String, Long> p = names.next();
            ctx = ctx.childContext(p.getFirst(), p.getSecond());
        }
        return ctx;
    }

    /**
     * Get the name of the resource that this lock context pertains to.
     */
    public ResourceName getResourceName() {
        return name;
    }

    /**
     * this method checks whether it is valid for the given
     * transaction to acquire a lock of given type
     * @param type
     * @return
     */
    private boolean checkValidLockType(TransactionContext transaction, LockType type){

        //if this is the outer most level (database) no need to check
        if (parent == null) return true;

        //Check Multigranularity constraint:

        //Acquiring S on this resource you need IS or IX on all the parent
        //If: Acquire S, parent hold SIX or S or X -> DuplicatedLockRequest
        //parent hold NL -> InvalidLock

        //Acquiring X on this resource you need IX or SIX on all the parent
        //If: Acquire X, parent hold X -> DuplicatedLockRequest
        //parent hold S, IS, NL -> InvalidLock

        //Acquiring IS: parent IS or IX
        //parent S or SIX or X -> DuplicatedLock
        //parent NL -> InvalidLock

        //Acquiring IX: parent IX or SIX
        //parent X -> Duplicate
        //parent S or IS or NL -> Invalid

        //Acquirng SIX: parent IX or SIX
        //parent X -> Duplicate
        //parent S or IS or NL -> invalid

        //Acquiring NL: Do nothing
        Set<LockType> invalidTypes = new HashSet<>();
        switch (type){
            case S:
            case IS:
                invalidTypes.add(LockType.NL);
                invalidTypes.add(LockType.S);
                invalidTypes.add(LockType.X);
                invalidTypes.add(LockType.SIX);
                break;
            case X:
            case IX:
            case SIX:
                invalidTypes.add(LockType.S);
                invalidTypes.add(LockType.IS);
                invalidTypes.add(LockType.NL);
                invalidTypes.add(LockType.X);
                break;
            case NL:
            default:
                //if the transaction is acquiring NL
                //always valid
                break;
        }

        LockContext lckIter = parentContext();
        while (lckIter != null){
            if (invalidTypes.contains(lckIter.getExplicitLockType(transaction))) return false;
            lckIter = lckIter.parentContext();
        }

        //
        return true;
    }

    /**
     * Acquire a LOCKTYPE lock, for transaction TRANSACTION.
     *
     * Note: you *must* make any necessary updates to numChildLocks, or
     * else calls to LockContext#saturation will not work properly.
     *
     * @throws InvalidLockException if the request is invalid
     * @throws DuplicateLockRequestException if a lock is already held by TRANSACTION
     * @throws UnsupportedOperationException if context is readonly
     */
    public void acquire(TransactionContext transaction, LockType lockType)
    throws InvalidLockException, DuplicateLockRequestException {

        //if readonly
        if (readonly) throw new UnsupportedOperationException("Lock Context is read only");

        //if the transaction has already held a lock
        if (this.getExplicitLockType(transaction) != LockType.NL) throw new DuplicateLockRequestException("Lock Already Held");

        //if the request violates the multigranularity constraint
        if (!checkValidLockType(transaction, lockType))
            throw new InvalidLockException("Violates Multi-granularity constraint");

        //Now This request is valid, use the lock manager to acquire the lock
        lockman.acquire(transaction, name, lockType);

        //Now lock acquisition has succeeded
        //inform the parent that the transaction have acquired a new lock on this child context
        if (parent != null){
            int v = parent.numChildLocks.getOrDefault(transaction.getTransNum(), 0);
            v++;
            parent.numChildLocks.put(transaction.getTransNum(), v);
        }

    }

    /**
     * Release TRANSACTION's lock on NAME.
     *
     * Note: you *must* make any necessary updates to numChildLocks, or
     * else calls to LockContext#saturation will not work properly.
     *
     * @throws NoLockHeldException if no lock on NAME is held by TRANSACTION
     * @throws InvalidLockException if the lock cannot be released (because doing so would
     *  violate multigranularity locking constraints)
     * @throws UnsupportedOperationException if context is readonly
     */
    public void release(TransactionContext transaction)
    throws NoLockHeldException, InvalidLockException {

        //unsupported operation
        if (readonly) throw new UnsupportedOperationException("Lock Context read only");

        //no lock held
        if (getExplicitLockType(transaction) == LockType.NL)
            throw new NoLockHeldException("No Lock Held");

        //multi-granularity constraint:
        //to release a lock on this level, must have no lock held at lower level
        if (numChildLocks.containsKey(transaction.getTransNum()) && numChildLocks.get(transaction.getTransNum()) > 0)
            throw new InvalidLockException("Haven't Released Child Locks");

        //release and update
        lockman.release(transaction, name);

        //inform the parent that this transaction has released a lock on its child
        if (parent != null) {
            Map<Long, Integer> parentMap = parent.numChildLocks;
            //we assumed that the parentMap contains this transaction number
            //because when we acquired this lock, we surely have to have informed
            //the parent
            int v = parentMap.get(transaction.getTransNum());
            --v;
            parentMap.put(transaction.getTransNum(), v);
        }

    }

    /**
     * Promote TRANSACTION's lock to NEWLOCKTYPE. For promotion to SIX from IS/IX/S, all S,
     * IS, and SIX locks on descendants must be simultaneously released. The helper function sisDescendants
     * may be helpful here.
     *
     * Note: you *must* make any necessary updates to numChildLocks, or
     * else calls to LockContext#saturation will not work properly.
     *
     * @throws DuplicateLockRequestException if TRANSACTION already has a NEWLOCKTYPE lock
     * @throws NoLockHeldException if TRANSACTION has no lock
     * @throws InvalidLockException if the requested lock type is not a promotion or promoting
     * would cause the lock manager to enter an invalid state (e.g. IS(parent), X(child)). A promotion
     * from lock type A to lock type B is valid if B is substitutable
     * for A and B is not equal to A, or if B is SIX and A is IS/IX/S, and invalid otherwise. hasSIXAncestor may
     * be helpful here.
     * @throws UnsupportedOperationException if context is readonly
     */
    public void promote(TransactionContext transaction, LockType newLockType)
    throws DuplicateLockRequestException, NoLockHeldException, InvalidLockException {
        //check readonly
        if (readonly) throw new UnsupportedOperationException("Lock Context is readonly");

        //check duplicate
        if (getExplicitLockType(transaction) == newLockType)
            throw new DuplicateLockRequestException("Lock Already Held");

        //check No Lock held
        if (getExplicitLockType(transaction) == LockType.NL)
            throw new NoLockHeldException("Not a promotion: No old lock held");

        //check invalid promote request
        //violates multigranularity constraints or
        //new lock type is not strictly more permissive
        if (!checkValidLockType(transaction, newLockType) || !LockType.substitutable(newLockType, getExplicitLockType(transaction)))
            throw new InvalidLockException("Invalid promotion request");
        //special case: promote to SIX
        if (newLockType == LockType.SIX && hasSIXAncestor(transaction))
            throw new InvalidLockException("Promote to SIX redundant");

        //Now Valid Request, Begin Processing
        //special case: promote to SIX
        if (newLockType == LockType.SIX){
            //we'll have to simultaneously release all S/IS lock held on child
            //and also the old lock held on this resource
            List<ResourceName> list = sisDescendants(transaction);
            list.add(0, this.name);
            lockman.acquireAndRelease(transaction, name, newLockType, list);

            //now we're going to update numChild
            list.remove(name);
            for (ResourceName rname: list){
                //if this resource has a parent
                if (rname.parent() != null) {

                    LockContext parentContext = childContext(rname.parent().getCurrentName().getSecond());
                    //if this resource is a direct child of us
                    if (rname.parent().getCurrentName().getSecond().equals(name.getCurrentName().getSecond()))
                        parentContext = this;

                    int v = parentContext.numChildLocks.get(transaction.getTransNum());
                    parentContext.numChildLocks.put(transaction.getTransNum(), --v);
                }
            }

        } else{
            lockman.promote(transaction, name, newLockType);
        }
    }

    /**
     * Escalate TRANSACTION's lock from descendants of this context to this level, using either
     * an S or X lock. There should be no descendant locks after this
     * call, and every operation valid on descendants of this context before this call
     * must still be valid. You should only make *one* mutating call to the lock manager,
     * and should only request information about TRANSACTION from the lock manager.
     *
     * For example, if a transaction has the following locks:
     *      IX(database) IX(table1) S(table2) S(table1 page3) X(table1 page5)
     * then after table1Context.escalate(transaction) is called, we should have:
     *      IX(database) X(table1) S(table2)
     *
     * You should not make any mutating calls if the locks held by the transaction do not change
     * (such as when you call escalate multiple times in a row).
     *
     * Note: you *must* make any necessary updates to numChildLocks of all relevant contexts, or
     * else calls to LockContext#saturation will not work properly.
     *
     * @throws NoLockHeldException if TRANSACTION has no lock at this level
     * @throws UnsupportedOperationException if context is readonly
     */
    public void escalate(TransactionContext transaction) throws NoLockHeldException {
        //unsupported operation
        if (readonly) throw new UnsupportedOperationException("Lock Context read only");

        //no lock held
        if (getExplicitLockType(transaction) == LockType.NL)
            throw new NoLockHeldException("No Lock held");

        //figure out what the new locktype on this level should be
        //Scan all descendent
        //IS -> IS
        //S -> S
        //IX -> IX
        //SIX -> SIX
        //X -> X
        LockType newLockType = LockType.S;
        List<ResourceName> list = new ArrayList<>();
        for (Lock l: lockman.getLocks(transaction)){
            if (l.lockType != LockType.NL && l.name.isDescendantOf(this.name)){
                switch(l.lockType){
                    case IX:
                    case SIX:
                    case X:
                        newLockType = LockType.X;
                        break;
                }
                list.add(l.name);
            }
        }


        //check itself
        switch(getExplicitLockType(transaction)){
            case IX:
            case SIX:
            case X:
                newLockType = LockType.X;
        }
        list.add(this.name);

        if (newLockType != getExplicitLockType(transaction)){
            lockman.acquireAndRelease(transaction, name, newLockType, list);
        }

        list.remove(this.name);
        for (ResourceName rname: list){
            if (rname.parent() != null){
                LockContext parentContext;
                if (rname.parent().getCurrentName().getSecond().equals(this.name.getCurrentName().getSecond())) parentContext = this;
                else parentContext = childContext(rname.parent().getCurrentName().getSecond());

                int v = parentContext.numChildLocks.get(transaction.getTransNum());
                parentContext.numChildLocks.put(transaction.getTransNum(), --v);
            }
        }

    }

    /**
     * Gets the type of lock that the transaction has at this level, either implicitly
     * (e.g. explicit S lock at higher level implies S lock at this level) or explicitly.
     * Returns NL if there is no explicit nor implicit lock.
     */
    public LockType getEffectiveLockType(TransactionContext transaction) {

        LockType effectiveType = getExplicitLockType(transaction);

        List<Lock> locks = lockman.getLocks(transaction);

        for (Lock l: locks){
            if (this.name.isDescendantOf(l.name) && !LockType.isIntentLock(l.lockType)){//we don't consider intent locks
                //S or SIX
                if (l.lockType == LockType.S || l.lockType == LockType.SIX){
                    if (LockType.substitutable(l.lockType, effectiveType))
                        effectiveType = l.lockType;
                }

                if (l.lockType == LockType.X){
                    effectiveType = LockType.X;
                }
            }
        }

        return effectiveType;

    }

    /**
     * Helper method to see if the transaction holds a SIX lock at an ancestor of this context
     * @param transaction the transaction
     * @return true if holds a SIX at an ancestor, false if not
     */
    private boolean hasSIXAncestor(TransactionContext transaction) {

        LockContext lckIter = parent;
        while (lckIter != null){
            //found
            if (lckIter.getExplicitLockType(transaction) == LockType.SIX) return true;
            lckIter = lckIter.parentContext();
        }

        //not found
        return false;
    }

    /**
     * Helper method to get a list of resourceNames of all locks that are S or IS and are descendants of current context
     * for the given transaction.
     * @param transaction the given transaction
     * @return a list of ResourceNames of descendants which the transaction holds a S or IS lock.
     */
    private List<ResourceName> sisDescendants(TransactionContext transaction) {
        if (transaction == null) return Collections.emptyList();

        List<ResourceName> result = new ArrayList<>();

        List<Lock> locks = lockman.getLocks(transaction);
        for (Lock l: locks){
            if ((l.lockType == LockType.S || l.lockType == LockType.IS) && l.name.isDescendantOf(this.name))
                result.add(l.name);
        }

        return result;
    }

    /**
     * Get the type of lock that TRANSACTION holds at this level, or NL if no lock is held at this level.
     */
    public LockType getExplicitLockType(TransactionContext transaction) {
        if (transaction == null) {
            return LockType.NL;
        }

        List<Lock> locks = lockman.getLocks(transaction);

        for (Lock l: locks){
            if (l.name == this.name) return l.lockType;
        }

        return LockType.NL;
    }

    /**
     * Disables locking descendants. This causes all new child contexts of this context
     * to be readonly. This is used for indices and temporary tables (where
     * we disallow finer-grain locks), the former due to complexity locking
     * B+ trees, and the latter due to the fact that temporary tables are only
     * accessible to one transaction, so finer-grain locks make no sense.
     */
    public void disableChildLocks() {
        this.childLocksDisabled = true;
    }

    /**
     * Gets the parent context.
     */
    public LockContext parentContext() {
        return parent;
    }

    /**
     * Gets the context for the child with name NAME (with a readable version READABLE).
     */
    public synchronized LockContext childContext(String readable, long name) {
        LockContext temp = new LockContext(lockman, this, new Pair<>(readable, name),
                                           this.childLocksDisabled || this.readonly);
        LockContext child = this.children.putIfAbsent(name, temp);
        if (child == null) {
            child = temp;
        }
        if (child.name.getCurrentName().getFirst() == null && readable != null) {
            child.name = new ResourceName(this.name, new Pair<>(readable, name));
        }
        return child;
    }

    /**
     * Gets the context for the child with name NAME.
     */
    public synchronized LockContext childContext(long name) {
        return childContext(Long.toString(name), name);
    }

    /**
     * Sets the capacity (number of children).
     */
    public synchronized void capacity(int capacity) {
        this.capacity = capacity;
    }

    /**
     * Gets the capacity. Defaults to number of child contexts if never explicitly set.
     */
    public synchronized int capacity() {
        return this.capacity < 0 ? this.children.size() : this.capacity;
    }

    /**
     * Gets the saturation (number of locks held on children / number of children) for
     * a single transaction. Saturation is 0 if number of children is 0.
     */
    public double saturation(TransactionContext transaction) {
        if (transaction == null || capacity() == 0) {
            return 0.0;
        }
        return ((double) numChildLocks.getOrDefault(transaction.getTransNum(), 0)) / capacity();
    }

    @Override
    public String toString() {
        return "LockContext(" + name.toString() + ")";
    }
}


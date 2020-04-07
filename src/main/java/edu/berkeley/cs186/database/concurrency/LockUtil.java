package edu.berkeley.cs186.database.concurrency;
// If you see this line, you have successfully pulled the latest changes from the skeleton for proj4!
import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.TransactionContext;

/**
 * LockUtil is a declarative layer which simplifies multigranularity lock acquisition
 * for the user (you, in the second half of Part 2). Generally speaking, you should use LockUtil
 * for lock acquisition instead of calling LockContext methods directly.
 */
public class LockUtil {
    /**
     * Ensure that the current transaction can perform actions requiring LOCKTYPE on LOCKCONTEXT.
     *
     * This method should promote/escalate as needed, but should only grant the least
     * permissive set of locks needed.
     *
     * lockType is guaranteed to be one of: S, X, NL.
     *
     * If the current transaction is null (i.e. there is no current transaction), this method should do nothing.
     */
    public static void ensureSufficientLockHeld(LockContext lockContext, LockType lockType) {

        TransactionContext transaction = TransactionContext.getTransaction(); // current transaction

        if (transaction == null) return;

        //if we've already has the required lock
        if (LockType.substitutable(lockContext.getEffectiveLockType(transaction), lockType)) return;

        //Two phases implementation:
        //1. get ancestor locks
        //determine what kind of intent lock to get
        LockType IntentLocks;
        if (lockType == LockType.S) IntentLocks = LockType.IS;
        else IntentLocks = LockType.IX;

        recGetParentIntentLocks(lockContext.parentContext(), transaction, IntentLocks);

        //2. acquire this lock
        LockType oldLockType = lockContext.getExplicitLockType(transaction);
        //no lock already held
        if (oldLockType == LockType.NL)
            lockContext.acquire(transaction, lockType);
        //less permissive lock held, prepare to promote
        else {
            //special case: already held IX and want S: get an SIX
            if (oldLockType == LockType.IX && lockType == LockType.S){
                lockType = LockType.SIX;
                lockContext.promote(transaction, lockType);
            } else { //normal case: promoting

                //if saturated, first escalate
                if (lockContext.saturation(transaction) > 0)
                    lockContext.escalate(transaction);

                //then try to promote
                try {
                    lockContext.promote(transaction, lockType);
                } catch (DuplicateLockRequestException e) {
                    //escalation did the job, good, let it pass
                }
            }
        }


    }

    /**
     * this function handles getting the right intent lock from all the parent (IS or IX)
     * @param context
     * @param transaction
     * @param lockType
     */
    private static void recGetParentIntentLocks(LockContext context, TransactionContext transaction, LockType lockType){

        //this is the base case
        if (context == null) return;

        //if we already has an at least equally permissive intent lock, no need to proceed
        if (LockType.substitutable(context.getExplicitLockType(transaction), lockType)) return;

        //recursively asks the parent for the intent lock
        recGetParentIntentLocks(context.parentContext(), transaction, lockType);

        //acquire the intent lock at the current level
        LockType oldLockType = context.getExplicitLockType(transaction);
        //no lock already held, acquire one
        if (oldLockType == LockType.NL)
            context.acquire(transaction, lockType);
        //held a less permissive lock: promote
        else{
            //special case: already held an S lock and want to get an IX
            //need to get an SIX instead
            if (oldLockType == LockType.S && lockType == LockType.IX){
                lockType = LockType.SIX;
            }
            context.promote(transaction, lockType);
        }

    }

}

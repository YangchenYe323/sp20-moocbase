package edu.berkeley.cs186.database.concurrency;

// If you see this line, you have successfully pulled the latest changes from the skeleton for proj4!

public enum LockType {
    S,   // shared
    X,   // exclusive
    IS,  // intention shared
    IX,  // intention exclusive
    SIX, // shared intention exclusive
    NL;  // no lock held

    /**
     * This method checks whether lock types A and B are compatible with
     * each other. If a transaction can hold lock type A on a resource
     * at the same time another transaction holds lock type B on the same
     * resource, the lock types are compatible.
     *
     * Compatibility Matrix:
     * S -> S, IS, SIX, NL
     * X -> NL
     * IS -> S, IS, IX, SIX, NL
     * IX -> IX, IS, NL
     * SIX -> IS, NL
     * NL -> S, X, IS, IX, SIX, NL
     */
    public static boolean compatible(LockType a, LockType b) {
        if (a == null || b == null) {
            throw new NullPointerException("null lock type");
        }

        //NL is compatible with all locks
        if (a == LockType.NL || b == LockType.NL) return true;

        //if none of a and b is NL, X lock is not compatible
        //with any
        if (a == LockType.X || b == LockType.X) return false;

        //now none of a or b is either NL or X
        if (a == LockType.IS || b == LockType.IS) return true;

        //now we're left with: S, IX, SIX
        if (a == LockType.S){
            return b == LockType.S;
        }

        if (a == LockType.IX){
            return b == LockType.IX;
        }

        return false;
    }

    /**
     * This method returns the lock on the parent resource
     * that should be requested for a lock of type A to be granted.
     */
    public static LockType parentLock(LockType a) {
        if (a == null) {
            throw new NullPointerException("null lock type");
        }
        switch (a) {
        case S: return IS;
        case X: return IX;
        case IS: return IS;
        case IX: return IX;
        case SIX: return IX;
        case NL: return NL;
        default: throw new UnsupportedOperationException("bad lock type");
        }
    }

    /**
     * This method returns if parentLockType has permissions to grant a childLockType
     * on a child.
     */
    public static boolean canBeParentLock(LockType parentLockType, LockType childLockType) {
        if (parentLockType == null || childLockType == null) {
            throw new NullPointerException("null lock type");
        }

        if (childLockType == LockType.NL) return true;

        if (parentLockType == LockType.IX || parentLockType == LockType.SIX) return true;

        if (parentLockType == LockType.NL) return false;

        if (parentLockType == LockType.IS) return childLockType == LockType.S || childLockType == LockType.IS;

        //if parentLockType is S or X, then they should not have any child lock not NL: Redundant

        System.err.println("Should not reach this line");
        return false;
    }

    /**
     * This method returns whether a lock can be used for a situation
     * requiring another lock (e.g. an S lock can be substituted with
     * an X lock, because an X lock allows the transaction to do everything
     * the S lock allowed it to do).
     */
    public static boolean substitutable(LockType substitute, LockType required) {
        if (required == null || substitute == null) {
            throw new NullPointerException("null lock type");
        }

        if (substitute == required) return true;
        if (required == LockType.NL) return true;
        if (substitute == LockType.NL) return false;
        if (required == LockType.X) return false;
        if (required == LockType.IX) return substitute == LockType.X || substitute == LockType.SIX;
        if (required == LockType.SIX) return substitute == LockType.X;
        if (required == LockType.S) return substitute == LockType.X || substitute == LockType.SIX;
        if (required == LockType.IS) return true;

        return false;
    }

    public static boolean isIntentLock(LockType type){
        return type == LockType.IS || type == LockType.IX;
    }

    @Override
    public String toString() {
        switch (this) {
        case S: return "S";
        case X: return "X";
        case IS: return "IS";
        case IX: return "IX";
        case SIX: return "SIX";
        case NL: return "NL";
        default: throw new UnsupportedOperationException("bad lock type");
        }
    }
}


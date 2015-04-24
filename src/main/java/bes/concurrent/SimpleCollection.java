package bes.concurrent;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

public final class SimpleCollection<E> implements Iterable<E>
{

    // dummy head pointer, tail pointer only useful for addition.
    // both are only <= their true position, and can be reached
    // from any node (deleted or not) prior to them via a chain
    // of next links.
    private volatile Node head, tail;

    public SimpleCollection()
    {
        head = tail = new Node(null);
    }

    public Node add(E item)
    {
        Node insert = new Node(item);
        Node tail = this.tail;
        while (true)
        {
            Node next = tail.next;
            if (next != null)
            {
                tail = next;
            }
            else
            {
                insert.prev = tail;
                if (nextUpdater.compareAndSet(tail, null, insert))
                {
                    // no need for atomicity; if updates are out of order, chain simply needs to be walked forwards
                    this.tail = insert;
                    return insert;
                }
                else
                {
                    tail = tail.next;
                }
            }
        }
    }

    public Iterator<E> iterator()
    {
        return new Iterator<E>()
        {
            Node cur = head;
            Object curv = null;
            { setNext(true); }

            private boolean setNext(boolean isHead)
            {
                boolean adjacent = true;

                Node p = cur, n = p.next;
                while (n != null)
                {
                    Object item = n.item;
                    if (item != null)
                    {
                        if (!adjacent)
                        {
                            // edit out a chain of deleted items
                            cur.next = n;
                            n.prev = cur;
                            if (isHead)
                                head = p;
                        }

                        // stash value and position
                        curv = item;
                        cur = n;
                        return true;
                    }

                    if (isHead)
                        prevUpdater.lazySet(p, null);
                    p = n;
                    n = n.next;
                    adjacent = false;
                }
                return false;
            }

            public boolean hasNext()
            {
                return curv != null || setNext(false);
            }

            public E next()
            {
                if (curv == null && !setNext(false))
                    throw new NoSuchElementException();
                Object r = curv;
                curv = null;
                return (E) r;
            }

            public void remove()
            {
                cur.remove();
            }
        };
    }

    public final class Node
    {
        volatile Object item;
        volatile Node next, prev;

        public Node(Object item)
        {
            this.item = item;
        }

        public boolean isDeleted()
        {
            return item == null;
        }

        // mark ourselves deleted, and attempt to edit ourselves out of the list
        public void remove()
        {
            item = null;
            // in case the list has some phantom elements, we try to remove ourselves
            // and any contiguous adjacent range of deleted nodes

            // start by finding our live predecessor
            Node p = prev;
            while (true)
            {
                if (p == null)
                {
                    // we have no live predecessor; hasWaiters() will remove us
                    cleanupHead();
                    return;
                }
                if (!p.isDeleted())
                    break;
                p = p.prev;
            }

            // we walk forwards from our live predecessor to find the next live node,
            // since the forward chaining is our source of truth (prev is only a helping hand)
            Node n = p.next;
            // if we are the tail of the list, we cannot be removed
            if (n == null)
                return;
            Node n2;
            while (n.isDeleted() && (n2 = n.next) != null)
                n = n2;
            p.next = n;
        }
    }

    void cleanupHead()
    {
        Node ph = head, h = ph, n;
        while ((n = h.next) != null && n.isDeleted())
        {
            prevUpdater.lazySet(n, null);
            h = n;
        }
        if (ph != h)
            head = h;
    }

    private static final AtomicReferenceFieldUpdater<SimpleCollection.Node, SimpleCollection.Node> nextUpdater
    = AtomicReferenceFieldUpdater.newUpdater(SimpleCollection.Node.class, SimpleCollection.Node.class, "next");
    private static final AtomicReferenceFieldUpdater<SimpleCollection.Node, SimpleCollection.Node> prevUpdater
    = AtomicReferenceFieldUpdater.newUpdater(SimpleCollection.Node.class, SimpleCollection.Node.class, "prev");
}

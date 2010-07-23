de.setf.cassandra: a Common Lisp Cassandra Library

Summary
-------

This is a Common Lisp interface for Cassandra[[0]].


Building 
--------

    (asdf:load-system :de.setf.cassandra)

It includes several Cassandra thrift API versions. The initial development is based on 2.1.0.
See [readme-cassandra.lisp](READMES/readme-cassandra.lisp).


Licensing
---------

This version is released under version 3 of the GNU Lesser General Public License (LGPL).[[1]]
It relies on libraries which are covered by their respective licenses:

- [uuid](http://www.dardoria.net/software/uuid.html) : LLGPL
- [ironclad](http://method-combination.net/lisp/ironclad/) : MIT

Status
======

In development with mcl 5.2, sbcl 1.0.36, and cassandra 0.6.3/2.1.0.
At any given moment it ought to build, but it may break.

---
 [0]: http://wiki.apache.org/cassandra
 [1]: lgpl.txt
;;; -*- Mode: lisp; Syntax: ansi-common-lisp; Base: 10; Package: de.setf.cassandra; -*-

(in-package :de.setf.cassandra)

;;;  This file is a constituent of the 'de.setf.cassandra' library component.
;;;  It defines an API in terms of reified keyspaces, and column families
;;;  (c) 2010 james anderson
;;;
;;;  'de.setf.cassandra' is free software: you can redistribute it and/or modify
;;;  it under the terms of the GNU Lesser General Public License as published by
;;;  the Free Software Foundation, either version 3 of the License, or
;;;  (at your option) any later version.
;;;
;;;  'de.setf.cassandra' is distributed in the hope that it will be useful,
;;;  but WITHOUT ANY WARRANTY; without even the implied warranty of
;;;  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
;;;  GNU Lesser General Public License for more details.
;;;
;;;  You should have received a copy of the GNU Lesser General Public License along
;;;  with 'de.setf.cassandra' as 'lgpl.txt'. If not, see the GNU [site](http://www.gnu.org/licenses/).

(defgeneric get (protocol-connection
                 &key keyspace-name key column-path consistency-level)
  (:documentation "See cassandra:get."))


(defgeneric get-slice (protocol-connection
                       &key keyspace-name key column-parent predicate consistency-level)
  (:documentation "See cassandra:get-slice."))


#+cassandra-thrift-2-1-0
(defgeneric multiget (protocol-connection
                      &key keyspace-name keys column-path consistency-level)
  (:documentation "See cassandra:multiget."))


(defgeneric multiget-slice (protocol-connection
                            &key keyspace-name keys column-parent predicate consistency-level)
  (:documentation "See cassandra:multiget-slice."))

#+cassandra-thrift-8
(defgeneric multiget-count (protocol-connection
                            &key keyspace-name keys column-parent predicate consistency-level)
  (:documentation "See cassandra:multiget-count."))


(defgeneric get-count (protocol-connection
                       &key keyspace-name key column-parent consistency-level)
  (:documentation "See cassandra:get-count."))


(defgeneric get-range-slice (protocol-connection
                              &key keyspace-name column-parent predicate
                              start-key finish-key row-count consistency-level)
  (:documentation "See cassandra:get-range-slice."))


(defgeneric get-range-slices (protocol-connection
                              &key keyspace-name column-parent predicate range consistency-level)
  (:documentation "See cassandra:get-range-slices."))


(defgeneric insert (protocol-connection
                    &key keyspace-name key column-path value timestamp consistency-level)
  (:documentation "See cassandra:insert."))


(defgeneric batch-insert (protocol-connection
                          &key keyspace-name key cfmap consistency-level)
  (:documentation "See cassandra:batch-insert."))


(defgeneric remove (protocol-connection
                    &key keyspace-name key column-path timestamp consistency-level)
  (:documentation "See cassandra:remove."))


(defgeneric batch-mutate (protocol-connection
                          &key keyspace-name mutation-map consistency-level)
  (:documentation "See cassandra:get-range-slices."))


(defgeneric get-string-property (protocol-connection
                                 &key property)
  (:documentation "See cassandra:get-string-property."))


(defgeneric get-string-list-property (protocol-connection
                                      &key property)
  (:documentation "See cassandra:get-string-list-property."))


(defgeneric describe-keyspaces (protocol-connection)
  (:documentation "See cassandra:describe-keyspaces."))


(defgeneric describe-cluster-name (protocol-connection)
  (:documentation "See cassandra:describe-cluster-name."))


(defgeneric describe-version (protocol-connection)
  (:documentation "See cassandra:describe-version."))


(defgeneric describe-ring (protocol-connection
                           &key keyspace-name)
  (:documentation "See cassandra:describe-ring."))


(defgeneric describe-keyspace (protocol-connection
                               &key keyspace-name)
  (:documentation "See cassandra:describe-keyspace."))


(defgeneric describe-splits (protocol-connection
                             &key start-token end-token key-per-split)
  (:documentation "See cassandra:describe-splits."))

#+cassandra-thrift-8
(defgeneric system-add-column-family (protocol-connection &key cf-def))

#+cassandra-thrift-8
(defgeneric system-drop-column-family (protocol-connection column-family))

#+cassandra-thrift-8
(defgeneric system-rename-column-family (protocol-connection &key old-name new-name))

#+cassandra-thrift-8
(defgeneric system-add-keyspace (protocol-connection ks-def))

#+cassandra-thrift-8
(defgeneric system-drop-keyspace (protocol-connection keyspace-name))

#+cassandra-thrift-8
(defgeneric system-rename-keyspace (protcol-connection old-name new-name))


                                

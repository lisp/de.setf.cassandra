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
                 &key key column-path consistency-level)
  (:documentation "See get."))


(defgeneric get-slice (protocol-connection
                       &key key column-parent predicate consistency-level)
  (:documentation "See get-slice."))


(defgeneric multiget (protocol-connection
                      &key keys column-path consistency-level)
  (:documentation "See cassandra:multiget."))


(defgeneric multiget-slice (protocol-connection
                            &key keys column-parent predicate consistency-level)
  (:documentation "See cassandra:multiget-slice."))


(defgeneric get-count (protocol-connection
                       &key key column-parent predicate consistency-level)
  (:documentation "See cassandra:get-count."))


(defgeneric multiget-count (protocol-connection
                            &key keys column-parent predicate consistency-level)
  (:documentation "See cassandra:multiget-count."))


(defgeneric get-range-slice (protocol-connection
                              &key column-parent predicate
                              start-key finish-key row-count consistency-level)
  (:documentation "See cassandra:get-range-slice."))


(defgeneric get-range-slices (protocol-connection
                              &key column-parent predicate range consistency-level)
  (:documentation "See cassandra:get-range-slices."))


(defgeneric scan (keyspace
                  &key column-parent row-predicate column-predicate consistency-level)
  (:documentation "See cassandra:scan"))


(defgeneric scan-count (keyspace
                  &key column-parent row-predicate column-predicate consistency-level)
  (:documentation "See cassandra:scan-count"))


(defgeneric insert (protocol-connection
                    &key key column-family super-column column value timestamp ttl clock consistency-level)
  (:documentation "See cassandra:insert."))


(defgeneric batch-insert (protocol-connection
                          &key key cfmap consistency-level)
  (:documentation "See cassandra:batch-insert."))


(defgeneric remove (protocol-connection
                    &key key column-path timestamp clock consistency-level)
  (:documentation "See cassandra:remove."))


(defgeneric batch-mutate (protocol-connection
                          &key mutation-map consistency-level)
  (:documentation "See cassandra:get-range-slices."))


(defgeneric truncate (protocol-connection
                      &key column-family)
  (:documentation "See cassandra:truncate."))


(defgeneric check-schema-agreement (protocol-connection)
  (:documentation "See cassandra:check-schema-agreement."))



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


(defgeneric describe-ring (protocol-connection)
  (:documentation "See cassandra:describe-ring."))


(defgeneric describe-keyspace (protocol-connection)
  (:documentation "See cassandra:describe-keyspace."))


(defgeneric system-add-column-family (protocol-connection &key cf-def)
  (:documentation "See cassandra:system-add-column-family"))
  

(defgeneric system-drop-column-family (protocol-connection column-family)
  (:documentation "See cassandra:system-drop-column-family"))


(defgeneric system-rename-column-family (protocol-connection &key old-name new-name)
  (:documentation "See cassandra:system-rename-column-family"))


(defgeneric system-add-keyspace (protocol-connection ks-def)
  (:documentation "See cassandra:system-add-keyspace"))


(defgeneric system-drop-keyspace (protocol-connection keyspace-name)
  (:documentation "See cassandra:system-drop-keyspace"))


(defgeneric system-rename-keyspace (protocol-connection old-name new-name)
  (:documentation "See cassandra:system-rename-keyspace"))

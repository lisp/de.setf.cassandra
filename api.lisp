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

;;; This, the generic cassandra API, covers the thrift API versions for both 0.6 and 0.7 cassandra versions
;;; It attempts to avoid the interface differences - the explicit keyspace name in 0.6, the change from
;;; timestamps to clocks in 8.*, and the disjoint parameter class hierachies between API versions
;;; by specializing on a versioned protocol argument, and either passing through other arguments separately
;;; to permit the version-specific implementation to construct the requisite structure argument.


(defparameter most-positive-i32 (1- (expt 2 31)))

(defgeneric get (protocol-connection
                 &key key column-family column consistency-level)
  (:documentation "See cassandra:get."))


(defgeneric get-slice (protocol-connection
                       &key key column-family super-column start finish column-names reversed count
                       consistency-level)
  (:documentation "See cassandra:get-slice."))

(defgeneric map-slice (operator protocol-connection
                       &key key column-family super-column start finish column-names reversed count
                       consistency-level)
  (:documentation "The mapped version of get-slice."))


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
                              &key key start-key finish-key column-family super-column start finish count column-names reversed
                              consistency-level)
  (:documentation "See cassandra:get-range-slices."))


(defgeneric map-range-slices (operator protocol-connection
                              &key key start-key finish-key column-family super-column start finish count column-names reversed
                              consistency-level)
  (:documentation "The mapped version of cassandra:get-range-slices."))


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
                    &key key column-family super-column column timestamp clock consistency-level)
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


;;;
;;; coercing accessors

(defgeneric binary (object)
  (:method ((object string))
    (trivial-utf-8:string-to-utf-8-bytes object))
  (:method ((object symbol))
    (trivial-utf-8:string-to-utf-8-bytes (symbol-name object)))
  (:method ((object number))
    (map 'vector #'char-code (princ-to-string object))))


(macrolet ((def-binary-writer (name &key (types '(string symbol))
                                    (packages '(:cassandra_8.3.0 :cassandra_2.1.0)))
             `(progn
                ,@(loop for package in packages
                        append (loop for type in types
                                     collect
                                     `(defmethod (setf ,(or (find-symbol (string name) package)
                                                            (error "missing ~a:~a." package name)))
                                                 ((value ,type) instance)
                                        (setf (,(find-symbol (string name) package) instance)
                                              (binary value))))))))
  (def-binary-writer column-name)
  (def-binary-writer column-value :types (string symbol number))
  (def-binary-writer columnpath-column)
  (def-binary-writer supercolumn-name)
  (def-binary-writer columnparent-super-column)
  (def-binary-writer columnpath-super-column)
  (def-binary-writer columnpath-column)
  (def-binary-writer slicerange-start)
  (def-binary-writer slicerange-finish)
  (def-binary-writer deletion-super-column)
  ;; present in 8.3.0 only
  (def-binary-writer indexexpression-column-name :packages (cassandra_8.3.0))
  (def-binary-writer indexexpression-value :packages (cassandra_8.3.0))
  (def-binary-writer indexclause-start-key :packages (cassandra_8.3.0))
  (def-binary-writer keycount-key :packages (cassandra_8.3.0))
  ;; binary in 8.3.0, string in 2.1.0
  (def-binary-writer keyrange-start-key :packages (cassandra_8.3.0))
  (def-binary-writer keyrange-end-key :packages (cassandra_8.3.0))
  (def-binary-writer keyslice-key :packages (cassandra_8.3.0))
  (def-binary-writer columndef-name :packages (cassandra_8.3.0)))



(defgeneric column-value (column)
  (:method ((column cassandra_2.1.0:column))
    (cassandra_2.1.0:column-value column))
  (:method ((column cassandra_8.3.0:column))
    (cassandra_8.3.0:column-value column)))

(defgeneric column-value-string (column)
  (:method ((column cassandra_2.1.0:column))
    (trivial-utf-8:utf-8-bytes-to-string (cassandra_2.1.0:column-value column)))
  (:method ((column cassandra_8.3.0:column))
    (trivial-utf-8:utf-8-bytes-to-string (cassandra_8.3.0:column-value column))))

(defgeneric column-name (column)
  (:method ((column cassandra_2.1.0:column))
    (cassandra_2.1.0:column-name column))
  (:method ((column cassandra_8.3.0:column))
    (cassandra_8.3.0:column-name column)))

(defgeneric column-name-string (column)
  (:method ((column cassandra_2.1.0:column))
    (trivial-utf-8:utf-8-bytes-to-string (cassandra_2.1.0:column-name column)))
  (:method ((column cassandra_8.3.0:column))
    (trivial-utf-8:utf-8-bytes-to-string (cassandra_8.3.0:column-name column))))

(defgeneric columnorsupercolumn-column (column-or-super-column)
  (:method ((cosc null))
    ;; allow for null result from a retrieval
    nil)
  (:method ((cosc cassandra_2.1.0:columnorsupercolumn))
    (when (slot-boundp cosc 'cassandra_2.1.0::column)
      (cassandra_2.1.0:columnorsupercolumn-column cosc)))
  (:method ((cosc cassandra_8.3.0:columnorsupercolumn))
    (when (slot-boundp cosc 'cassandra_8.3.0::column)
      (cassandra_8.3.0:columnorsupercolumn-column cosc))))

(defgeneric columnorsupercolumn-super-column (column-or-super-column)
  (:method ((cosc null))
    ;; allow for null result from a retrieval
    nil)
  (:method ((cosc cassandra_2.1.0:columnorsupercolumn))
    (when (slot-boundp cosc 'cassandra_2.1.0::super-column)
      (cassandra_2.1.0:columnorsupercolumn-super-column cosc)))
  (:method ((cosc cassandra_8.3.0:columnorsupercolumn))
    (when (slot-boundp cosc 'cassandra_8.3.0::super-column)
      (cassandra_8.3.0:columnorsupercolumn-super-column cosc))))

(defgeneric keyslice-key (key-slice)
  (:method ((key-slice cassandra_2.1.0:keyslice))
    (cassandra_2.1.0:keyslice-key key-slice))
  (:method ((key-slice cassandra_8.3.0:keyslice))
    (cassandra_8.3.0:keyslice-key key-slice)))

(defgeneric keyslice-columns (key-slice)
  (:method ((key-slice cassandra_2.1.0:keyslice))
    (cassandra_2.1.0:keyslice-columns key-slice))
  (:method ((key-slice cassandra_8.3.0:keyslice))
    (cassandra_8.3.0:keyslice-columns key-slice)))

(defgeneric supercolumn-name (super-column)
  (:method ((super-column cassandra_2.1.0:supercolumn))
    (cassandra_2.1.0:supercolumn-name super-column))
  (:method ((super-column cassandra_8.3.0:supercolumn))
    (cassandra_8.3.0:supercolumn-name super-column)))

(defgeneric supercolumn-columns (super-column)
  (:method ((super-column cassandra_2.1.0:supercolumn))
    (cassandra_2.1.0:supercolumn-columns super-column))
  (:method ((super-column cassandra_8.3.0:supercolumn))
    (cassandra_8.3.0:supercolumn-columns super-column)))
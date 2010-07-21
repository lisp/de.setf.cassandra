;;; -*- Mode: lisp; Syntax: ansi-common-lisp; Base: 10; Package: de.setf.cassandra; -*-

(in-package :de.setf.cassandra)

;;;  This file defines keyspace operators for the 'de.setf.cassandra' library component.
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


(defclass keyspace (thrift:binary-protocol)
  ((slice-size
     :initarg :slice-size :initform 100
     :accessor keyspace-slice-size
     :type i32)
   (consistency-level
     :initarg :consitency-level :initform cassandra:consistency-level.one
     :type (enum cassandra:consistency-level)
     :accessor keyspace-consistency-level)
   (name
    :initarg :name :initform nil
    :accessor keyspace-name))

  (:documentation "A thrift protocol to a cassandra instance for a particular keyspace.
 Provides cached state and defaults for 
 * name : a string designating a keyspace
 ;;; break this into individual option slots
 * consistency-level : the default level for operations
 * siice-size : the default slice size for retrieveal operations

 Acts as the top-level interface to the cassandra API. It implements operators which
 accept arguments as keywords, provides defaults for the keyspace, consistency level, and
 timestamps, and delegates to the thrift-based positional request operators.

 In addition to the standard interface, it provides composite opertors:
    insert-data

 The current interface corresponds to the 2.1.0 thrift version, one cassandra 0.7 appears with
 8.*.* interfaces, some form of conditionalization will be required."))


(defgeneric keyspace (location &key protocol direction element-type &allow-other-keys)
  (:documentation "Open a client connection to a cassandra thrift service.")

  (:method ((location t) &rest args &key (protocol 'keyspace) &allow-other-keys)
    (declare (dynamic-extent args))
    (print args)
    (apply #'thrift:client location :protocol protocol args)))


;;;

(defmethod get ((keyspace keyspace) &key
                (keyspace-name (keyspace-name keyspace))
                key column-path
                (consistency-level (keyspace-consistency-level keyspace)))
  (cassandra:get keyspace keyspace-name key column-path consistency-level))


(defmethod get-slice ((keyspace keyspace) &key
                      (keyspace-name (keyspace-name keyspace))
                      key column-parent predicate
                      (consistency-level (keyspace-consistency-level keyspace)))
  (cassandra:get-slice keyspace keyspace-name key column-parent predicate consistency-level))


#+cassandra-thrift-2-1-0
(defmethod multiget ((keyspace keyspace) &key
                     (keyspace-name (keyspace-name keyspace))
                     keys column-path
                     (consistency-level (keyspace-consistency-level keyspace)))
  (cassandra:multiget keyspace keyspace-name keys column-path consistency-level))


(defmethod multiget-slice ((keyspace keyspace) &key
                           (keyspace-name (keyspace-name keyspace))
                           keys column-parent predicate
                           (consistency-level (keyspace-consistency-level keyspace)))
  (cassandra:multiget-slice keyspace keyspace-name keys column-parent predicate consistency-level))

#+cassandra-thrift-8
(defmethod multiget-count ((keyspace keyspace) &key
                           (keyspace-name (keyspace-name keyspace))
                           keys column-parent predicate
                           (consistency-level (keyspace-consistency-level keyspace)))
  (cassandra:multiget-count keyspace keyspace-name keys column-parent predicate consistency-level))


(defmethod get-count ((keyspace keyspace) &key
                      (keyspace-name (keyspace-name keyspace))
                      key column-parent
                      (consistency-level (keyspace-consistency-level keyspace)))
  (cassandra:get-count keyspace keyspace-name key column-parent consistency-level))


(defmethod get-range-slice ((keyspace keyspace) &key
                            (keyspace-name (keyspace-name keyspace))
                            column-parent predicate start-key finish-key row-count
                            (consistency-level (keyspace-consistency-level keyspace)))
  (cassandra:get-range-slice keyspace keyspace-name column-parent predicate
                             start-key finish-key row-count  consistency-level))


(defmethod get-range-slices ((keyspace keyspace) &key
                             (keyspace-name (keyspace-name keyspace))
                             column-parent predicate range
                             (consistency-level (keyspace-consistency-level keyspace)))
  (cassandra:get-range-slices keyspace keyspace-name column-parent predicate range consistency-level))


(defmethod insert ((keyspace keyspace) &key
                   (keyspace-name (keyspace-name keyspace))
                   key column-path value (timestamp (uuid::get-timestamp)) 
                   (consistency-level (keyspace-consistency-level keyspace)))
  (cassandra:insert keyspace keyspace-name key column-path value timestamp consistency-level))


(defmethod batch-insert ((keyspace keyspace) &key
                         (keyspace-name (keyspace-name keyspace))
                         key cfmap
                         (consistency-level (keyspace-consistency-level keyspace)))
  (cassandra:batch-insert keyspace keyspace-name key cfmap consistency-level))


(defmethod remove ((keyspace keyspace) &key
                   (keyspace-name (keyspace-name keyspace))
                   key column-path (timestamp (uuid::get-timestamp))
                   (consistency-level (keyspace-consistency-level keyspace)))
  (cassandra:remove keyspace keyspace-name key column-path timestamp consistency-level))


(defmethod batch-mutate ((keyspace keyspace) &key
                         (keyspace-name (keyspace-name keyspace))
                         mutation-map
                         (consistency-level (keyspace-consistency-level keyspace)))
  (cassandra:batch-mutate keyspace keyspace-name mutation-map consistency-level))


(defmethod get-string-property ((keyspace keyspace) &key property)
  (cassandra:get-string-property keyspace property))


(defmethod get-string-list-property ((keyspace keyspace) &key property)
  (cassandra:get-string-list-property keyspace property))


(defmethod describe-keyspaces ((keyspace keyspace))
  (cassandra:describe-keyspaces keyspace))


(defmethod describe-cluster-name ((keyspace keyspace))
  (cassandra:describe-cluster-name keyspace))


(defmethod describe-version ((keyspace keyspace))
  (cassandra:describe-version keyspace))


(defmethod describe-ring ((keyspace keyspace) &key (keyspace-name (keyspace-name keyspace)))
  (cassandra:describe-ring keyspace keyspace-name))


(defmethod describe-keyspace ((keyspace keyspace) &key (keyspace-name (keyspace-name keyspace)))
  (cassandra:describe-keyspace keyspace keyspace-name))


(defmethod describe-splits ((keyspace keyspace) &key start-token end-token key-per-split)
  (cassandra:describe-splits keyspace start-token end-token key-per-split))


;;;
;;; additional operators

(defmethod insert-data ((keyspace keyspace) data &key (timestamp (uuid::get-timestamp)))
  data timestamp)





                                

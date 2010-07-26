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
  ((name
    :initarg :name :initform (error "name is required.")
    :accessor keyspace-name
    :documentation "Specifies the keyspace in the store.")
   (slice-size
    :initarg :slice-size :initform 100
    :accessor keyspace-slice-size
    :type i32
    :documentation "The default slice size to use for retrieval operations which return a list of values.")
   (consistency-level
    :initarg :consitency-level :initform cassandra-2-1-0:consistency-level.one
    :type (enum cassandra-2-1-0:consistency-level)
    :accessor keyspace-consistency-level
    :documentation "The default cassandra consistency level to use for store operations.
 The default value is cassandra-2-1-0:consistency-level.one.")
   (description
    :initform nil :initarg :description
    :type list
    :reader get-keyspace-description :writer setf-keyspace-description
    :documentation "Caches the result of describe-keyspace.")
   (clock
    :initform (cassandra-8-3-0:make-clock :timestamp (uuid::get-timestamp))
    :reader get-keyspace-clock
    :documentation "Stub support for whatever vector clocks turn into. Usage is round-about for
     0.6, as that uses just the timestamp, but permits a consistent API.")
   (version
    :initform nil :initarg :version
    :reader keyspace-version)
   (version-map
    :initform '(("0.6.4" . keyspace-2-1-0)
                ("0.7.0" . keyspace-8-3-0))
    :allocation :class
    :reader keyspace-version-map))

  (:documentation "A keyspace represents thrift protocol connection to a cassandra instance for access to 
 a particular keyspace. It provides cached state and defaults for 
 * name : a string designating a keyspace
 * consistency-level : the default level for operations
 * siice-size : the default slice size for retrieveal operations

 Acts as the top-level interface to the cassandra API. It implements operators which
 accept arguments as keywords, provides defaults for the keyspace, consistency level, and
 timestamps, and delegates to the thrift-based positional request operators.

 In addition to the standard interface, it provides composite operators:
    insert-data

 The class abstract keyspace class is specialized as per the thruft api versions for access to the respective
 service versions:
 * keyspace-2-1-0 : 0.6 
 * keyspace-8-3-0 : 0.7"))

(defclass keyspace-2-1-0 (keyspace)
  ((version :initform "0.6.4"))
  (:documentation "The keyspace protocol class for cassandra 0.6.3 w/api 2.1.0"))

(defclass keyspace-8-3-0 (keyspace)
  ((version :initform "0.7.0"))
  (:documentation "The keyspace protocol class for cassandra 0.7.0 w/api 8.3.0"))




(defgeneric keyspace (location &key protocol direction element-type &allow-other-keys)
  (:documentation "Open a client connection to a cassandra thrift service and instantiate a keyspace
 and bind it to a named keyspace in the store.")

  (:method ((location t) &rest args &key (protocol 'keyspace) &allow-other-keys)
    (declare (dynamic-extent args))
    (apply #'thrift:client location :protocol protocol args)))

(defmethod initialize-instance :after ((instance keyspace) &key)
  (let ((version (describe-version instance)))
    (unless (equal version (keyspace-version instance))
      (let ((new-class (rest (assoc version (keyspace-version-map instance)))))
        (cond ((null new-class)
               (error "Service version not supported: ~s. Expected one of: ~s."
                      version (mapcar #'first (keyspace-version-map instance))))
              ((eq new-class (type-of instance)))
              ((subtypep new-class (type-of instance))
               (change-class instance new-class))
              (t
               (error "Version requires an invalid implementation: ~s; ~s."
                      version new-class)))))))

(defmethod initialize-instance :after ((instance keyspace-8-3-0) &key)
  (cassandra-8-3-0:set-keyspace instance (keyspace-name instance)))
    
(defgeneric keyspace-clock (keyspace &optional timestamp)
  (:documentation "Return the keyspace clock with an updated timestamp.")
  (:method ((keyspace keyspace) &optional timestamp)
    (let ((clock (get-keyspace-clock keyspace)))
      (setf (cassandra-8-3-0:clock-timestamp clock) (or timestamp (uuid::get-timestamp)))
      clock)))


;;;
;;; utility-operators

(defgeneric keyspace-description (keyspace)
  (:method ((keyspace keyspace))
    (or (get-keyspace-description keyspace)
        (setf-keyspace-description (describe-keyspace keyspace) keyspace))))



;;;
;;; interface operators

;;; get

(defmethod get ((keyspace keyspace-2-1-0) &key
                key column-path
                (consistency-level (keyspace-consistency-level keyspace)))
  (cassandra-2-1-0:get keyspace (keyspace-name keyspace) key column-path consistency-level))

(defmethod get ((keyspace keyspace-8-3-0) &key
                key column-path
                (consistency-level (keyspace-consistency-level keyspace)))
  (cassandra-8-3-0:get keyspace key column-path consistency-level))


;;; get_slice

(defmethod get-slice ((keyspace keyspace-2-1-0) &key
                      key column-parent predicate
                      (consistency-level (keyspace-consistency-level keyspace)))
  (cassandra-2-1-0:get-slice keyspace (keyspace-name keyspace) key column-parent predicate consistency-level))

(defmethod get-slice ((keyspace keyspace-8-3-0) &key
                      key column-parent predicate
                      (consistency-level (keyspace-consistency-level keyspace)))
  (cassandra-8-3-0:get-slice keyspace key column-parent predicate consistency-level))


;;; multiget: 0.6 only

(defmethod multiget ((keyspace keyspace-2-1-0) &key
                     keys column-path
                     (consistency-level (keyspace-consistency-level keyspace)))
  (cassandra-2-1-0:multiget keyspace (keyspace-name keyspace) keys column-path consistency-level))


;;; multiget_slice

(defmethod multiget-slice ((keyspace keyspace-2-1-0) &key
                           keys column-parent predicate
                           (consistency-level (keyspace-consistency-level keyspace)))
  (cassandra-2-1-0:multiget-slice keyspace (keyspace-name keyspace) keys column-parent predicate consistency-level))

(defmethod multiget-slice ((keyspace keyspace-8-3-0) &key
                           keys column-parent predicate
                           (consistency-level (keyspace-consistency-level keyspace)))
  (cassandra-8-3-0:multiget-slice keyspace keys column-parent predicate consistency-level))


;;; get_count

(defmethod get-count ((keyspace keyspace-2-1-0) &key
                      key column-parent predicate
                      (consistency-level (keyspace-consistency-level keyspace)))
  (when predicate (warn "Predicate is ignored: ~a; ~a" keyspace predicate))
  (cassandra-2-1-0:get-count keyspace (keyspace-name keyspace) key column-parent consistency-level))

(defmethod get-count ((keyspace keyspace-8-3-0) &key
                      key column-parent predicate
                      (consistency-level (keyspace-consistency-level keyspace)))
  (cassandra-8-3-0:get-count keyspace key column-parent predicate consistency-level))


;;; multiget_count : >= 0.7, ? but with a keyspace name ?

(defmethod multiget-count ((keyspace keyspace-8-3-0) &key
                           keys column-parent predicate
                           (consistency-level (keyspace-consistency-level keyspace)))
  (cassandra-8-3-0:multiget-count keyspace (keyspace-name keyspace) keys column-parent predicate consistency-level))


;;; get_range_slice : <= 0.6

(defmethod get-range-slice ((keyspace keyspace-2-1-0) &key
                            column-parent predicate start-key finish-key row-count
                            (consistency-level (keyspace-consistency-level keyspace)))
  (cassandra-2-1-0:get-range-slice keyspace (keyspace-name keyspace) column-parent predicate
                             start-key finish-key row-count  consistency-level))


;;; get_range_slices

(defmethod get-range-slices ((keyspace keyspace-2-1-0) &key
                             column-parent predicate range
                             (consistency-level (keyspace-consistency-level keyspace)))
  (cassandra-2-1-0:get-range-slices keyspace (keyspace-name keyspace) column-parent predicate range consistency-level))

(defmethod get-range-slices ((keyspace keyspace-8-3-0) &key
                             column-parent predicate range
                             (consistency-level (keyspace-consistency-level keyspace)))
  (cassandra-8-3-0:get-range-slices keyspace column-parent predicate range consistency-level))


;;; scan

(defmethod scan ((keyspace keyspace-8-3-0) &key
                 column-parent row-predicate column-predicate
                 (consistency-level (keyspace-consistency-level keyspace)))
  (cassandra-8-3-0:scan keyspace column-parent row-predicate column-predicate consistency-level))


;;; scan-count : >= 0.7

(defmethod scan-count ((keyspace keyspace-8-3-0) &key
                 column-parent row-predicate column-predicate
                 (consistency-level (keyspace-consistency-level keyspace)))
  (cassandra-8-3-0:scan-count keyspace column-parent row-predicate column-predicate consistency-level))


;;; insert

(defmethod insert ((keyspace keyspace-2-1-0) &key
                   key column-family super-column column value ttl
                   timestamp (clock (keyspace-clock keyspace timestamp))
                   (consistency-level (keyspace-consistency-level keyspace)))
  (when ttl (warn "TTL is ignored: ~a; ~a" keyspace ttl))
  (let ((column-path (cassandra-2-1-0:make-columnpath :column-family column-family
                                                      :super-column super-column
                                                      :column column)))
    (cassandra-2-1-0:insert keyspace (keyspace-name keyspace) key column-path value (cassandra-8-3-0:clock-timestamp clock) consistency-level)))

(defmethod insert ((keyspace keyspace-8-3-0) &key
                   key column-family super-column column value ttl
                   timestamp (clock (keyspace-clock keyspace timestamp))
                   (consistency-level (keyspace-consistency-level keyspace)))
  (let ((column-parent (cassandra-8-3-0:make-columnparent :column-family column-family
                                                      :super-column super-column))
        (column (cassandra-8-3-0:make-column  :name column :value value :clock clock)))
    (when ttl (setf (cassandra-8-3-0:column-ttl column) ttl))
    (cassandra-8-3-0:insert keyspace key column-parent column consistency-level)))


;;; batch_insert : <= 0.6 

(defmethod batch-insert ((keyspace keyspace-2-1-0) &key
                         key cfmap
                         (consistency-level (keyspace-consistency-level keyspace)))
  (cassandra-2-1-0:batch-insert keyspace (keyspace-name keyspace) key cfmap consistency-level))


;;; remove

(defmethod remove ((keyspace keyspace-2-1-0) &key
                   key column-path
                   timestamp (clock (keyspace-clock keyspace timestamp))
                   (consistency-level (keyspace-consistency-level keyspace)))
  (cassandra-2-1-0:remove keyspace (keyspace-name keyspace) key column-path (cassandra-8-3-0:clock-timestamp clock) consistency-level))

(defmethod remove ((keyspace keyspace-8-3-0) &key
                   key column-path
                   timestamp (clock (keyspace-clock keyspace timestamp))
                   (consistency-level (keyspace-consistency-level keyspace)))
  (cassandra-8-3-0:remove keyspace key column-path clock consistency-level))



;;; batch_mutate

(defmethod batch-mutate ((keyspace keyspace-2-1-0) &key
                         mutation-map
                         (consistency-level (keyspace-consistency-level keyspace)))
  (cassandra-2-1-0:batch-mutate keyspace (keyspace-name keyspace) mutation-map consistency-level))

(defmethod batch-mutate ((keyspace keyspace-8-3-0) &key
                         mutation-map
                         (consistency-level (keyspace-consistency-level keyspace)))
  (cassandra-8-3-0:batch-mutate keyspace mutation-map consistency-level))


;;; truncate : >= 0.7

(defmethod truncate ((keyspace keyspace-8-3-0) &key
                     column-family)
  (cassandra-8-3-0:truncate keyspace column-family))


;;; check_schema_agreement : >= 0.7

(defmethod check-schema-agreement ((keyspace keyspace-8-3-0))
  (cassandra-8-3-0:check-schema-agreement keyspace))


;;; get_string_property : <= 0.6

(defmethod get-string-property ((keyspace keyspace-2-1-0) &key property)
  (cassandra-2-1-0:get-string-property keyspace property))


;;; get_string_list_property : <= 0.6

(defmethod get-string-list-property ((keyspace keyspace-2-1-0) &key property)
  (cassandra-2-1-0:get-string-list-property keyspace property))


;;; describe_keyspaces

(defmethod describe-keyspaces ((keyspace keyspace-2-1-0))
  (cassandra-2-1-0:describe-keyspaces keyspace))

(defmethod describe-keyspaces ((keyspace keyspace-8-3-0))
  (cassandra-8-3-0:describe-keyspaces keyspace))


;;; describe_cluster_name

(defmethod describe-cluster-name ((keyspace keyspace-2-1-0))
  (cassandra-2-1-0:describe-cluster-name keyspace))

(defmethod describe-cluster-name ((keyspace keyspace-8-3-0))
  (cassandra-8-3-0:describe-cluster-name keyspace))


;;; describe_version

(defmethod describe-version ((keyspace keyspace-2-1-0))
  (cassandra-2-1-0:describe-version keyspace))

(defmethod describe-version ((keyspace keyspace-8-3-0))
  (cassandra-8-3-0:describe-version keyspace))


;;; describe_ring

(defmethod describe-ring ((keyspace keyspace-2-1-0))
  (cassandra-2-1-0:describe-ring keyspace (keyspace-name keyspace)))

(defmethod describe-ring ((keyspace keyspace-8-3-0))
  (cassandra-8-3-0:describe-ring keyspace (keyspace-name keyspace)))


;;; describe_keyspace

(defmethod describe-keyspace ((keyspace keyspace-2-1-0))
  (cassandra-2-1-0:describe-keyspace keyspace (keyspace-name keyspace)))

(defmethod describe-keyspace ((keyspace keyspace-8-3-0))
  (cassandra-8-3-0:describe-keyspace keyspace (keyspace-name keyspace)))


(defmethod system-add-column-family ((keyspace keyspace-8-3-0) &key cf-def)
  (cassandra-8-3-0:system-add-column-family keyspace cf-def))


(defmethod system-drop-column-family ((keyspace keyspace-8-3-0) column-family)
  (cassandra-8-3-0:system-drop-column-family keyspace column-family))


(defmethod system-rename-column-family ((keyspace keyspace-8-3-0) &key old-name new-name)
  (cassandra-8-3-0:system-rename-column-family keyspace old-name new-name))


(defmethod system-add-keyspace ((keyspace keyspace-8-3-0) ks-def)
  (cassandra-8-3-0:system-add-keyspace keyspace ks-def))


(defmethod system-drop-keyspace ((keyspace keyspace-8-3-0) keyspace-name)
  (cassandra-8-3-0:system-drop-keyspace keyspace keyspace-name))


(defmethod system-rename-keyspace ((keyspace keyspace-8-3-0) old-name new-name)
  (cassandra-8-3-0:system-rename-keyspace keyspace old-name new-name))

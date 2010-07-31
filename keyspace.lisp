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
    :initarg :consitency-level :initform cassandra_2.1.0:consistency-level.one
    :type (enum cassandra_2.1.0:consistency-level)
    :accessor keyspace-consistency-level
    :documentation "The default cassandra consistency level to use for store operations.
     The default value is cassandra_2.1.0:consistency-level.one.")
   (description
    :initform nil :initarg :description
    :type list
    :reader get-keyspace-description :writer setf-keyspace-description
    :documentation "Caches a list of descriptions of keyspaces present in the store.")
   (keyspaces
    :initform nil :initarg :keyspaces
    :type list
    :reader get-keyspace-keyspaces :writer setf-keyspace-keyspaces
    :documentation "Caches a list of keyspaces present in the store.")
   (clock
    :initform (cassandra_8.3.0:make-clock :timestamp (uuid::get-timestamp))
    :allocation :class
    :reader get-keyspace-clock
    :documentation "Stub support for whatever vector clocks turn into. Usage is round-about for
     0.6, as that uses just the timestamp, but permits a consistent API.")
   (version
    :initform nil :initarg :version
    :reader keyspace-version)
   (version-class-map
    :initform '(("2.1.0" . cassandra_2.1.0:keyspace)
                ("8.3.0" . cassandra_8.3.0:keyspace))
    :allocation :class
    :reader keyspace-version-class-map
    :documentation "Maps the _protocol_ version to the class which supports it. This implies the server version as
     as 2.* is .6 and 8.* is .7, but the server does not report it actual version."))

  (:documentation "A keyspace represents thrift protocol connection to a cassandra instance for access to 
 a particular keyspace. It provides cached state and defaults for 
 * name : a string designating a keyspace
 * consistency-level : the default level for operations
 * siice-size : the default slice size for retrieveal operations

 Acts as the top-level interface to the cassandra API. It implements operators which
 accept arguments as keywords, provides defaults for the keyspace, c(:mutation .nsistency level, and
 timestamps, and delegates to the thrift-based positional request operators.

 In addition to the standard interface, it provides composite operators:
    insert-data

 The class abstract keyspace class is specialized as per the thruft api versions for access to the respective
 service versions:
 * cassandra_2.1.0:keyspace : 0.6 
 * cassandra_8.3.0:keyspace : 0.7"))

(defun compute-versioned-constructor (type package)
  (or (find-symbol (concatenate 'string (string :make-) (string type)) package)
      (error "invalid struct-name: ~s." type)))



(defclass cassandra_2.1.0:keyspace (keyspace)
  ((version :initform "0.6.4" :allocation :class))
  (:documentation "The keyspace protocol class for cassandra 0.6.4 w/api 2.1.0"))

(defclass cassandra_8.3.0:keyspace (keyspace)
  ((version :initform "0.7.0" :allocation :class))
  (:documentation "The keyspace protocol class for cassandra 0.7.0 w/api 8.3.0"))



(defgeneric keyspace (location &key protocol direction element-type &allow-other-keys)
  (:documentation "Open a client connection to a cassandra thrift service and instantiate a keyspace
 and bind it to a named keyspace in the store.")

  (:method ((location t) &rest args &key (protocol 'keyspace) &allow-other-keys)
    (declare (dynamic-extent args))
    (apply #'thrift:client location :protocol protocol args)))

(defmethod initialize-instance :after ((instance keyspace) &rest initargs)
  (let ((concrete-class (compute-keyspace-class instance)))
    (cond ((typep instance concrete-class))
          ((subtypep concrete-class (type-of instance))
           (change-class instance concrete-class))
          (t
           (error "Service requires an incompatible implementation: ~s, ~s."
                  concrete-class (type-of instance)))))
  ;; once the effective class is set
  (apply #'keyspace-bind-columns instance initargs))

(defmethod initialize-instance :after ((instance cassandra_8.3.0:keyspace) &key)
  (cassandra_8.3.0:set-keyspace instance (keyspace-name instance)))
    
(defgeneric keyspace-clock (keyspace &optional timestamp)
  (:documentation "Return the keyspace clock with an updated timestamp.")
  (:method ((keyspace keyspace) &optional timestamp)
    (let ((clock (get-keyspace-clock keyspace)))
      (setf (cassandra_8.3.0:clock-timestamp clock) (or timestamp (uuid::get-timestamp)))
      clock)))


(defgeneric compute-keyspace-class (keyspace)
  (:method ((keyspace keyspace))
    "The base method is based on version only."
    (let* ((service-version (describe-version keyspace))
           (new-class (rest (assoc service-version (keyspace-version-class-map keyspace) :test #'equal))))
      (unless new-class
        (error "Service version not supported: ~s. Expected one of: ~s."
               service-version (mapcar #'first (keyspace-version-class-map keyspace))))
      new-class)))

(defgeneric keyspace-bind-columns (keyspace &key &allow-other-keys)
  (:method ((keyspace keyspace) &key &allow-other-keys) ))

;;;
;;; utility-operators

;;; these are invoked latest in the :after initialize instance to obtain information for
;;; adjusting the class to match the keyspaces present in the store.

(defgeneric keyspace-description (keyspace)
  (:method ((keyspace keyspace))
    (or (get-keyspace-description keyspace)
        (setf-keyspace-description (describe-keyspace keyspace) keyspace))))

(defgeneric keyspace-keyspaces (keyspace)
  (:method ((keyspace keyspace))
    (or (get-keyspace-keyspaces keyspace)
        (setf-keyspace-keyspaces (describe-keyspaces keyspace) keyspace))))


(defgeneric set-keyspace-column-family (keyspace column-family slot-name &key class required)
  (:method ((keyspace keyspace) (cf-name string) slot-name &key (class 'standard-column-family) required)
    (let ((cf-description (rest (assoc cf-name (keyspace-description keyspace) :test #'string-equal))))
      (if cf-description
        (if (string-equal (rest (assoc "Type" cf-description :test #'string-equal))
                          (ecase class
                            (standard-column-family "Standard")
                            (super-column-family "Super")))
          (setf (slot-value keyspace slot-name) (make-instance class :name cf-name :keyspace keyspace))
          (error "Column family class does not match: ~s; ~s; ~s."
                 cf-name class (rest (assoc "Type" cf-description :test #'string-equal))))
        (when required
          (error "The column family is required: ~s." cf-name)))))

  (:method ((keyspace keyspace) (cf null) slot-name &key class (required t))
    (declare (ignore class))
    (when required
      (error "The column family is required: ~s." slot-name))))



;;;
;;; interface operators

;;; get

(defmethod get ((keyspace cassandra_2.1.0:keyspace) &key
                column-family key super-column column
                (consistency-level (keyspace-consistency-level keyspace)))
  (let ((column-path (cassandra_2.1.0:make-columnpath :column-family column-family
                                                      :column column)))
    (when super-column
      (setf (cassandra_2.1.0:columnpath-super-column column-path) super-column))
    (cassandra_2.1.0:get keyspace (keyspace-name keyspace) key column-path consistency-level)))

(defmethod get ((keyspace cassandra_8.3.0:keyspace) &key
                column-family key super-column column
                (consistency-level (keyspace-consistency-level keyspace)))
  (let ((column-path (cassandra_8.3.0:make-columnpath :column-family column-family
                                                      :column column)))
    (when super-column
      (setf (cassandra_8.3.0:columnpath-super-column column-path) super-column))
    (cassandra_8.3.0:get keyspace key column-path consistency-level)))


;;; get_slice

(defmethod get-slice ((keyspace cassandra_2.1.0:keyspace) &key
                      key column-family super-column (start "") (finish "") (column-names nil cn-s) reversed count
                      (consistency-level (keyspace-consistency-level keyspace)))
  (let ((column-parent (cassandra_2.1.0:make-columnparent :column-family column-family))
        (slice-predicate (cassandra_2.1.0:make-slicepredicate))
        (slice-range nil))
    (when super-column
      (setf (cassandra_2.1.0:columnparent-super-column column-parent) super-column))
    (if cn-s
      (setf (cassandra_2.1.0:slicepredicate-column-names slice-predicate) (mapcar #'string column-names))
      (setf slice-range (cassandra_2.1.0:make-slicerange :reversed reversed :count count :start start :finish finish)
            (cassandra_2.1.0:slicepredicate-slice-range slice-predicate) slice-range))
    (cassandra_2.1.0:get-slice keyspace (keyspace-name keyspace) key column-parent slice-predicate consistency-level)))


(defmethod map-slice (op (keyspace cassandra_2.1.0:keyspace) &key
                      key column-family super-column (start "") (finish "") (column-names nil cn-s) reversed count
                      (consistency-level (keyspace-consistency-level keyspace)))
  (let ((column-parent (cassandra_2.1.0:make-columnparent :column-family column-family))
        (slice-predicate (cassandra_2.1.0:make-slicepredicate))
        (slice-range nil))
    (when super-column
      (setf (cassandra_2.1.0:columnparent-super-column column-parent) super-column))
    (cond (cn-s
           (setf (cassandra_2.1.0:slicepredicate-column-names slice-predicate) (mapcar #'string column-names))
           (map nil op
                (cassandra_2.1.0:get-slice keyspace (keyspace-name keyspace) key column-parent slice-predicate consistency-level)))
          (t
           (setf slice-range (cassandra_2.1.0:make-slicerange :reversed reversed :count count :start start :finish finish))
           (setf (cassandra_2.1.0:slicepredicate-slice-range slice-predicate) slice-range)
           (let ((last-column nil))
             (loop (let ((slice (cassandra_2.1.0:get-slice keyspace (keyspace-name keyspace) key column-parent slice-predicate consistency-level)))
                     (when last-column (pop slice))
                     (unless slice (return))
                     (loop (unless slice (return))
                           (when (minusp (decf count)) (return))
                           (setf last-column (cassandra_2.1.0:columnorsupercolumn-column (pop slice)))
                           (funcall op last-column))
                       (setf (cassandra_2.1.0:slicerange-start slice-range)
                             (cassandra_2.1.0:column-name last-column)))))))))


(defmethod get-slice ((keyspace cassandra_8.3.0:keyspace) &key
                      key column-family super-column (start "") (finish "") (column-names nil cn-s) reversed count
                      (consistency-level (keyspace-consistency-level keyspace)))
  (let ((column-parent (cassandra_8.3.0:make-columnparent :column-family column-family))
        (slice-predicate (cassandra_8.3.0:make-slicepredicate))
        (slice-range nil))
    (when super-column
      (setf (cassandra_8.3.0:columnparent-super-column column-parent) super-column))
    (if cn-s
      (setf (cassandra_8.3.0:slicepredicate-column-names slice-predicate) (mapcar #'string column-names))
      (setf slice-range (cassandra_8.3.0:make-slicerange :reversed reversed :count count :start start :finish finish)
            (cassandra_8.3.0:slicepredicate-slice-range slice-predicate) slice-range))
    (cassandra_8.3.0:get-slice keyspace key column-parent slice-predicate consistency-level)))


(defmethod map-slice (op (keyspace cassandra_8.3.0:keyspace) &key
                      key column-family super-column (start "") (finish "") (column-names nil cn-s) reversed count
                      (consistency-level (keyspace-consistency-level keyspace)))
  (let ((column-parent (cassandra_8.3.0:make-columnparent :column-family column-family))
        (slice-predicate (cassandra_8.3.0:make-slicepredicate))
        (slice-range nil))
    (when super-column
      (setf (cassandra_8.3.0:columnparent-super-column column-parent) super-column))
    (cond (cn-s
           (setf (cassandra_8.3.0:slicepredicate-column-names slice-predicate) (mapcar #'string column-names))
           (map nil op
                (cassandra_8.3.0:get-slice keyspace key column-parent slice-predicate consistency-level)))
          (t
           (setf slice-range (cassandra_8.3.0:make-slicerange :reversed reversed :count count :start start :finish finish))
           (setf (cassandra_8.3.0:slicepredicate-slice-range slice-predicate) slice-range)
           (let ((last-column nil))
             (loop (let ((slice (cassandra_8.3.0:get-slice keyspace key column-parent slice-predicate consistency-level)))
                     (when last-column (pop slice))
                     (unless slice (return))
                     (loop (unless slice (return))
                           (when (minusp (decf count)) (return))
                           (funcall op (setf last-column (cassandra_8.3.0:columnorsupercolumn-column (pop slice)))))
                       (setf (cassandra_8.3.0:slicerange-start slice-range)
                             (cassandra_8.3.0:column-name last-column)))))))))


;;; multiget: 0.6 only

(defmethod multiget ((keyspace cassandra_2.1.0:keyspace) &key
                     keys column-path
                     (consistency-level (keyspace-consistency-level keyspace)))
  (cassandra_2.1.0:multiget keyspace (keyspace-name keyspace) keys column-path consistency-level))


;;; multiget_slice

(defmethod multiget-slice ((keyspace cassandra_2.1.0:keyspace) &key
                           keys column-parent predicate
                           (consistency-level (keyspace-consistency-level keyspace)))
  (cassandra_2.1.0:multiget-slice keyspace (keyspace-name keyspace) keys column-parent predicate consistency-level))

(defmethod multiget-slice ((keyspace cassandra_8.3.0:keyspace) &key
                           keys column-parent predicate
                           (consistency-level (keyspace-consistency-level keyspace)))
  (cassandra_8.3.0:multiget-slice keyspace keys column-parent predicate consistency-level))


;;; get_count

(defmethod get-count ((keyspace cassandra_2.1.0:keyspace) &key
                      key column-parent predicate
                      (consistency-level (keyspace-consistency-level keyspace)))
  (when predicate (warn "Predicate is ignored: ~a; ~a" keyspace predicate))
  (cassandra_2.1.0:get-count keyspace (keyspace-name keyspace) key column-parent consistency-level))

(defmethod get-count ((keyspace cassandra_8.3.0:keyspace) &key
                      key column-parent predicate
                      (consistency-level (keyspace-consistency-level keyspace)))
  (cassandra_8.3.0:get-count keyspace key column-parent predicate consistency-level))


;;; multiget_count : >= 0.7, ? but with a keyspace name ?

(defmethod multiget-count ((keyspace cassandra_8.3.0:keyspace) &key
                           keys column-parent predicate
                           (consistency-level (keyspace-consistency-level keyspace)))
  (cassandra_8.3.0:multiget-count keyspace (keyspace-name keyspace) keys column-parent predicate consistency-level))


;;; get_range_slice : <= 0.6

(defmethod get-range-slice ((keyspace cassandra_2.1.0:keyspace) &key
                            column-parent predicate start-key finish-key row-count
                            (consistency-level (keyspace-consistency-level keyspace)))
  (cassandra_2.1.0:get-range-slice keyspace (keyspace-name keyspace) column-parent predicate
                             start-key finish-key row-count  consistency-level))


;;; get_range_slices

(defmethod get-range-slices ((keyspace cassandra_2.1.0:keyspace) &key
                              key (start-key key) (finish-key key)
                              column-family super-column (start "") (finish "") (column-names nil cn-s) reversed count
                             (consistency-level (keyspace-consistency-level keyspace)))
  (unless count (setf count (keyspace-slice-size keyspace)))
  (let ((key-range (cassandra_2.1.0:make-keyrange :start-key start-key :end-key finish-key :count count))
        (column-parent (cassandra_2.1.0:make-columnparent :column-family  column-family))
        (slice-predicate (cassandra_2.1.0:make-slicepredicate)))
    (when super-column
      (setf (cassandra_2.1.0:columnparent-super-column column-parent) super-column))
    (if cn-s
      (setf (cassandra_2.1.0:slicepredicate-column-names slice-predicate) (mapcar #'string column-names))
      (setf (cassandra_2.1.0:slicepredicate-slice-range slice-predicate)
            (cassandra_2.1.0:make-slicerange :reversed reversed :count most-positive-i32 :start start :finish finish)))
    (cassandra_2.1.0:get-range-slices keyspace (keyspace-name keyspace) column-parent slice-predicate key-range consistency-level)))


(defmethod get-range-slices ((keyspace cassandra_8.3.0:keyspace) &key
                              key (start-key key) (finish-key key)
                              column-family super-column (start "") (finish "") (column-names nil cn-s) reversed count
                             (consistency-level (keyspace-consistency-level keyspace)))
  (unless count (setf count (keyspace-slice-size keyspace)))
  (let ((key-range (cassandra_8.3.0:make-keyrange :start-key start-key :end-key finish-key :count count))
        (column-parent (cassandra_2.1.0:make-columnparent :column-family  column-family))
        (slice-predicate (cassandra_2.1.0:make-slicepredicate)))
    (when super-column
      (setf (cassandra_2.1.0:columnparent-super-column column-parent) super-column))
    (if cn-s
      (setf (cassandra_2.1.0:slicepredicate-column-names slice-predicate) (mapcar #'string column-names))
      (setf (cassandra_2.1.0:slicepredicate-slice-range slice-predicate)
            (cassandra_2.1.0:make-slicerange :reversed reversed :count count :start start :finish finish)))
    (cassandra_8.3.0:get-range-slices keyspace column-parent slice-predicate key-range consistency-level)))


(defmethod map-range-slices (op (keyspace cassandra_2.1.0:keyspace) &key
                              key (start-key key) (finish-key key)
                              column-family super-column (start "") (finish "") (column-names nil cn-s) reversed
                              (count (keyspace-slice-size keyspace))
                              (consistency-level (keyspace-consistency-level keyspace)))
  (let ((key-range (cassandra_2.1.0:make-keyrange :start-key start-key :end-key finish-key :count (keyspace-slice-size keyspace)))
        (column-parent (cassandra_2.1.0:make-columnparent :column-family  column-family))
        (slice-predicate (cassandra_2.1.0:make-slicepredicate))
        (slice-range nil))
    (when super-column
      (setf (cassandra_2.1.0:columnparent-super-column column-parent) super-column))
    (cond (cn-s
           (setf (cassandra_2.1.0:slicepredicate-column-names slice-predicate) (mapcar #'string column-names)))
          (t
           (setf slice-range (cassandra_2.1.0:make-slicerange :reversed reversed :count (keyspace-slice-size keyspace) :start start :finish finish))
           (setf (cassandra_2.1.0:slicepredicate-slice-range slice-predicate) slice-range)))
    (let ((last-key-slice nil))
      (loop (let ((slice (cassandra_2.1.0:get-range-slices  keyspace (keyspace-name keyspace) column-parent slice-predicate key-range consistency-level)))
              (when last-key-slice (pop slice))
              (unless slice (return))
              (loop (unless slice (return))
                    (when (and count (minusp (decf count))) (return))
                    (setf last-key-slice (pop slice))
                    (funcall op last-key-slice)
                    (setf (cassandra_2.1.0:keyrange-start-key  key-range)
                          (cassandra_2.1.0:keyslice-key last-key-slice))))))))


(defmethod map-range-slices (op (keyspace cassandra_8.3.0:keyspace) &key
                              key (start-key key) (finish-key key)
                              column-family super-column (start "") (finish "") (column-names nil cn-s) reversed
                              (count (keyspace-slice-size keyspace))
                              (consistency-level (keyspace-consistency-level keyspace)))
  (let ((key-range (cassandra_8.3.0:make-keyrange :start-key start-key :end-key finish-key :count (keyspace-slice-size keyspace)))
        (column-parent (cassandra_8.3.0:make-columnparent :column-family  column-family))
        (slice-predicate (cassandra_8.3.0:make-slicepredicate))
        (slice-range nil))
    (when super-column
      (setf (cassandra_8.3.0:columnparent-super-column column-parent) super-column))
    (cond (cn-s
           (setf (cassandra_8.3.0:slicepredicate-column-names slice-predicate) (mapcar #'string column-names)))
          (t
           (setf slice-range (cassandra_8.3.0:make-slicerange :reversed reversed :count (keyspace-slice-size keyspace) :start start :finish finish))
           (setf (cassandra_8.3.0:slicepredicate-slice-range slice-predicate) slice-range)))
    (let ((last-key-slice nil))
      (loop (let ((slice (cassandra_8.3.0:get-range-slices keyspace column-parent slice-predicate key-range consistency-level)))
              (when last-key-slice (pop slice))
              (unless slice (return))
              (loop (unless slice (return))
                    (when (and count (minusp (decf count))) (return))
                    (setf last-key-slice (pop slice))
                    (funcall op last-key-slice)
                    (setf (cassandra_8.3.0:keyrange-start-key  key-range)
                          (cassandra_8.3.0:keyslice-key last-key-slice))))))))


;;; scan

(defmethod scan ((keyspace cassandra_8.3.0:keyspace) &key
                 column-parent row-predicate column-predicate
                 (consistency-level (keyspace-consistency-level keyspace)))
  (cassandra_8.3.0:scan keyspace column-parent row-predicate column-predicate consistency-level))


;;; scan-count : >= 0.7

(defmethod scan-count ((keyspace cassandra_8.3.0:keyspace) &key
                 column-parent row-predicate column-predicate
                 (consistency-level (keyspace-consistency-level keyspace)))
  (cassandra_8.3.0:scan-count keyspace column-parent row-predicate column-predicate consistency-level))


;;; insert

(defmethod insert ((keyspace cassandra_2.1.0:keyspace) &key
                   key column-family super-column column value ttl
                   timestamp (clock (keyspace-clock keyspace timestamp))
                   (consistency-level (keyspace-consistency-level keyspace)))
  (when ttl (warn "TTL is ignored: ~a; ~a" keyspace ttl))
  (let ((column-path (cassandra_2.1.0:make-columnpath :column-family column-family
                                                      :column column)))
    (when super-column (setf (cassandra_2.1.0:columnpath-super-column column-path) super-column))
    (cassandra_2.1.0:insert keyspace (keyspace-name keyspace) key column-path value (cassandra_8.3.0:clock-timestamp clock) consistency-level)))

(defmethod insert ((keyspace cassandra_8.3.0:keyspace) &key
                   key column-family super-column column value ttl
                   timestamp (clock (keyspace-clock keyspace timestamp))
                   (consistency-level (keyspace-consistency-level keyspace)))
  (let ((column-parent (cassandra_8.3.0:make-columnparent :column-family column-family))
        (column (cassandra_8.3.0:make-column  :name column :value value :clock clock)))
    (when super-column (setf (cassandra_8.3.0:columnparent-super-column column-parent) super-column))
    (when ttl (setf (cassandra_8.3.0:column-ttl column) ttl))
    (cassandra_8.3.0:insert keyspace key column-parent column consistency-level)))


;;; batch_insert : <= 0.6 

(defmethod batch-insert ((keyspace cassandra_2.1.0:keyspace) &key
                         key cfmap
                         (consistency-level (keyspace-consistency-level keyspace)))
  (cassandra_2.1.0:batch-insert keyspace (keyspace-name keyspace) key cfmap consistency-level))


;;; remove

(defmethod remove ((keyspace cassandra_2.1.0:keyspace) &key
                   (key (error "key is required."))
                   (column-family (error "column-family is required."))
                   super-column column
                   timestamp (clock (keyspace-clock keyspace timestamp))
                   (consistency-level (keyspace-consistency-level keyspace)))
  (let ((column-path (cassandra_2.1.0:make-columnpath :column-family column-family)))
    (when column (setf (cassandra_2.1.0:columnpath-column column-path) column))
    (when super-column (setf (cassandra_2.1.0:columnpath-super-column column-path) super-column))
    (cassandra_2.1.0:remove keyspace (keyspace-name keyspace) key column-path (cassandra_8.3.0:clock-timestamp clock) consistency-level)))

(defmethod remove ((keyspace cassandra_8.3.0:keyspace) &key
                   (key (error "key is required."))
                   (column-family (error "column-family is required."))
                   super-column column
                   timestamp (clock (keyspace-clock keyspace timestamp))
                   (consistency-level (keyspace-consistency-level keyspace)))
  (let ((column-path (cassandra_8.3.0:make-columnpath :column-family column-family)))
    (when column (setf (cassandra_2.1.0:columnpath-column column-path) column))
    (when super-column (setf (cassandra_8.3.0:columnpath-super-column column-path) super-column))
    (cassandra_8.3.0:remove keyspace key column-path clock consistency-level)))



;;; batch_mutate

(defmethod batch-mutate ((keyspace cassandra_2.1.0:keyspace) &key
                         mutation-map
                         (consistency-level (keyspace-consistency-level keyspace)))
  (cassandra_2.1.0:batch-mutate keyspace (keyspace-name keyspace) mutation-map consistency-level))

(defmethod batch-mutate ((keyspace cassandra_8.3.0:keyspace) &key
                         mutation-map
                         (consistency-level (keyspace-consistency-level keyspace)))
  (cassandra_8.3.0:batch-mutate keyspace mutation-map consistency-level))

(defgeneric make-standard-mutation-map (keyspace family-name key &rest plist)
  (:documentation "Construct a mutation script for a standard column family given a FAMILY-NAME, a row KEY,
 and a name/value PLIST for the individual columns.")

  (:method ((keyspace cassandra_2.1.0:keyspace) column-family-name key &rest name-value-plist)
    (declare (dynamic-extent name-value-plist))
    (let* ((clock (keyspace-clock keyspace))
           (timestamp (cassandra_8.3.0:clock-timestamp clock)))
      (thrift:map key (thrift:map column-family-name
                                  (loop for (column-name value) on name-value-plist by #'cddr
                                        when value     ; skip null values
                                        collect (cassandra_2.1.0:make-mutation
                                                  :column-or-supercolumn (cassandra_2.1.0:make-columnorsupercolumn
                                                                          :column (cassandra_2.1.0:make-column
                                                                                   :name column-name
                                                                                   :value value
                                                                                   :timestamp timestamp))))))))

  (:method ((keyspace cassandra_8.3.0:keyspace) column-family-name key &rest name-value-plist)
    (declare (dynamic-extent name-value-plist))
    (let ((clock (keyspace-clock keyspace)))
      (thrift:map key (thrift:map column-family-name
                                  (loop for (column-name value) on name-value-plist by #'cddr
                                        when value     ; skip null values
                                        collect (cassandra_8.3.0:make-mutation
                                                  :column-or-supercolumn (cassandra_8.3.0:make-columnorsupercolumn
                                                                          :column (cassandra_8.3.0:make-column
                                                                                   :name column-name
                                                                                   :value value
                                                                                   :clock clock)))))))))

(defgeneric make-super-mutation-map (keyspace family-name key super-column-key &rest plist)
  (:documentation "Construct a mutation script for a super column family given a FAMILY-NAME, a row KEY,
 a SUPER-COLUMN-KEY, and a name/value PLIST for the individual columns.")

  (:method ((keyspace cassandra_2.1.0:keyspace) column-family-name key super-column-key &rest name-value-plist)
    (declare (dynamic-extent name-value-plist))
    (let* ((clock (keyspace-clock keyspace))
           (timestamp (cassandra_8.3.0:clock-timestamp clock)))
     (thrift:map key (thrift:map column-family-name
                                 (list (cassandra_2.1.0:make-mutation
                                        :column-or-supercolumn
                                        (cassandra_2.1.0:make-columnorsupercolumn
                                         :super-column (cassandra_2.1.0:make-supercolumn
                                                        :name super-column-key
                                                        :columns (loop for (column-name value)
                                                                       on name-value-plist by #'cddr
                                                                       when value     ; skip null values
                                                                       collect (cassandra_2.1.0:make-column
                                                                                :name column-name
                                                                                :value value
                                                                                :timestamp timestamp))))))))))

   (:method ((keyspace cassandra_8.3.0:keyspace) column-family-name key super-column-key &rest name-value-plist)
    (declare (dynamic-extent name-value-plist))
    (let* ((clock (keyspace-clock keyspace)))
      (thrift:map key (thrift:map column-family-name
                                  (list (cassandra_8.3.0:make-mutation
                                         :column-or-supercolumn
                                         (cassandra_8.3.0:make-columnorsupercolumn
                                          :super-column (cassandra_8.3.0:make-supercolumn
                                                         :name super-column-key
                                                         :columns (loop for (column-name value)
                                                                        on name-value-plist by #'cddr
                                                                        when value     ; skip null values
                                                                        collect (cassandra_8.3.0:make-column
                                                                                 :name column-name
                                                                                 :value value
                                                                                 :clock clock)))))))))))



#+(or)
(make-standard-mutation-map (allocate-instance (find-class 'cassandra_8.3.0:keyspace)) "SPOCIndex" "spoc-1"
                            :subject "s1" :predicate "p1" :object "o1" :context "c1")


;;; truncate : >= 0.7

(defmethod truncate ((keyspace cassandra_8.3.0:keyspace) &key
                     column-family)
  (cassandra_8.3.0:truncate keyspace column-family))


;;; check_schema_agreement : >= 0.7

(defmethod check-schema-agreement ((keyspace cassandra_8.3.0:keyspace))
  (cassandra_8.3.0:check-schema-agreement keyspace))


;;; get_string_property : <= 0.6

(defmethod get-string-property ((keyspace cassandra_2.1.0:keyspace) &key property)
  (cassandra_2.1.0:get-string-property keyspace property))


;;; get_string_list_property : <= 0.6

(defmethod get-string-list-property ((keyspace cassandra_2.1.0:keyspace) &key property)
  (cassandra_2.1.0:get-string-list-property keyspace property))


;;; describe_keyspaces

(defmethod describe-keyspaces ((keyspace keyspace))
  ;; get by with the earliest version
  (cassandra_2.1.0:describe-keyspaces keyspace))

(defmethod describe-keyspaces ((keyspace cassandra_2.1.0:keyspace))
  (cassandra_2.1.0:describe-keyspaces keyspace))

(defmethod describe-keyspaces ((keyspace cassandra_8.3.0:keyspace))
  (cassandra_8.3.0:describe-keyspaces keyspace))


;;; describe_cluster_name

(defmethod describe-cluster-name ((keyspace cassandra_2.1.0:keyspace))
  (cassandra_2.1.0:describe-cluster-name keyspace))

(defmethod describe-cluster-name ((keyspace cassandra_8.3.0:keyspace))
  (cassandra_8.3.0:describe-cluster-name keyspace))


;;; describe_version

(defmethod describe-version ((keyspace keyspace))
  ;; get by with the earliest version
  (cassandra_2.1.0:describe-version keyspace))

(defmethod describe-version ((keyspace cassandra_2.1.0:keyspace))
  (cassandra_2.1.0:describe-version keyspace))

(defmethod describe-version ((keyspace cassandra_8.3.0:keyspace))
  (cassandra_8.3.0:describe-version keyspace))


;;; describe_ring

(defmethod describe-ring ((keyspace cassandra_2.1.0:keyspace))
  (cassandra_2.1.0:describe-ring keyspace (keyspace-name keyspace)))

(defmethod describe-ring ((keyspace cassandra_8.3.0:keyspace))
  (cassandra_8.3.0:describe-ring keyspace (keyspace-name keyspace)))


;;; describe_keyspace

(defmethod describe-keyspace ((keyspace cassandra_2.1.0:keyspace))
  (cassandra_2.1.0:describe-keyspace keyspace (keyspace-name keyspace)))

(defmethod describe-keyspace ((keyspace cassandra_8.3.0:keyspace))
  (cassandra_8.3.0:describe-keyspace keyspace (keyspace-name keyspace)))


(defmethod system-add-column-family ((keyspace cassandra_8.3.0:keyspace) &key cf-def)
  (cassandra_8.3.0:system-add-column-family keyspace cf-def))


(defmethod system-drop-column-family ((keyspace cassandra_8.3.0:keyspace) column-family)
  (cassandra_8.3.0:system-drop-column-family keyspace column-family))


(defmethod system-rename-column-family ((keyspace cassandra_8.3.0:keyspace) &key old-name new-name)
  (cassandra_8.3.0:system-rename-column-family keyspace old-name new-name))


(defmethod system-add-keyspace ((keyspace cassandra_8.3.0:keyspace) ks-def)
  (cassandra_8.3.0:system-add-keyspace keyspace ks-def))


(defmethod system-drop-keyspace ((keyspace cassandra_8.3.0:keyspace) keyspace-name)
  (cassandra_8.3.0:system-drop-keyspace keyspace keyspace-name))


(defmethod system-rename-keyspace ((keyspace cassandra_8.3.0:keyspace) old-name new-name)
  (cassandra_8.3.0:system-rename-keyspace keyspace old-name new-name))

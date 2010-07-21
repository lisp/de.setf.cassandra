;;; -*- Mode: lisp; Syntax: ansi-common-lisp; Base: 10; Package: de.setf.cassandra; -*-

(in-package :de.setf.cassandra)

;;;  This file defines column family operators for the 'de.setf.cassandra' library component.
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


(defclass column-family ()
  ((keyspace
    :initform (error "keyspace is required.") :initarg :keyspace
    :reader column-family-keyspace)
   (name
    :initform (error "name is required.") :initarg :name
    :accessor column-family-name)
   (slice-size
     :accessor column-family-slice-size
     :type i32)
   (columnpath
    :reader column-family-columnpath)
   (columnparent
    :reader column-family-columnparent)
   (slicepredicate
    :reader column-family-slicepredicate)
   (slicerange
    :reader column-family-slicerange)))

(defmethod initialize-instance :after ((instance column-family) &key
                                       (slice-size (keyspace-slice-size (column-family-keyspace instance))))
  (setf (slot-value instance 'columnpath)
        (cassandra::make-columnpath :column-family (column-family-name instance)))
  (setf (slot-value instance 'columnparent)
        (cassandra::make-columnparent :column-family (column-family-name instance)))
  (setf (slot-value instance 'slicepredicate)
        (cassandra::make-slicepredicate))
  (setf (slot-value instance 'slicerange)
        (cassandra::make-slicerange :reversed nil :count slice-size :start "" :finish ""))
  (setf (column-family-slice-size instance)
        slice-size))

(defgeneric attribute-value (column-family key column-name)

  (:method ((column-family column-family) key column-name)
    (let ((column-path (column-family-columnpath column-family))
          (cosc nil))
      (setf (cassandra::columnpath-column column-path) column-name)
      (when (setf cosc (get (column-family-keyspace column-family)
                            :key key
                            :column-path column-path))
        (let ((column (cassandra::columnorsupercolumn-column cosc)))
          (values (cassandra::column-value column)
                  (cassandra::column-timestamp column)
                  (cassandra::column-name column)))))))


(defgeneric (setf attribute-value) (value column-family key column-name)

  (:method (value (column-family column-family) key column-name)
    (let ((column-path (column-family-columnpath column-family)))
      (setf (cassandra::columnpath-column column-path) column-name)
      (insert (column-family-keyspace column-family)
              :key key
              :column-path column-path
              :value value))))


(defgeneric attribute-values (column-family key &key start finish column-names reversed count)

  (:method ((column-family column-family) key &key (start "") (finish "") column-names reversed
                                            (count (column-family-slice-size column-family)))
    (let ((column-parent (column-family-columnparent column-family))
          (slice-predicate (column-family-slicepredicate column-family))
          (slice-range (column-family-slicerange column-family)))
      (if column-names
        (setf (cassandra::slicepredicate-column-names slice-predicate) column-names
              (cassandra::slicepredicate-slice-range slice-predicate) nil)
        (setf (cassandra::slicepredicate-column-names slice-predicate) nil
              (cassandra::slicepredicate-slice-range slice-predicate) slice-range
              (cassandra::slicerange-start slice-range) start 
              (cassandra::slicerange-finish slice-range) finish
              (cassandra::slicerange-reversed slice-range) reversed
              (cassandra::slicerange-count slice-range) count))
      (loop for cosc in (get-slice (column-family-keyspace column-family)
                                   :key key
                                   :column-parent column-parent
                                   :predicate slice-predicate)
            collect (let ((column (cassandra::columnorsupercolumn-column cosc)))
                      (list (cassandra::column-name column)
                            (cassandra::column-value column)
                            (cassandra::column-timestamp column)))))))


(defgeneric (setf attribute-values) (values column-family key &key column-names)
  
  (:method (values (column-family column-family) key &key column-names)
    (let ((timestamp (uuid::get-timestamp)))
      (batch-mutate (column-family-keyspace column-family)
                    :mutation-map (thrift:map `(,key . ((,(column-family-name column-family)
                                                        ,@(mapcar #'(lambda (column-name value)
                                                                      (cassandra:make-mutation
                                                                       :column-or-supercolumn
                                                                       (cassandra:make-columnorsupercolumn
                                                                        :column (cassandra:make-column
                                                                                 :name column-name
                                                                                 :value value
                                                                                 :timestamp timestamp))))
                                                                  column-names values)))))))))
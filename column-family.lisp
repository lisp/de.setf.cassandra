;;; -*- Mode: lisp; Syntax: ansi-common-lisp; Base: 10; Package: de.setf.cassandra; -*-

(in-package :de.setf.cassandra)

;;;  This file defines column family operators for the 'de.setf.cassandra' library.
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

;;;
;;; interface

;;; alternative:
;;; cf-get : returns a column
;;; cf-get-string : returns a string value
;;; cf-get-value : returns the raw value
;;; fc-get-as 'type : 

(defgeneric get-column (column-family key column-name)
  (:documentation "Given a COLUMN-FAMILY, KEY, and COLUMN-NAME, retrieve the designated attribute 'column'
 from the family's store. This returns the column struct, which comprises the name, value, and timestamp.
 A simple column-family requires atomic keys, while a super-column permits either the atomic family key or
 a list of family-key and super-column key."))


(defgeneric get-attribute (column-family key column-name)
  (:documentation "Given a COLUMN-FAMILY, KEY, and COLUMN-NAME, retrieve the designated attribute value
 from the family's store. This retrieves the attribute 'column' and returns just its value."))


(defgeneric set-attribute (column-family key column-name value)
  (:documentation "Given a COLUMN-FAMILY, KEY, COLUMN-NAME, VALUE, store the
 designated attribute 'column' in the family's store. The timestamp defaults to the UUID-V1 timestamp.
 A simple column-family requires atomic keys, while a super-column requires a list of family-key and
 super-column key.") )


(defgeneric get-columns (column-family key &key start finish column-names reversed count)
  (:documentation "Given a COLUMN-FAMILY, KEY, and a combination of START and FINISH column names, a
 COLUMN-NAME list, REVERSED indicator, and a COUNT, retrieve the designated attribute 'columns' from the 
 family's store. (see cassandra:get-silce.) A simple column-family requires atomic an key, while a super-column
 permits either the atomic family key or a list of family-key and super-column key.
 In the former case, the respective elements are of the form ((key supercolumn-key) . column*)."))


(defgeneric get-attributes (column-family key &key start finish column-names reversed count)
  (:documentation "Given a COLUMN-FAMILY, a KEY, and constraints as for get-attributes, retrieve the
 attribute 'columns' and of those the respective values."))


(defgeneric map-columns (operator column-family key &key start finish column-names reversed count)
  (:documentation "Given on OPERATOR, a COLUMN-FAMILY, KEY, and a combination of START and FINISH column names, a
 COLUMN-NAME list, REVERSED indicator, and a COUNT, map the operator over the designated attribute 'columns' from the 
 family's store. (see get-slice and getrange-slices.) A simple column-family requires atomic an key and yields
 elementary columns, while a super-column permits either the atomic family key or a list of family-key and super-column key.
 In the former case, the respective elements are of the form ((key supercolumn-key) . column*)."))


(defgeneric set-attributes (column-family key &rest property-list)
  (:documentation "Given a COLUMN-FAMILY, a key, and a property list of column names and values,
 store the designated attribute 'columns' in the family's store. The first property can be :timestamp,
 to specify the column timestamp, for which the default is the UUID-V1 timestamp."))


;;;
;;; classes
;;;  column-famiy
;;;  standard-column-family
;;;  super-column-family

(defclass column-family ()
  ((keyspace
    :initform (error "keyspace is required.") :initarg :keyspace
    :reader column-family-keyspace
    :type keyspace
    :documentation "The column's keyspace instance. All operations are delegated through this keyspace
     to the respective store.")
   (name
    :initform (error "name is required.") :initarg :name
    :accessor column-family-name
    :documentation "The column family name.[http://wiki.apache.org/cassandra/DataModel#Column_Families].")
   (type
    :reader column-family-type)
   (slice-size
     :accessor column-family-slice-size
     :type i32
     :documentation "The default count to use for retrieval operations which return multiple values.
      If no value is provided, the default is adopted from the column family's key space."))

  (:documentation "The abstract class defines the slots shared by the column-family and super-column-family
 classes to bind the store keyspace, the family name and the structs and parameters used in access operations."))


(defclass standard-column-family (column-family)
  ((type :initform :standard :allocation :class))
  (:documentation "A standard-column-family represents a single-level cassandra hash."))


(defclass super-column-family (column-family)
  ((type :initform :super :allocation :class))
  (:documentation "A super-column-family represents a two-level cassandra hash."))


;;; generic operations

(defmethod initialize-instance :after ((instance column-family) &key
                                       (slice-size (keyspace-slice-size (column-family-keyspace instance))))
  (setf (column-family-slice-size instance) slice-size)
  (let* ((ks (column-family-keyspace instance))
         (column-description (assoc (column-family-name instance) (keyspace-description ks) :test #'string-equal)))
    (cond ((string-equal (column-family-type instance) (rest (assoc "Type" (rest column-description) :test #'string-equal))))
          (column-description
           (error "Invalid column type: ~s: ~s."
                  instance column-description))
          (t
           (error "Invalid column: ~s (~s): ~s ~s."
                  instance (column-family-name instance) ks (mapcar #'first (keyspace-description ks)))))))

(defmethod print-object ((object column-family) stream)
  (print-unreadable-object (object stream :identity t :type t)
    (format stream "~s" (when (slot-boundp object 'name) (column-family-name object)))))

(defmethod get-attribute ((column-family column-family) key column-name)
  (column-value (get-column column-family key column-name)))


(defmethod get-attributes ((column-family column-family) key &rest args)
  (declare (dynamic-extent args))
  (labels ((walk-value (object)
             (typecase object
               (cons (cons (walk-value (first object)) (walk-value (rest object))))
               ((or cassandra_2.1.0:column cassandra_8.3.0:column)
                (cons (column-name object) (column-value object)))
               (t object))))
    (mapcar #'walk-value (apply #'get-columns column-family key args))))

(defmethod set-keyspace-column-family ((keyspace keyspace) (cf column-family) slot-name
                                       &key (class 'standard-column-family) required)
  (declare (ignore required))
  (assert (typep cf class) () "Supplied column family is not the correct type: ~s: ~s: ~s." slot-name cf class)
  (setf (slot-value keyspace slot-name) cf))



;;;
;;; standard column family operators expect the column key to map to a sequence of columns.

(defmethod get-column ((family standard-column-family) key column-name)
  (columnorsupercolumn-column (get (column-family-keyspace family)
                                   :column-family (column-family-name family) :key key :column column-name)))


(defmethod set-attribute ((family standard-column-family) key column-name column-value)
  (insert (column-family-keyspace family)
          :column-family (column-family-name family) :key key
          :column column-name :value column-value))


(defmethod set-attribute ((family standard-column-family) key column-name (value null))
  "Given a null value, delete the column"
  (remove (column-family-keyspace family)
          :column-family (column-family-name family) :key key
          :column column-name))


(defmethod get-columns ((family standard-column-family) key &rest args
                        &key start finish column-names reversed (count (column-family-slice-size family)))
  (declare (ignore start finish column-names reversed)
           (dynamic-extent args))
  (mapcar #'columnorsupercolumn-column
          (apply #'get-slice (column-family-keyspace family)
                 :column-family (column-family-name family) :key key
                 :count count
                 args)))


(defmethod map-columns (op (family standard-column-family) key &rest args
                        &key start finish column-names reversed (count (column-family-slice-size family)))
  (declare (ignore start finish column-names reversed)
           (dynamic-extent args))
  (flet ((coerce-cosc (cosc) (funcall op (columnorsupercolumn-column cosc))))
    (declare (dynamic-extent #'coerce-cosc))
    (apply #'map-slice #'coerce-cosc (column-family-keyspace family)
           :column-family (column-family-name family) :key key
           :count count
           args)))


(defmethod set-attributes ((column-family standard-column-family) key &rest property-list)
  (batch-mutate (column-family-keyspace column-family)
                :mutation-map (apply #'make-standard-mutation-map (column-family-keyspace column-family)
                                     (column-family-name column-family) key
                                     property-list)))


;;;
;;; super-column family methods expect the first key to identify a sequence of super columns, from which the
;;; second key selects a specific super-column which contains the column set

(defmethod get-column ((family super-column-family) (keys cons) column-name)
  (destructuring-bind (key super-column) keys
    (columnorsupercolumn-column (get (column-family-keyspace family)
                                     :column-family (column-family-name family) :key key
                                     :super-column super-column :column column-name))))


(defmethod set-attribute ((family super-column-family) (keys cons) column-name column-value)
  (destructuring-bind (key super-column) keys
    (insert (column-family-keyspace family)
            :column-family (column-family-name family) :key key :super-column super-column
            :column column-name :value column-value)))


(defmethod set-attribute ((family super-column-family) (keys cons) column-name (value null))
  "Given a null value, delete the column"
  (destructuring-bind (key super-column) keys
    (remove (column-family-keyspace family)
          :column-family (column-family-name family) :key key :super-column super-column
          :column column-name)))


;;;!!! combine the list v/s atomic key into one method which allows also a null row key with a supercolumn key
(defmethod get-columns ((family super-column-family) (keys cons) &rest args
                        &key start finish column-names reversed (count (column-family-slice-size family)))
  "Where the key is a list, the first element is the family key and the second is the syper-column key."
  (declare (ignore start finish column-names reversed)
           (dynamic-extent args))
  (destructuring-bind (key super-column) keys
    (mapcar #'columnorsupercolumn-column
            (apply #'get-slice (column-family-keyspace family)
                   :column-family (column-family-name family) :key key :super-column super-column
                   :count count
                   args))))


(defmethod map-columns (op (family super-column-family) (keys cons) &rest args
                        &key start finish column-names reversed (count (column-family-slice-size family)))
  "Where the key is a list, the first element is the family key and the second is the syper-column key."
  (declare (ignore start finish column-names reversed)
           (dynamic-extent args))
  (destructuring-bind (key super-column) keys
    (flet ((coerce-cosc (cosc) (funcall op (columnorsupercolumn-column cosc))))
      (declare (dynamic-extent #'coerce-cosc))
      (apply #'map-slice #'coerce-cosc (column-family-keyspace family)
             :column-family (column-family-name family) :key key :super-column super-column
             :count count
             args))))


(defmethod get-columns ((family super-column-family) key &rest args
                        &key start finish column-names reversed (count (column-family-slice-size family)))
  "Where the key is atomic, it applies to all supercolumns. The result is then a list of super-column results
 in which each entry has the form (supercolumn . columns)."
  (declare (ignore start finish column-names reversed)
           (dynamic-extent args))
  (loop for key-slice in (apply #'get-range-slices (column-family-keyspace family)
                                :column-family (column-family-name family)
                                :start-key (or key "") :finish-key (or key "")
                                :count count
                                args)
        for key = (keyslice-key key-slice)
        append (loop for cosc in (keyslice-columns key-slice)
                     for sc = (columnorsupercolumn-super-column cosc)
                     collect (cons (list key (supercolumn-name sc))
                                   (supercolumn-columns sc)))))


(defmethod map-columns (op (family super-column-family) key &rest args
                        &key start finish column-names reversed (count (column-family-slice-size family)))
  "Where the key is atomic, it applies to all supercolumns. The result is then a list of super-column results
 in which each entry has the form (supercolumn . columns)."
  (declare (ignore start finish column-names reversed)
           (dynamic-extent args))
  (flet ((do-key-slice (key-slice)
           (let ((key (keyslice-key key-slice)))
             (loop for cosc = (keyslice-columns key-slice)
                   for sc = (columnorsupercolumn-super-column cosc)
                   do (funcall op  (cons (list key (supercolumn-name sc)) (supercolumn-columns sc)))))))
    (declare (dynamic-extent #'do-key-slice))
    (apply #'map-range-slices #'do-key-slice (column-family-keyspace family)
           :column-family (column-family-name family)
           :start-key (or key "") :finish-key (or key "")
           :count count
           args)))


(defmethod set-attributes ((family super-column-family) (keys cons) &rest property-list)
  (destructuring-bind (key super-column) keys
    (batch-mutate (column-family-keyspace family)
                  :mutation-map (apply #'make-super-mutation-map (column-family-keyspace family)
                                       (column-family-name family) key super-column
                                       property-list))))

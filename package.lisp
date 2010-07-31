;;; -*- Mode: lisp; Syntax: ansi-common-lisp; Base: 10; Package: common-lisp-user; -*-

(in-package :common-lisp-user)

;;;  This file is the package definition for the 'de.setf.cassandra' library component.
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


(defpackage :de.setf.cassandra
  (:nicknames :dsc)
  (:use :common-lisp :thrift)
  (:documentation "This is the interface package for cassandra applications.")

  (:shadow :get :remove :truncate)
  ;; favor thrifts's stream-write-string, but prefer cl for the other conflicts
  (:shadowing-import-from :thrift
                          :stream-write-string)
  (:shadowing-import-from :common-lisp
                          :byte :set :list :map :type-of :float)

  (:export
   :column-family
   :column-name
   :column-value
   :column-family-keyspace
   :column-family-name
   :column-family-slice-size
   :column-family-type
   :column-name
   :columnorsupercolumn-column
   :columnorsupercolumn-column-family
   :columnorsupercolumn-super-column
   :compute-keyspace-class
   :batch-mutate
   :check-schema-agreement
   :describe-cluster-name
   :describe-keyspace
   :describe-keyspaces
   :describe-ring
   :describe-version
   :get
   :get-attribute
   :get-attributes
   :get-column
   :get-columns
   :get-count
   :get-range-slices
   :get-slice
   :insert
   :keyslice-columns
   :keyslice-key
   :keyspace
   :keyspace-bind-columns
   :keyspace-clock
   :keyspace-consistency-level
   :keyspace-description
   :keyspace-keyspaces
   :keyspace-name
   :keyspace-version
   :keyspace-version-class-map
   :login
   :map-columns
   :map-range-slices
   :map-slice
   :multiget
   :multiget-count
   :multiget-slice
   :remove
   :scan
   :scan-count
   :set-attribute
   :set-attributes
   :set-keyspace-column-family
   :standard-column-family
   :super-column-family
   :supercolumn-name
   :supercolumn-columns
   :system-add-column-family
   :system-add-keyspace
   :system-drop-column-family
   :system-drop-keyspace
   :system-rename-column-family
   :system-rename-keyspace
   :truncate
   :version-class-map
   ))


;;;
;;; add the names for the versioned keyspace classes

(eval-when (:compile-toplevel :load-toplevel :execute)
  (dolist (package '(:cassandra_2.1.0 :cassandra_8.3.0))
    (dolist (class '(:keyspace))
      (export (intern (string class) package) package))))


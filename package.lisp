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
                          :byte :set :list :map :type-of)

  (:export
   :get-attribute
   :set-attribute
   :get-attributes
   :set-attributes
   :keyspace
   :column-family
   :batch-mutate
   :check-schema-agreement
   :describe-cluster-name
   :describe-keyspace
   :describe-keyspaces
   :describe-ring
   :describe-splits
   :describe-version
   :get
   :get-slice
   :get-count
   :get-range-slices
   :insert
   :login
   :multiget
   :multiget-count
   :multiget-slice
   :remove
   :set-keyspace
   :system-add-column-family
   :system-add-keyspace
   :system-drop-column-family
   :system-drop-keyspace
   :system-rename-column-family
   :system-rename-keyspace
   :truncate
   ))

;;;
;;; pick an interface

#+cassandra-thrift-2-1-0
(rename-package :cassandra-2-1-0 :cassandra-2-1-0 '(:cassandra))

;;; extend the cassandra interface package

(macrolet ((extend-package (symbol)
             `(export (intern ,(string symbol) :cassandra) :cassandra)))
  (extend-package :cassandra)
  (extend-package :insert-data))


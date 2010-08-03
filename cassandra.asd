;;; -*- Mode: lisp; Syntax: ansi-common-lisp; Base: 10; Package: common-lisp-user; -*-

(in-package :common-lisp-user)

;;;  This file is the system definition for the 'de.setf.cassandra' library component.
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


(asdf:defsystem :de.setf.cassandra
  :depends-on (:org.apache.thrift
               :net.dardoria.uuid       ; order matters to get asdf to find them
               :com.github.ironclad)
  :description "A Common Lisp Cassandra interface based on the Thrift API"
  :serial t
  :components ((:module :gen-cl
                ;; 2.1.0 and 8.3.0 have radically different interfaces.
                ;; the latter has a distinct set-keyspace while the former require it for each call.
                :components ((:file "cassandra-2-1-0-binary-types")
                             (:file "cassandra-2-1-0-vars")
                             (:file "cassandra-8-3-0-types")
                             (:file "cassandra-8-3-0-vars")))
               (:file "package")
               (:file "api" :depends-on ("package"))
               (:file "keyspace" :depends-on ("api" :gen-cl))
               (:file "column-family" :depends-on ("keyspace")))
  :long-description
  "The de.setf.cassandra library is a client interface to Cassandra[[1]].
 It depends on a provisional Lisp thrift binding[[2]].

 ---
 [1]: http://cassandra.apache.org/
 [2]: http://github.com/lisp/de.setf.thrift
 ")

(pushnew :cassandra-thrift-2-1-0 *features*)
(pushnew :cassandra-thrift-8-3-0 *features*)


#+(or)  ;; when asdf does not notice that it should recompile the api for thrift changes
(load (compile-file (asdf::component-pathname (asdf::find-component
                                               (asdf:find-component
                                                (asdf:find-system :de.setf.cassandra)
                                                "gen-cl")
                                               "cassandra-2-1-0-types"))))

#+(or)  ;; generate documentation
(progn
  (asdf:load-system :de.setf.documentation)
  (setf.documentation:document ':de.setf.cassandra #p"LIBRARY:20100724;")
  )

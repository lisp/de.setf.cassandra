;;; -*- Mode: lisp; Syntax: ansi-common-lisp; Base: 10; Package: de.setf.cassandra; -*-


;;; this illustrates an rdf store structure which uses a RDF.rb keyspace
;;; - an spoc id index for exhaustive iteration
;;; - one each s, p, o, and c index for respective iteration
;;; - half of the two level indices for direct 2/4 and 3/4 retrieval
;;;
;;; see readme-columns for the load/connection steps
;;; see readme-rdf for utility operators ! and macros

(in-package :de.setf.cassandra)

(defclass cassandra-rdfrb-index-mediator (keyspace)
  ((resources-family
    :reader store-resources-family
    :documentation "a column family for the bas resource storage")
   (index-family
    :reader store-index-family
    :documentation "a column family for the index")
   (cache-family
    :reader store-cache-family
    :documentation "a cache column family."))

  (:default-initargs :resources-family "Resources" :cache-family "Cache" :index-family "Index")
  (:documentation "A keyspace of the form used by an RDF mediator."))


(defclass cassandra-rdfrb-index-mediator_2.1.0 (cassandra-rdfrb-index-mediator cassandra_2.1.0:keyspace)
  ())


(defclass cassandra-rdfrb-index-mediator_8.3.0 (cassandra-rdfrb-index-mediator cassandra_8.3.0:keyspace)
  ())

            
(defmethod initialize-instance :after ((instance cassandra-rdfrb-index-mediator)
                                       &key resources-family index-family cache-family)
  (set-keyspace-column-family instance resources-family 'resources-family :class 'super-column-family :required t)
  (set-keyspace-column-family instance index-family 'index-family :class 'super-column-family :required nil)
  (set-keyspace-column-family instance cache-family 'cache-family :class 'standard-column-family :required nil))


(defun compute-spoc-hex-id (&optional subject predicate object context)
  ;; as long as the base function does not pad for missing elements
  (with-output-to-string (stream)
    (loop for elt across (compute-spoc-sha1-id subject predicate object context)
          do (format stream "~(~2,'0x~)" elt))))
;;; (compute-spoc-hex-id #(1) #(2) #(3) #(4))
;;; (compute-spoc-hex-id nil #(2) #(3) nil)
;;; (compute-spoc-hex-id #(2) nil #(3) nil)
;;; (compute-spoc-hex-id nil (binary "<http://ar.to/#self>") nil nil)
;;; (compute-spoc-hex-id nil nil nil nil)

(defvar +default-cache-graph+ (compute-spoc-hex-id ))

(defgeneric cl-user::n3_format (object stream &optional colon at var)
  (:documentation "Given an N3 tuple, write it to the output stream.
 Enclude type information for any non-string encoded as a literal.")
  
  (:method ((stream stream) (object t) &optional colon at var)
    ;; flip the order
    (cl-user::n3_format object stream colon at var))
  
  (:method ((object list) stream &optional colon at var)
    (declare (ignore colon var))
    (destructuring-bind (subject predicate object) object
      (format stream " <~a> <~a> <~a>~:[.~;~]"
              subject predicate object at))
    object))

;;;
;;; manipulating rdf statements
;;; from repository.rb#insert_statements

(defun add-statement.rb (mediator subject predicate object context)
  (declare (ignore context))
  (let* ((s-subject (repository-value mediator subject))
         (s-predicate (repository-value mediator predicate))
         (s-object (repository-value mediator object))
         (index-cf (store-index-family mediator))
         (resources-cf (store-resources-family mediator))
         (cache-cf (store-cache-family mediator)))
    (flet ((index-predicate ()
             (let ((predicate-id (compute-spoc-hex-id s-predicate)))
               (dsc:set-attributes index-cf (list predicate-id :info) (compute-spoc-hex-id s-predicate) s-predicate)
               (dsc:set-attributes index-cf (list predicate-id :ps) (compute-spoc-hex-id s-subject) s-subject)))
           (index-object ()
             (let ((object-id (compute-spoc-hex-id s-object)))
               (dsc:set-attributes index-cf (list object-id :info) (compute-spoc-hex-id s-object) s-object)
               (dsc:set-attributes index-cf (list object-id :os) (compute-spoc-hex-id s-subject) s-subject)
               (dsc:set-attributes index-cf (list object-id :op) (compute-spoc-hex-id s-predicate) s-predicate))))

      (when cache-cf
        (dsc:set-attribute cache-cf +default-cache-graph+
                           (compute-spoc-sha1-id (binary (format nil "~/n3_format/"(list subject predicate object))) nil nil nil)
                           ""))
      (when index-cf
        (index-predicate)
        (index-object))
      (dsc:set-attribute resources-cf (list s-subject s-predicate) (compute-spoc-hex-id s-object) s-object))))



(defun map-statements.rb (mediator continuation subject predicate object context)
  ;; indices
  ;; spoc : ( [s.p.o.c] . statement-attribute* )
  ;; spo  : ( [s.p] . ( [o] . (c . spoc-id)* )* )
  ;; sp c : ( [s.p] . ( [c] . (o . spoc-id)* )* )
  (flet ((map-resource-family (op index subject-key predicate-key)
           ;; given both the subject and predicate, oterate over objects
           (if predicate-key
             (loop for column in (dsc:get-columns index (list subject-key predicate-key))
                   do (funcall op
                               (column-name column)
                               (model-value mediator (column-value column))))
             ;; given just the subject row key, iterate over all predicates
             (loop for ((nil supercolumn-key) . columns) in (dsc:get-columns index subject-key)
                   for value = (model-value mediator supercolumn-key)
                   do (loop for column in columns
                            do (funcall op
                                        value
                                        (column-name column)
                                        (model-value mediator (column-value column))))))))
    
    (handler-case 
      (spoc-case (mediator (s-subject s-predicate s-object s-context) subject predicate object nil)
        :spo-                           ; look for all equivalent statement 
        (let ((column (dsc:get-column (store-resources-family mediator) (list s-subject s-predicate) (compute-spoc-hex-id s-object))))
          (when column
            (funcall continuation subject predicate object context (column-name column))))
        
        :sp--                           ; retrieve objects for subject and predicates
        (flet ((do-constituents (object-sha1 object)
                 (funcall continuation subject predicate object context object-sha1)))
          (declare (dynamic-extent #'do-constituents))
          (map-resource-family #'do-constituents (store-resources-family mediator) s-subject s-predicate))
        
        :s-o-                           ; interate over subject row's supercolumns/columns filtering for object
        (flet ((do-constituents (predicate object-sha1 test-object)
                 (when (equal object test-object)
                   (funcall continuation subject predicate object context object-sha1))))
          (declare (dynamic-extent #'do-constituents))
          (map-resource-family #'do-constituents (store-resources-family mediator) s-subject nil))
        
        :s---                           ;interate over subject row's supercolumns/columns
        (flet ((do-constituents (predicate object-sha1 object)
                 (funcall continuation subject predicate object context object-sha1)))
          (declare (dynamic-extent #'do-constituents))
          (map-resource-family #'do-constituents (store-resources-family mediator) s-subject nil))
        
        :-po-                         ; retrieve p.o across contexts
        (flet ((do-key-slice (key-slice)
                 (let ((subject (model-value mediator (keyslice-key key-slice))))
                   (loop for cosc = (key-slice-columns key-slice)
                         for sc = (columnorsupercolumn-super-column cosc)
                         do (loop for column in (supercolumn-columns sc)
                                  when (equal object (model-value mediator (column-value column)))
                                  do (funcall continuation subject predicate object (column-value column)))))))
          (declare (dynamic-extent #'do-key-slice))
          (map-range-slices #'do-key-slice mediator :column-family (column-family-name (store-resources-family mediator))
                                           :start-key "" :finish-key "" :super-column s-predicate))
        
        :-p--                           ; iterate over all rows and filtered for predicate
        (flet ((do-key-slice (key-slice)
                 (let ((subject (model-value mediator (keyslice-key key-slice))))
                   (loop for cosc = (key-slice-columns key-slice)
                         for sc = (columnorsupercolumn-super-column cosc)
                         do (loop for column in (supercolumn-columns sc)
                                  do (funcall continuation subject predicate
                                              (model-value mediator (column-value column))
                                              (column-value column)))))))
          (declare (dynamic-extent #'do-key-slice))
          (map-range-slices #'do-key-slice mediator :column-family (column-family-name (store-resources-family mediator))
                                           :start-key "" :finish-key "" :super-column s-predicate))
        
        :--o-                           ; retrieve all o across all contexts
        (flet ((do-key-slice (key-slice)
                 (let ((subject (model-value mediator (keyslice-key key-slice))))
                   (loop for cosc = (key-slice-columns key-slice)
                         for sc = (columnorsupercolumn-super-column cosc)
                         for predicate = (model-value mediator (supercolumn-name sc))
                         do (loop for column in (supercolumn-columns sc)
                                  when (equal object (model-value mediator (column-value column)))
                                  do (funcall continuation subject predicate object (column-value column)))))))
          (declare (dynamic-extent #'do-key-slice))
          (map-range-slices #'do-key-slice mediator :column-family (column-family-name (store-resources-family mediator))
                                           :start-key "" :finish-key ""))
        
        :----                         ; retrieve all statements
        (flet ((do-key-slice (key-slice)
                 (let ((subject (model-value mediator (keyslice-key key-slice))))
                   (loop for cosc = (key-slice-columns key-slice)
                         for sc = (columnorsupercolumn-super-column cosc)
                         for predicate = (model-value mediator (supercolumn-name sc))
                         do (loop for column in (supercolumn-columns sc)
                                  do (funcall continuation subject predicate
                                              (model-value mediator (column-value column))
                                              (column-value column)))))))
          (declare (dynamic-extent #'do-key-slice))
          (map-range-slices #'do-key-slice mediator :column-family (column-family-name (store-resources-family mediator))
                                           :start-key "" :finish-key "")))

      (cassandra_2.1.0:notfoundexception (c) (declare (ignore c)) nil))))


(defun delete-statement.rb (mediator subject predicate object context)
  (declare (ignore context))
  (let* ((s-subject (repository-value mediator subject))
         (s-predicate (repository-value mediator predicate))
         (s-object (repository-value mediator object))
         (index-cf (store-index-family mediator))
         (resources-cf (store-resources-family mediator))
         (cache-cf (store-cache-family mediator)))
    (flet ((unindex-predicate ()
             (let ((predicate-id (compute-spoc-hex-id s-predicate)))
               (dsc:set-attributes index-cf (list predicate-id :info) (compute-spoc-hex-id s-predicate) nil)
               (dsc:set-attributes index-cf (list predicate-id :ps) (compute-spoc-hex-id s-subject) nil)))
           (unindex-object ()
             (let ((object-id (compute-spoc-hex-id s-object)))
               (dsc:set-attributes index-cf (list object-id :info) (compute-spoc-hex-id s-object) nil)
               (dsc:set-attributes index-cf (list object-id :os) (compute-spoc-hex-id s-subject) nil)
               (dsc:set-attributes index-cf (list object-id :op) (compute-spoc-hex-id s-predicate) nil))))

      (when cache-cf
        (dsc:set-attribute cache-cf +default-cache-graph+
                           (compute-spoc-sha1-id (binary (format nil "~/n3_format/"(list subject predicate object))) nil nil nil)
                           nil))
      (when index-cf
        (unindex-predicate)
        (unindex-object))
      (dsc:set-attribute resources-cf (list s-subject s-predicate) (compute-spoc-hex-id s-object) nil))))



#|

(defparameter *c-location*
  ;; remote
  ;; #u"thrift://ec2-174-129-66-148.compute-1.amazonaws.com:9160"
  ;; local
  #u"thrift://127.0.0.1:9160"
  "A cassandra service location - either the local one or a remote service 
 - always a 'thrift' uri.")

(defparameter *rdfrb* (client *c-location* :name "RDF" :protocol 'cassandra-rdfrb-index-mediator))

(add-statement.rb *rdfrb* "http://rdf.rubyforge.org/" "http://www.w3.org/1999/02/22-rdf-syntax-ns#type" "http://usefulinc.com/ns/doap#Project" "2010-07-20")
(add-statement.rb *rdfrb* "http://rdf.rubyforge.org/" "http://usefulinc.com/ns/doap#developer" "http://ar.to/#self" "2010-07-30")
(add-statement.rb *rdfrb* "http://rdf.rubyforge.org/" "http://usefulinc.com/ns/doap#developer" "http://bhuga.net/#ben" "2010-07-30")

(add-statement.rb *rdfrb* "http://ar.to/#self" "http://www.w3.org/1999/02/22-rdf-syntax-ns#type" "http://xmlns.com/foaf/0.1/Person" "2010-07-30")
(add-statement.rb *rdfrb* "http://ar.to/#self" "http://xmlns.com/foaf/0.1/name" "Arto Bendiken" "2010-07-30")

(add-statement.rb *rdfrb* "http://bhuga.net/#ben" "http://www.w3.org/1999/02/22-rdf-syntax-ns#type" "http://xmlns.com/foaf/0.1/Person" "2010-07-30")
(add-statement.rb *rdfrb* "http://bhuga.net/#ben" "http://xmlns.com/foaf/0.1/name" "Ben Lavender" "2010-07-30")

(graph-keyspace *rdfrb* "READMES/rdfrb.dot")
|#

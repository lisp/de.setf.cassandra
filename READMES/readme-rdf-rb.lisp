;;; -*- Mode: lisp; Syntax: ansi-common-lisp; Base: 10; Package: de.setf.cassandra; -*-


;;; this illustrates an rdf store structure which uses a RDF.rb keyspace
;;; - an spoc id index for exhaustive iteration
;;; - one each s, p, o, and c index for respective iteration
;;; - half of the two level indices for direct 2/4 and 3/4 retrieval
;;;
;;; see readme-columns for the load/connection steps
;;; see readme-rdf for utility operators

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
    :documentation "a cache column family.")
   (version-map
    :initform '(("2.1.0" . cassandra-rdfrb-index-mediator_2.1.0)
                ("8.3.0" . cassandra-rdfrb-index-mediator_8.3.0))
    :allocation :class))

  (:default-initargs :resources-family "Resources" :cache-family "Cache" :index-family "Index")
  (:documentation "A keyspace of the form used by an RDF mediator."))


(defclass cassandra-rdfrb-index-mediator_2.1.0 (cassandra-rdfrb-index-mediator cassandra_2.1.0:keyspace)
  ())


(defclass cassandra-rdfrb-index-mediator_8.3.0 (cassandra-rdfrb-index-mediator cassandra_8.3.0:keyspace)
  ())

            
(defmethod initialize-instance :after ((instance cassandra-spoc-index-mediator)
                                       &key resources-family index-family cache-family)
  (set-keyspace-column-family instance resources-family 'resources-family :class 'super-column-family :required t)
  (set-keyspace-column-family instance index-family 'index-family :class 'super-column-family :required nil)
  (set-keyspace-column-family instance cache-family 'cache-family :class 'super-column-family :required nil))


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
                           (compute-spoc-sha1-id (binary (format nil "~/n3:format/"(list subject predicate object))) nil nil nil)
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
                                        (model-value mediator (column-value column)))))))
         (map-spoc-columns (op index key column-names)
           (flet ((cmv (column) (model-value mediator (column-value column))))
             (declare (dynamic-extent #'mv))
             (loop for column-id.c in (dsc:get-columns index key)
                   for s-columns = (dsc:get-columns (store-spoc-index mediator)
                                                    (column-name column-id.c)
                                                    :column-names column-names)
                   do (apply op 
                             (model-value mediator (column-value column-id.c))
                             (column-name column-id.c)
                             (map-into s-columns #'cmv s-columns))))))
    
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
                           (compute-spoc-sha1-id (binary (format nil "~/n3:format/"(list subject predicate object))) nil nil nil)
                           nil))
      (when index-cf
        (unindex-predicate)
        (unindex-object))
      (dsc:set-attribute resources-cf (list s-subject s-predicate) (compute-spoc-hex-id s-object) nil))))



#|

(defun test-map (test)
  (destructuring-bind (pattern &rest expected-results) test
    (let ((results ()))
      (block :map
        (handler-bind ((error (lambda (c) (push c results) (break "error: ~a" c) (return-from :map nil))))
          (apply #'map-statements *spoc* #'(lambda (subject predicate object context id)
                                             (declare (ignore id))
                                             (push (list subject predicate object context) results))
                 pattern)))
      (format *trace-output* "~&~a ~:[ok~;failed: ~:*~s~]"
              pattern
              (set-exclusive-or results expected-results :test #'equalp)))))

(defparameter *c-location*
  ;; remote
  ;; #u"thrift://ec2-174-129-66-148.compute-1.amazonaws.com:9160"
  ;; local
  #u"thrift://127.0.0.1:9160"
  "A cassandra service location - either the local one or a remote service 
 - always a 'thrift' uri.")

(defparameter *spoc* (client *c-location* :name "SPOC" :protocol 'cassandra-spoc-index-mediator))

(add-statement *spoc* "vanille" "scoops" "100" "2010-07-28")
(add-statement *spoc* "vanille" "scoops" "10" "2010-07-27")
(add-statement *spoc* "cheesecake" "slices" "2" "2010-07-28")
(add-statement *spoc* "cheesecake" "cheesecake" "20" "2010-07-29")


(add-statement *spoc* "subject" "predicate" "object" "context2")
(delete-statement *spoc* "subject" "predicate" "object" "context")
(map nil #'test-map
     '(((nil nil nil nil) .                 (("cheesecake" "slices" "20" "2010-07-29") ("vanille" "scoops" "100" "2010-07-28")
                                             ("cheesecake" "slices" "2" "2010-07-28") ("vanille" "scoops" "10" "2010-07-27")))
       (("vanille" nil nil nil) .           (("vanille" "scoops" "100" "2010-07-28") ("vanille" "scoops" "10" "2010-07-27")))
       ((nil "scoops" nil nil) .            (("vanille" "scoops" "100" "2010-07-28") ("vanille" "scoops" "10" "2010-07-27")))
       (("vanille" "scoops" nil nil) .      (("vanille" "scoops" "10" "2010-07-27") ("vanille" "scoops" "100" "2010-07-28")))
       ((nil nil "10" nil) .                (("vanille" "scoops" "10" "2010-07-27")))
       ((nil nil "100" nil).                (("vanille" "scoops" "100" "2010-07-28")))
       (("vanille" nil "10" nil) .          (("vanille" "scoops" "10" "2010-07-27")))
       (("vanille" nil "100" nil) .         (("vanille" "scoops" "100" "2010-07-28")))
       ((nil "scoops" "10" nil) .           (("vanille" "scoops" "10" "2010-07-27")))
       ((nil "scoops" "100" nil) .          (("vanille" "scoops" "100" "2010-07-28")))
       (("vanille" "scoops" "10" nil) .     (("vanille" "scoops" "10" "2010-07-27")))
       (("vanille" "scoops" "100" nil) .    (("vanille" "scoops" "100" "2010-07-28")))
       ((nil nil nil "2010-07-27") .        (("vanille" "scoops" "10" "2010-07-27")))
       ((nil nil nil "2010-07-28") .        (("vanille" "scoops" "100" "2010-07-28") ("cheesecake" "slices" "2" "2010-07-28")))
       (("vanille" nil nil "2010-07-28") .  (("vanille" "scoops" "100" "2010-07-28")))
       (("vanille" "scoops" nil "2010-07-27") . (("vanille" "scoops" "10" "2010-07-27")))
       ((nil nil "10" "2010-07-27") .       (("vanille" "scoops" "10" "2010-07-27")))
       ((nil nil "100" "2010-07-28") .      (("vanille" "scoops" "100" "2010-07-28")))
       (("vanille" nil "10" "2010-07-27") . (("vanille" "scoops" "10" "2010-07-27")))
       (("vanille" nil "100" "2010-07-27"))     ; note scoop count
       ((nil "scoops" "10" "2010-07-27") .  (("vanille" "scoops" "10" "2010-07-27")))
       ((nil "scoops" "100" "2010-07-27"))
       ((nil "scoops" "100" "2010-07-28") . (("vanille" "scoops" "100" "2010-07-28")))
       (("vanille" "scoops" "10" "2010-07-27") . (("vanille" "scoops" "10" "2010-07-27")))
       (("vanille" "scoops" "100" "2010-07-28") . (("vanille" "scoops" "100" "2010-07-28")))))


(defun test-spoc-case (mediator subject predicate object context)
  (spoc-case (mediator (s-subject s-predicate s-object s-context) subject predicate object context)
    :spoc :spoc
    :spo- :spo-
    :sp-c :sp-c
    :sp-- :sp--
    :s-oc :s-oc
    :s-o- :s-o-
    :s--c :s--c
    :s--- :s---
    :-poc :-poc
    :-po- :-po-
    :-p-c :-p-c
    :-p-- :-p--
    :--oc :--oc
    :--o- :--o-
    :---c :---c
    :---- :----))

(test-spoc-case t 1 nil 2 nil)
|#

;;; -*- Mode: lisp; Syntax: ansi-common-lisp; Base: 10; Package: de.setf.cassandra; -*-

;;; see readme-cassandra.lisp to build...

(in-package :de.setf.cassandra)

;;;  see readme-cassandra for the load/connection steps
;;;

;;; work with SPOC keyspace

(defclass cassandra-spoc-index-mediator (keyspace)
  ((c-index
    :reader store-c-index
    :documentation "a column family index: (context . (spoc-id . context)* )*")
   (o-index
    :reader store-o-index
    :documentation "a column family index: (object . (spoc-id . context)* )*")
   (p-index
    :reader store-p-index
    :documentation "a column family index: (predicate . (spoc-id . context)* )*")
   (s-index
    :reader store-s-index
    :documentation "a column family index: (subject . (spoc-id . context)* )*")
   (cos-index
    :reader store-cos-index
    :documentation "a super-column family index: ([..object.context] . (subject . (spoc-id . predicate)* )* )*")
   (cpo-index
    :reader store-cpo-index
    :documentation "a super-column family index: ([.predicate..context] . (object . (spoc-id . subject)* )* )*")
   (cso-index
    :reader store-cso-index
    :documentation "a super-column family index: ([subject...context] . (object . (spoc-id . predicate)* )* )*")
   (csp-index
    :reader store-csp-index
    :documentation "a super-column family index: ([subject...context] . (predicate . (spoc-id . object)* )* )*")
   (poc-index
    :reader store-poc-index
    :documentation "a super-column family index: ([.predicate.object.] . (context . (spoc-id . subject)* )* )*")
   (soc-index
    :reader store-soc-index
    :documentation "a super-column family index: ([subject..object.] . (context . (spoc-id . predicate)* )* )*")
   (spc-index
    :reader store-spc-index
    :documentation "a super-column family index: ([subject.predicate..] . (context . (spoc-id . subject)* )* )*")
   (spo-index
    :reader store-spo-index
    :documentation "a super-column family index: ([subject.predicate..] . (object . (spoc-id . context)* )* )*")
   (spoc-index
    :reader store-spoc-index
    :documentation "a column family index:
     ([subject.predicate.object.context] . ((:subject . 's') (:predicate . 'p') (:object . 'o') (:context . 'c') ...))
     It enumerates the respective statement's constituents.")
   (version-map
    :initform '(("2.1.0" . cassandra-spoc-index-mediator_2.1.0)
                ("8.3.0" . cassandra-spoc-index-mediator_8.3.0))
    :allocation :class))

  (:documentation "A keyspace of the form used by an RDF mediator."))

(defclass cassandra-spoc-index-mediator_2.1.0 (cassandra-spoc-index-mediator cassandra_2.1.0:keyspace)
  ())

(defclass cassandra-spoc-index-mediator_8.3.0 (cassandra-spoc-index-mediator cassandra_8.3.0:keyspace)
  ())


(defmethod initialize-instance :after ((instance cassandra-spoc-index-mediator)
                                       &key (spoc-index nil spoc-index-s)
                                       (c-index nil c-index-s)(o-index nil o-index-s)
                                       (p-index nil p-index-s) (s-index nil s-index-s)
                                       (cos-index nil cos-index-s) (cpo-index nil cpo-index-s)
                                       (cso-index nil cso-index-s) (csp-index nil csp-index-s)
                                       (poc-index nil poc-index-s) (soc-index nil soc-index-s)
                                       (spc-index nil spc-index-s) (spo-index nil spo-index-s))
  (macrolet ((set-index (slot-name name class &optional (required nil))
               `(setf (slot-value instance ',slot-name)
                      (or (etypecase ,slot-name
                            (null (unless ,(intern (concatenate 'string (string slot-name) (string :-s)) (symbol-package slot-name))
                                    (make-instance ',class :name ,name :keyspace instance)))
                            (string (make-instance ',class :name ,slot-name :keyspace instance))
                            (column-family ,slot-name))
                          (when ,required (error "Index not present: ~s." ',slot-name))))))
    (set-index spoc-index "SPOCIndex" standard-column-family t)
    (set-index c-index "CIndex" standard-column-family t)
    (set-index o-index "OIndex" standard-column-family t)
    (set-index p-index "PIndex" standard-column-family t)
    (set-index s-index "SIndex" standard-column-family t)
    (set-index cos-index "COSIndex" super-column-family t)
    (set-index cpo-index "CPOIndex" super-column-family t)
    (set-index cso-index "CSOIndex" super-column-family t)
    (set-index csp-index "CSPIndex" super-column-family t)
    (set-index poc-index "POCIndex" super-column-family)
    (set-index soc-index "SOCIndex" super-column-family)
    (set-index spc-index "SPCIndex" super-column-family)
    (set-index spo-index "SPOIndex" super-column-family)))

(define-condition duplicate-statement (simple-error)
  ((store :initarg :store :reader error-store)
   (statement :initarg :statement :reader error-statement))
  (:report (lambda (c stream)
             (format stream "Statement exists in repository: ~a, ~s."
                     (error-store c) (error-statement c)))))



;;; for a g5x32bit md5 / sha1 == 2.6 / 5.3
(defun compute-spoc-md5-id (subject predicate object context)
  (let* ((p-pos (length subject))
         (o-pos (+ p-pos (length predicate)))
         (c-pos (+ o-pos (length object)))
         (length (+ c-pos (length context)))
         (buffer (make-array length :element-type '(unsigned-byte 8))))
    (declare (type fixnum length)
             (type (simple-array (unsigned-byte 8) (*)) buffer)
             (dynamic-extent buffer))
    (replace buffer subject)
    (replace buffer predicate :start1 p-pos)
    (replace buffer object :start1 o-pos)
    (replace buffer context :start1 c-pos)
    (ironclad:digest-sequence 'crypto:md5 buffer)))

(defun compute-spoc-sha1-id (subject predicate object context)
  (let* ((p-pos (length subject))
         (o-pos (+ p-pos (length predicate)))
         (c-pos (+ o-pos (length object)))
         (length (+ c-pos (length context)))
         (buffer (make-array length :element-type '(unsigned-byte 8))))
    (declare (type fixnum length)
             (type (simple-array (unsigned-byte 8) (*)) buffer)
             (dynamic-extent buffer))
    (replace buffer subject)
    (replace buffer predicate :start1 p-pos)
    (replace buffer object :start1 o-pos)
    (replace buffer context :start1 c-pos)
    (ironclad:digest-sequence 'ironclad:sha1 buffer)))

(defun compute-spoc-id (subject predicate object context)
  (compute-spoc-md5-id subject predicate object context))

(defgeneric repository-value (mediator object)
  (:method ((mediator t) (object t))
    (let* ((s (write-to-string object))
           (v (make-array (length s) :element-type '(unsigned-byte 8))))
      (map-into v #'char-code s))))

(defgeneric model-value (mediator object)
  (:method ((mediator t) (object vector))
    (let ((s (make-array (length object) :element-type 'character)))
      (read-from-string (map-into s #'code-char object)))))
;;; (model-value t (repository-value t "asdf"))

;;;
;;; adding statements

(defun add-statement (mediator subject predicate object context)
  
  (let* ((s-subject (repository-value mediator subject))
         (s-predicate (repository-value mediator predicate))
         (s-object (repository-value mediator object))
         (s-context (repository-value mediator context))
         (spoc-id (compute-spoc-id s-subject s-predicate s-object s-context)))
    (labels ((store-supercolumn-index (index key sc-key name value)
               (when index (dsc:set-attribute index (list key sc-key) name value)))
             (store-column-index (index key name value)
               (when index (dsc:set-attribute index key name value)))
             (store-statement ()
               (dsc:set-attributes (store-spoc-index mediator) spoc-id
                                   :subject s-subject :predicate s-predicate :object s-object
                                   :context s-context)
               
               (store-column-index (store-c-index mediator) s-context spoc-id s-context)
               (store-column-index (store-o-index mediator) s-object spoc-id s-context)
               (store-column-index (store-p-index mediator) s-predicate spoc-id s-context)
               (store-column-index (store-s-index mediator) s-subject spoc-id s-context)
               
               (store-supercolumn-index (store-cos-index mediator) (compute-spoc-id nil nil s-object s-context) s-subject spoc-id s-predicate)
               (store-supercolumn-index (store-cpo-index mediator) (compute-spoc-id nil s-predicate nil  s-context) s-object spoc-id s-subject)
               (store-supercolumn-index (store-cso-index mediator) (compute-spoc-id s-subject nil nil s-context) s-object spoc-id s-predicate)
               (store-supercolumn-index (store-csp-index mediator) (compute-spoc-id s-subject nil nil s-context) s-predicate spoc-id s-object)
               (store-supercolumn-index (store-poc-index mediator) (compute-spoc-id nil s-predicate s-object nil) s-context spoc-id s-subject)
               (store-supercolumn-index (store-soc-index mediator) (compute-spoc-id s-subject nil s-object nil) s-context spoc-id s-predicate)
               (store-supercolumn-index (store-spc-index mediator) (compute-spoc-id s-subject s-predicate nil nil) s-context spoc-id s-object)
               (store-supercolumn-index (store-spo-index mediator) (compute-spoc-id s-subject s-predicate nil nil) s-object spoc-id s-context)))

      (handler-case (progn (dsc:get-attribute (store-spoc-index mediator) spoc-id :context)
                           ;; if this completes, the statement is already present in the graph
                           (signal 'duplicate-statement :statement (list subject predicate object context) :store mediator)
                           ;; if the signal is not handled, just return nil
                           nil)
        ;; if there was none, compute the id and add the properties
        ((or cassandra_2.1.0:notfoundexception cassandra_8.3.0:notfoundexception)  (c)
         (declare (ignore c))
         (store-statement)
         ;; return the new id
         spoc-id)))))


;;; (defparameter *spoc* (client *c-location* :name "SPOC" :protocol 'cassandra-spoc-index-mediator))

;;; (add-statement *spoc* "subject" "predicate" "object" "context")
;;; (add-statement *spoc* "subject" "is" "mysterious" "context")
;;; (add-statement *spoc* "subject" "is" "wonderful" "context")

(defmacro spoc-case ((mediator (sub pre obj con) subject predicate object context)
                     &key spoc spo (spo- spo) spc (sp-c spc) sp (sp-- sp)
                     soc (s-oc soc) so (s-o- so) sc (s--c sc) s (s--- s)
                     poc (-poc poc) po (-po- po) pc (-p-c pc) p (-p-- p)
                     oc (--oc oc) o (--o- o) c (---c c) all (---- all))
  `(let ((,sub (repository-value ,mediator ,subject))
         (,pre (repository-value ,mediator ,predicate))
         (,obj (repository-value ,mediator ,object))
         (,con (repository-value ,mediator ,context)))
     (ecase (logior (if ,sub #b1000 0) (if ,pre #b0100 0) (if ,obj #b0010 0) (if ,con #b0001 0))
       (#b1111 ,spoc)
       (#b1110 ,spo-)
       (#b1101 ,sp-c)
       (#b1100 ,sp--)
       (#b1011 ,s-oc)
       (#b1010 ,s-o-)
       (#b1001 ,s--c)
       (#b1000 ,s---)
       (#b0111 ,-poc)
       (#b0110 ,-po-)
       (#b0101 ,-p-c)
       (#b0100 ,-p--)
       (#b0011 ,--oc)
       (#b0010 ,--o-)
       (#b0001 ,---c)
       (#b0000 ,----))))


(defun map-statements (mediator continuation subject predicate object context)
  ;; indices
  ;; spoc : ( [s.p.o.c] . statement-attribute* )
  ;; spo  : ( [s.p] . ( [o] . (c . spoc-id)* )* )
  ;; sp c : ( [s.p] . ( [c] . (o . spoc-id)* )* )
  (flet ((map-supercolumn-family (op index row-key &optional super-column-key)
           ;; given the terms for just the super-column row, iterate over all identifier supercolumns
           (if super-column-key
             (loop for column in (dsc:get-columns index (list row-key super-column-key))
                   do (funcall continuation
                               (model-value mediator (column-name column))
                               (model-value mediator (column-value column))))
             (loop for (key . columns) in (dsc:get-columns index row-key)
                   for value = (model-value mediator key)
                   do (loop for column in columns
                            do (funcall op
                                        value
                                        (model-value mediator (column-name column))
                                        (model-value mediator (column-value column)))))))
         (map-spoc-columns (op index key column-names)
           (flet ((mv (s-value) (model-value mediator s-value)))
             (declare (dynamic-extent #'mv))
             (loop for column-id.c in (dsc:get-columns index key)
                   for s-constituents = (dsc:get-attributes (store-spoc-index mediator)
                                                            (column-name column-id.c)
                                                            :column-names column-names)
                   do (apply op 
                             (model-value mediator (column-value column-id.c))
                             (model-value mediator (column-name column-id.c))
                             (map-into s-constituents #'mv s-constituents))))))
    
    (handler-case 
      (spoc-case (mediator (s-subject s-predicate s-object s-context) subject predicate object context)
        :spoc                           ; check for a context-specific statement
        (let ((spoc-id (compute-spoc-id s-subject s-predicate s-object s-context)))
          (when (dsc:get-attributes (store-spoc-index mediator) spoc-id :count 0)
            ;; if it completes with an equivalent object
            (funcall continuation subject predicate object context spoc-id)))
        
        :spo-                           ; look for all equivalent statements across contexts
        (flet ((do-constituents (context id)
                 (funcall continuation subject predicate object context id)))
          (declare (dynamic-extent #'do-constituents))
          (map-supercolumn-family #'do-constituents (store-cpo-index mediator) (compute-spoc-id s-subject s-predicate nil nil) s-object))
        
        :sp-c                           ; retrieve for subject and predicate in the context
        (let ((spc-index (store-spc-index mediator))
              (csp-index (store-csp-index mediator)))
          (flet ((do-constituents (subject id)
                   (funcall continuation subject predicate object context id)))
            (declare (dynamic-extent #'do-constituents))
            (cond (spc-index
                   (map-supercolumn-family #'do-constituents spc-index (compute-spoc-id s-subject s-predicate nil nil) s-context))
                  (csp-index
                   (map-supercolumn-family #'do-constituents csp-index (compute-spoc-id s-subject nil nil s-context) s-predicate))
                  (t
                   (flet ((do-constituents (test-context id test-subject test-predicate)
                            (when (and (equal subject test-subject) (equal predicate test-predicate) (equal context test-context))
                              (funcall continuation subject predicate object context id))))
                     (declare (dynamic-extent #'do-constituents))
                     (map-spoc-columns #'do-constituents (store-o-index mediator) s-subject '(:subject :predicate)))))))
        
        :sp--                           ; look for subject and predicates across contexts
        (flet ((do-constituents (object context id)
                 (funcall continuation subject predicate object context id)))
          (declare (dynamic-extent #'do-constituents))
          (map-supercolumn-family #'do-constituents (store-spo-index mediator) (compute-spoc-id s-subject s-predicate nil nil)))
        
        :s-oc                          ; look for s.o in the context
        (let ((soc-index (store-soc-index mediator))
              (cso-index (store-cso-index mediator)))
          (flet ((do-constituents (predicate id)
                   (funcall continuation subject predicate object context id)))
            (declare (dynamic-extent #'do-constituents))
            (cond (soc-index
                   (map-supercolumn-family #'do-constituents soc-index (compute-spoc-id s-subject nil s-object nil) s-context))
                  (cso-index
                   (map-supercolumn-family #'do-constituents cso-index (compute-spoc-id s-subject nil nil s-context) s-object))
                  (t
                   (flet ((do-constituents (test-context id test-subject test-object)
                            (when (and (equal subject test-subject) (equal object test-object) (equal context test-context))
                              (funcall continuation subject predicate object context id))))
                     (declare (dynamic-extent #'do-constituents))
                     (map-spoc-columns #'do-constituents (store-p-index mediator) s-subject '(:subject :object)))))))
        
        :s-o-                           ; retrieve s.o across contexts
        (flet ((do-constituents (context predicate id)
                 (funcall continuation subject predicate object context id)))
          (declare (dynamic-extent #'do-constituents))
          (map-supercolumn-family #'do-constituents (store-soc-index mediator) (compute-spoc-id s-subject nil s-object nil)))
        
        :s--c                           ; retrieve all s in the context
        (flet ((do-constituents (object predicate id)
                 (funcall continuation subject predicate object context id)))
          (declare (dynamic-extent #'do-constituents))
          (map-supercolumn-family #'do-constituents (store-cso-index mediator) (compute-spoc-id s-subject nil nil s-context)))
        
        :s---                           ; retrieve all s across all contexts
        (flet ((do-constituents (context id predicate object)
                 (funcall continuation subject predicate object context id)))
          (declare (dynamic-extent #'do-constituents))
          (map-spoc-columns #'do-constituents (store-s-index mediator) s-subject '(:predicate :object)))
        
        :-poc                          ; look for p.o in the context
        (let ((poc-index (store-poc-index mediator))
              (cpo-index (store-cpo-index mediator)))
          (flet ((do-constituents (subject id)
                   (funcall continuation subject predicate object context id)))
            (declare (dynamic-extent #'do-constituents))
            (cond (poc-index
                   (map-supercolumn-family #'do-constituents poc-index (compute-spoc-id nil s-predicate s-object nil) s-context))
                  (cpo-index
                   (map-supercolumn-family #'do-constituents cpo-index (compute-spoc-id nil s-predicate nil s-context) s-object))
                  (t
                   (flet ((do-constituents (test-context id test-predicate test-object)
                            (when (and (equal predicate test-predicate) (equal object test-object) (equal context test-context))
                              (funcall continuation subject predicate object context id))))
                     (declare (dynamic-extent #'do-constituents))
                     (map-spoc-columns #'do-constituents (store-s-index mediator) s-subject '(:predicate :object)))))))
        
        :-po-                         ; retrieve p.o across contexts
        (flet ((do-constituents (context subject id)
                 (funcall continuation subject predicate object context id)))
          (declare (dynamic-extent #'do-constituents))
          (map-supercolumn-family #'do-constituents (store-poc-index mediator) (compute-spoc-id nil s-predicate s-object nil)))
        
        :-p-c                         ; retrieve all p in the context
        (flet ((do-constituents (object subject id)
                 (funcall continuation subject predicate object context id)))
          (declare (dynamic-extent #'do-constituents))
          (map-supercolumn-family #'do-constituents (store-cpo-index mediator) (compute-spoc-id nil s-predicate nil s-context)))
        
        :-p--                           ; retrieve all p across all contexts
        (flet ((do-constituents (context id subject object)
                 (funcall continuation subject predicate object context id)))
          (declare (dynamic-extent #'do-constituents))
          (map-spoc-columns #'do-constituents (store-p-index mediator) s-predicate '(:subject :object)))
        
        :--oc                         ; retrieve all o in the context
        (flet ((do-constituents (subject predicate id)
                 (funcall continuation subject predicate object context id)))
          (declare (dynamic-extent #'do-constituents))
          (map-supercolumn-family #'do-constituents (store-cos-index mediator) (compute-spoc-id nil nil s-object s-context)))
        
        :--o-                           ; retrieve all o across all contexts
        (flet ((do-constituents (context id subject predicate)
                 (funcall continuation subject predicate object context id)))
          (declare (dynamic-extent #'do-constituents))
          (map-spoc-columns #'do-constituents (store-o-index mediator) s-object '(:subject :predicate)))
        
        :---c                         ; retrieve all from a context
        (flet ((do-constituents (context id subject predicate object)
                 (funcall continuation subject predicate object context id)))
          (declare (dynamic-extent #'do-constituents))
          (map-spoc-columns #'do-constituents (store-c-index mediator) s-context '(:subject :predicate :object)))
        
        :----                         ; retrieve all statements
        (multiple-value-bind (s-subject s-predicate s-object s-context s-id)
                             (dsc:get-attributes (store-spoc-index mediator) nil :column-names '(:subject :predicate :object :context :id))
          (funcall continuation (model-value mediator s-subject)
                   (model-value mediator s-predicate)
                   (model-value mediator s-object)
                   (model-value mediator s-context)
                   (model-value mediator s-id))))
      (cassandra_2.1.0:notfoundexception (c) (declare (ignore c)) nil))))
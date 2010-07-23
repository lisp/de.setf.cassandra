;;; -*- Mode: lisp; Syntax: ansi-common-lisp; Base: 10; Package: cl-user; -*-

(in-package :cl-user)

#+(or ccl sbcl sbcl)
(load "/development/source/library/build-init.lisp")

;;; ! first, select the api version in the cassandra system definition
;;; as only one should be loaded at a time.
(let ((*compile-verbose* nil))
  (asdf:load-system :de.setf.cassandra))

(in-package :de.setf.cassandra)

(defparameter *c-location*
  ;; remote
  ;; #u"thrift://ec2-174-129-66-148.compute-1.amazonaws.com:9160"
  ;; local
  #u"thrift://127.0.0.1:9160"
  "A cassandra service location - either the local one or a remote service 
 - always a 'thrift' uri.")

(defparameter *c* (thrift:client *c-location*))


(cassandra:describe-keyspaces *c*)
;; => ("Keyspace1" "system")

(cassandra:describe-cluster-name *c*)
;; =>"Test Cluster"

(cassandra:describe-version *c*)
;; => "2.1.0"

(close *c*)

(loop for space in (cassandra:describe-keyspaces *c*)
      collect (loop for key being each hash-key of (cassandra:describe-keyspace *c* space)
                    using (hash-value value)
                    collect (cons key
                                  (loop for key being each hash-key of value
                                        using (hash-value value)
                                        collect (cons key value)))))


(defgeneric describe-cassandra (location &optional stream)
  (:documentation "Print the first-order store metadata for a cassandra LOCATION.")
  
  (:method ((location puri:uri) &optional (stream *standard-output*))
    (thrift:with-client (cassandra location)
      (describe-cassandra cassandra stream)))
  
  (:method ((cassandra thrift:binary-protocol) &optional (stream *standard-output*))
    (let* ((keyspace-names (cassandra:describe-keyspaces cassandra))
           (cluster (cassandra:describe-cluster-name cassandra))
           (version (cassandra:describe-version cassandra))
           (keyspace-descriptions (loop for space in keyspace-names
                                        collect (cons space
                                                      (loop for key being each hash-key
                                                            of (cassandra:describe-keyspace cassandra space)
                                                            using (hash-value value)
                                                            collect (cons key
                                                                          (loop for key being each hash-key of value
                                                                                using (hash-value value)
                                                                                collect (cons key value))))))))
      (format stream "~&connection to : ~a" cassandra)
      (format stream "~&version : ~a" version)
      (format stream "~&cluster : ~a" cluster)
      (format stream "~&keyspaces~{~{~%~%space: ~a~@{~%  ~{~a :~@{~20t~:w~^~%~}~}~}~}~}"
              keyspace-descriptions))))

;;; (describe-cassandra *c-location*)

;;; work with Keyspace2

(defparameter *ks* (keyspace *c-location* :name "Keyspace1"))
(defparameter *standard2* (make-instance 'column-family :keyspace *ks* :name "Standard2"))

;;; (describe-cassandra *ks*)

;;; simple, column-based access

(insert *standard2* :key "user1" :column "one" :value "2")
(insert *standard2* :key "user2" :column "one" :value "1")

(get *standard2* :key "user1" :column "one")


;;; in terms of attribute-value
;;; individual
(attribute-value *standard2* "user1" "one")

(princ (nth-value 1 (ignore-errors (attribute-value *standard2* "user" "one"))))

(dotimes (x 10)
  (setf (attribute-value *standard2* "user1" (format nil "~:r" x)) (princ-to-string x)))

(loop for x from 0 below 10
      for column-name = (format nil "~:r" x)
      collect (cons column-name (attribute-value *standard2* "user1" column-name)))

;;; and 'sliced'
(attribute-values *standard2* "user1")

(attribute-values *standard2* "user1" :start "ninth" :finish "second")

(attribute-values *standard2* "user1" :start "ninth" :count 2)

(setf (attribute-values *standard2* "user3" :column-names '("some" "little" "details"))
      '("come" "to" "light"))

(mapcar #'cassandra::column-value (attribute-values *standard2* "user3"))

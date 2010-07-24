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

;;; (describe-cassandra *ks*)

;;; simple column access

(defparameter *standard2* (make-instance 'column-family :keyspace *ks* :name "Standard2"))

(set-attribute *standard2* "user1" "one" "2")
(set-attribute *standard2* "user2" "one" "1")

(get *standard2* :key "user1" :column "one")


;;; in terms of attribute-value
;;; individual
(describe (get-attribute *standard2* "user1" "one"))

(princ (nth-value 1 (ignore-errors (get-attribute *standard2* "user" "one"))))

(dotimes (x 10)
  (set-attribute *standard2* "user1" (format nil "~:r" x) (princ-to-string x)))

(loop for x from 0 below 10
      for column-name = (format nil "~:r" x)
      collect (cons column-name (cassandra::column-value (get-attribute *standard2* "user1" column-name))))

;;; and 'sliced'
(mapcar #'cassandra::column-value (get-attributes *standard2* "user1"))

(mapcar #'cassandra::column-value (attribute-values *standard2* "user1" :start "ninth" :finish "second"))

(mapcar #'cassandra::column-value (attribute-values *standard2* "user1" :start "ninth" :count 2))

(set-attributes *standard2* "user3" "some" "little" "details" "come" "to" "light")

(mapcar #'cassandra::column-value (get-attributes *standard2* "user3"))


;;; super column access

(defparameter *super2* (make-instance 'super-column-family :keyspace *ks* :name "Super2"))

(set-attribute *super2* '("user1" "collection1") "first" "1")

(cassandra::column-value (get-attribute *super2* '("user1" "collection1") "first"))

(set-attribute *super2* '("user1" "collection2") "second" "2")


(mapcar #'cassandra::column-value (get-attributes *super2* '("user1" "collection1")))


(mapcar #'cassandra::column-value (get-attributes *super2* "user1"))
;;; => ("1" "2")

(set-attributes *super2* '("user1" "collection3") "some" "little" "details" "come" "to" "light")

(mapcar #'cassandra::column-value (get-attributes *super2* "user1"))
;;; => ("1" "2" "come" "little" "light")


(mapcar #'cassandra::column-value (get-attributes *super2* '("user1" "collection3")))
;;; => ("come" "little" "light")

;;; -*- Mode: lisp; Syntax: ansi-common-lisp; Base: 10; Package: cl-user; -*-

(in-package :cl-user)

(cond ((equal (machine-instance) "ip-10-251-122-82")
       (load "/development/source/library/build-init.lisp"))
      ((equal (machine-instance) "yoda")
       (load "/Development/Source/dev/Library/build-init.lisp"))
      (t
       (error "Unknown instance: ~s." (machine-instance))))

;;; nb: this expects the test storage configuration from the cassandra 0.6.4 release!
;;; nb.1: the 2.1.0 interface requires a string column value for insert
;;; a move to 0.7 requires moving from the .xml to the .yaml initialization and a one-time load
;;; of the schema via the jconsole interface. running from ec2 with 'ssh -Y' it starts in about 15 min.
;;; $ sudo apt-get install openjdk-6-jdk

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

(close *c*)

(defgeneric describe-cassandra (location &optional stream)
  (:documentation "Print the first-order store metadata for a cassandra LOCATION.")
  
  (:method ((location puri:uri) &optional (stream *standard-output*))
    (thrift:with-client (cassandra location)
      (describe-cassandra cassandra stream)))
  
  (:method ((cassandra thrift:binary-protocol) &optional (stream *standard-output*))
    (let* ((keyspace-names (cassandra_2.1.0:describe-keyspaces cassandra))
           (cluster (cassandra_2.1.0:describe-cluster-name cassandra))
           (version (cassandra_2.1.0:describe-version cassandra))
           (keyspace-descriptions (loop for space in keyspace-names
                                        collect (cons space (cassandra_2.1.0:describe-keyspace cassandra space)))))
      (format stream "~&connection to : ~a" cassandra)
      (format stream "~&version : ~a" version)
      (format stream "~&cluster : ~a" cluster)
      (format stream "~&keyspaces~{~{~%~%space: ~a~@{~%  ~{~a :~@{~20t~:w~^~%~}~}~}~}~}"
              keyspace-descriptions))))

(defgeneric describe-keyspace-columns (keyspace &optional stream)
  (:method ((keyspace keyspace) &optional (stream *standard-output*))
    (let ((description (loop for key being each hash-key
                             of (cassandra_2.1.0:describe-keyspace keyspace (keyspace-name keyspace))
                             using (hash-value value)
                             collect (cons key
                                           (loop for key being each hash-key of value
                                                 using (hash-value value)
                                                 collect (cons key value))))))
      (format stream "~&~{~%~%space: ~a~@{~%  ~{~a :~@{~20t~:w~^~%~}~}~}~}"
              description))))

;;; (describe-cassandra *c-location*)

;;; work with Keyspace2

(defparameter *ks* (keyspace *c-location* :name "Keyspace1"))

(keyspace-description *ks*)


;;;
;;; standard column access

(defparameter *standard2* (make-instance 'standard-column-family :keyspace *ks* :name "Standard2"))

(set-attribute *standard2* "user1" "one" "2")
(set-attribute *standard2* "user2" "one" "1")

;;; in terms of attribute-value
;;; individual
(describe (get-column *standard2* "user1" "one"))
(column-value-string (get-column *standard2* "user1" "one"))
(column-name-string (get-column *standard2* "user1" "one"))

(princ (nth-value 1 (ignore-errors (get-attribute *standard2* "user" "one"))))

(dotimes (x 10)
  (set-attribute *standard2* "user1" (format nil "~:r" x) (princ-to-string x)))

(loop for x from 0 below 10
      for column-name = (format nil "~:r" x)
      collect (cons column-name (get-attribute *standard2* "user1" column-name)))

;;; and 'sliced'
(get-attributes *standard2* "user1")

(get-attributes *standard2* "user1" :start "ninth" :finish "second")

(get-attributes *standard2* "user1" :start "ninth" :count 2)


(set-attributes *standard2* "user3" :we "for" :all "ice" :scream "cream")

(mapcar #'column-value-string (get-columns *standard2* "user3"))
;;; => ("ice" "cream" "for")

(set-attribute *standard2* "user3" :scream nil)

(mapcar #'column-value-string (get-columns *standard2* "user3"))
;;; => ("ice"  "for")


;;;
;;; super column access

(defparameter *super2* (make-instance 'super-column-family :keyspace *ks* :name "Super2"))

(set-attribute *super2* '("superuser1" "collection1") "first" "1")

(get-attribute *super2* '("superuser1" "collection1") "first")

(set-attribute *super2* '("superuser1" "collection2") "second" "0123456789")


(get-attributes *super2* '("user1" "collection1"))
;;; => nil

(get-attributes *super2* '("superuser1" "collection1"))
;;; => ((#(102 105 114 115 116) . #(49)))


(set-attributes *super2* '("icecream" "20100701") :vanille 100 :chocolade 2 :riesling-sorbet 900)
(set-attributes *super2* '("icecream" "20100702") :vanille 100 :chocolade 200 :riesling-sorbet 100)

(set-attributes *super2* '("cake" "20100701") :cheescake 2 :linzer 100 :apfel 2)
(set-attributes *super2* '("cake" "20100702") :cheescake 100 :linzer 10 :apfel 20)

(get-attributes *super2* '("icecream" "20100701"))
;;; => ((#(67 72 79 67 79 76 65 68 69) . #(50))
 (#(82 73 69 83 76 73 78 71 45 83 79 82 66 69 84) . #(57 48 48))
 (#(86 65 78 73 76 76 69) . #(49 48 48)))


(get-attributes *super2* '("icecream" "20100701") :column-names '("VANILLE"))
;;; => ((#(86 65 78 73 76 76 69) . #(49 48 48)))


(get-attributes *super2* "icecream")
;;; => ((("icecream" #(50 48 49 48 48 55 48 49))
  (#(67 72 79 67 79 76 65 68 69) . #(50))
  (#(82 73 69 83 76 73 78 71 45 83 79 82 66 69 84) . #(57 48 48))
  (#(86 65 78 73 76 76 69) . #(49 48 48)))
 (("icecream" #(50 48 49 48 48 55 48 50))
  (#(67 72 79 67 79 76 65 68 69) . #(50 48 48))
  (#(82 73 69 83 76 73 78 71 45 83 79 82 66 69 84) . #(49 48 48))
  (#(86 65 78 73 76 76 69) . #(49 48 48))))


(get-attributes *super2* '("icecream" "20100701") :column-names '("VANILLE"))
;;; => ((#(86 65 78 73 76 76 69) . #(49 48 48)))

(get-attributes *super2* "icecream" :column-names '("VANILLE"))
;;; => nil ?

(get-attributes *super2* nil :column-names '("VANILLE"))
;;; => nil ?


;;; variations key, supercolumn, column

#|

;;; (k- s- c-) => everything
(CASSANDRA_2.1.0:GET-RANGE-SLICE
 *ks* "Keyspace1"
 (CASSANDRA_2.1.0:make-columnparent :column-family "Super2")
 (CASSANDRA_2.1.0:make-slicepredicate :slice-range (CASSANDRA_2.1.0:make-slicerange :start "" :finish ""))
 "" "" 100 1)
==>
(****)


;;; (k- s- c+) => a keyslice for each key, but no supercolumns 
(CASSANDRA_2.1.0:GET-RANGE-SLICE
 *ks* "Keyspace1"
 (CASSANDRA_2.1.0:make-columnparent :column-family "Super2")
 (CASSANDRA_2.1.0:make-slicepredicate :column-names '("VANILLE"))
 "" "" 100 1)
==>
(#<CASSANDRA_2.1.0:KEYSLICE  :KEY "icecream" :COLUMNS NIL {B587269}>
 #<CASSANDRA_2.1.0:KEYSLICE  :KEY "cake" :COLUMNS NIL {B587351}>)


;;; (k- s+ c-) => a keyslice for each key, where the supercolumn is present, with its columns
(CASSANDRA_2.1.0:GET-RANGE-SLICE
 *ks* "Keyspace1"
 (CASSANDRA_2.1.0:make-columnparent :column-family "Super2" :super-column "20100701")
 (CASSANDRA_2.1.0:make-slicepredicate :slice-range (CASSANDRA_2.1.0:make-slicerange :start "" :finish ""))
 "" "" 100 1)
==>
(#<CASSANDRA_2.1.0:KEYSLICE
 :KEY "icecream"
 :COLUMNS (#<CASSANDRA_2.1.0:COLUMNORSUPERCOLUMN
           :COLUMN #<CASSANDRA_2.1.0:COLUMN
                    :NAME #(67 72 79 67 79 76 65 68 69)
                    :VALUE #(50)
                    :TIMESTAMP 134996243690000000 {B568641}> {B568621}>
           #<CASSANDRA_2.1.0:COLUMNORSUPERCOLUMN
           :COLUMN #<CASSANDRA_2.1.0:COLUMN
                   :NAME #(82 73 69 83 76 73 78 71 45 83 79 82 66 69 84)
                   :VALUE #(57 48 48)
                   :TIMESTAMP 134996243690000000 {B5687D1}> {B5687B1}>
           #<CASSANDRA_2.1.0:COLUMNORSUPERCOLUMN
           :COLUMN #<CASSANDRA_2.1.0:COLUMN
                   :NAME #(86 65 78 73 76 76 69)
                   :VALUE #(49 48 48)
                   :TIMESTAMP 134996243690000000 {B568961}> {B568941}>)
 {B568541}>
 #<CASSANDRA_2.1.0:KEYSLICE
 :KEY "cake"
 :COLUMNS (#<CASSANDRA_2.1.0:COLUMNORSUPERCOLUMN
           :COLUMN #<CASSANDRA_2.1.0:COLUMN
                   :NAME #(65 80 70 69 76)
                   :VALUE #(50)
                   :TIMESTAMP 134996244070000000 {B568BA1}> {B568B81}>
           #<CASSANDRA_2.1.0:COLUMNORSUPERCOLUMN
           :COLUMN #<CASSANDRA_2.1.0:COLUMN
                   :NAME #(67 72 69 69 83 67 65 75 69)
                   :VALUE #(50)
                   :TIMESTAMP 134996244070000000 {B568D29}> {B568D09}>
           #<CASSANDRA_2.1.0:COLUMNORSUPERCOLUMN
           :COLUMN #<CASSANDRA_2.1.0:COLUMN
                   :NAME #(76 73 78 90 69 82)
                   :VALUE #(49 48 48)
                   :TIMESTAMP 134996244070000000 {B568EB9}> {B568E99}>) {B568AD1}>)


;;; (k- s+ c+) => a keyslice for each key, with the column if it is present for the supercolumn
(CASSANDRA_2.1.0:GET-RANGE-SLICE
 *ks* "Keyspace1"
 (CASSANDRA_2.1.0:make-columnparent :column-family "Super2" :super-column "20100701")
 (CASSANDRA_2.1.0:make-slicepredicate :column-names '("VANILLE"))
 "" "" 100 1)
==>
(#<CASSANDRA_2.1.0:KEYSLICE
    :KEY "icecream"
    :COLUMNS (#<CASSANDRA_2.1.0:COLUMNORSUPERCOLUMN
              :COLUMN #<CASSANDRA_2.1.0:COLUMN
                      :NAME #(86 65 78 73 76 76 69)
                      :VALUE #(49 48 48)
                      :TIMESTAMP 134996243690000000 {B56EE21}> {B56EE01}>) {B56ED21}>
 #<CASSANDRA_2.1.0:KEYSLICE  :KEY "cake" :COLUMNS NIL {B56EF91}>)


;;; (k+ s- c-) => everything for the given key supercolumns and nested columns
(CASSANDRA_2.1.0:GET-RANGE-SLICE
 *ks* "Keyspace1"
 (CASSANDRA_2.1.0:make-columnparent :column-family "Super2")
 (CASSANDRA_2.1.0:make-slicepredicate :slice-range (CASSANDRA_2.1.0:make-slicerange :start "" :finish ""))
 "icecream" "icecream" 100 1)
==>
(**** for "icecream")


;;; (k+ s- c+) => a single keyslice, with no columns
(CASSANDRA_2.1.0:GET-RANGE-SLICE
 *ks* "Keyspace1"
 (CASSANDRA_2.1.0:make-columnparent :column-family "Super2")
 (CASSANDRA_2.1.0:make-slicepredicate :column-names '("VANILLE"))
 "icecream" "icecream" 100 1)
==>
(#<CASSANDRA_2.1.0:KEYSLICE  :KEY "icecream" :COLUMNS NIL {B59F1D9}>)


;;; (k+ s+ c-) => a single keyslice, with all columns
(CASSANDRA_2.1.0:GET-RANGE-SLICE
 *ks* "Keyspace1"
 (CASSANDRA_2.1.0:make-columnparent :column-family "Super2" :super-column "20100701")
 (CASSANDRA_2.1.0:make-slicepredicate :slice-range (CASSANDRA_2.1.0:make-slicerange :start "" :finish ""))
 "icecream" "icecream" 100 1)
==>
(#<CASSANDRA_2.1.0:KEYSLICE
 :KEY "icecream"
 :COLUMNS (#<CASSANDRA_2.1.0:COLUMNORSUPERCOLUMN
           :COLUMN #<CASSANDRA_2.1.0:COLUMN
                   :NAME #(67 72 79 67 79 76 65 68 69)
                   :VALUE #(50)
                   :TIMESTAMP 134996243690000000 {B5ABBE9}> {B5ABBC9}>
           #<CASSANDRA_2.1.0:COLUMNORSUPERCOLUMN
           :COLUMN #<CASSANDRA_2.1.0:COLUMN
                   :NAME #(82 73 69 83 76 73 78 71 45 83 79 82 66 69 84)
                   :VALUE #(57 48 48)
                   :TIMESTAMP 134996243690000000 {B5ABD79}> {B5ABD59}>
           #<CASSANDRA_2.1.0:COLUMNORSUPERCOLUMN
           :COLUMN #<CASSANDRA_2.1.0:COLUMN
                   :NAME #(86 65 78 73 76 76 69)
                   :VALUE #(49 48 48)
                   :TIMESTAMP 134996243690000000 {B5ABF09}> {B5ABEE9}>) {B5ABAE9}>)


;;; (k+ s+ c+) => a single key slice with the single column for the single supercolumn
(CASSANDRA_2.1.0:GET-RANGE-SLICE
 *ks* "Keyspace1"
 (CASSANDRA_2.1.0:make-columnparent :column-family "Super2" :super-column "20100701")
 (CASSANDRA_2.1.0:make-slicepredicate :column-names '("VANILLE"))
 "icecream" "icecream" 100 1)
==>
(#<CASSANDRA_2.1.0:KEYSLICE
 :KEY "icecream"
 :COLUMNS (#<CASSANDRA_2.1.0:COLUMNORSUPERCOLUMN
           :COLUMN #<CASSANDRA_2.1.0:COLUMN
                   :NAME #(86 65 78 73 76 76 69)
                   :VALUE #(49 48 48)
                   :TIMESTAMP 134996243690000000 {B5B0301}> {B5B02E1}>) {B5B0201}>)
|#

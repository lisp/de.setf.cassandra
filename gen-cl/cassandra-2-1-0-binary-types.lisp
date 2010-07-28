;;;  -*- Package: cassandra_2.1.0 -*-
;;;
;;; alternative methods specialized for binary keys

(cl:in-package :cassandra_2.1.0)

(cl:EVAL-WHEN (:COMPILE-TOPLEVEL :LOAD-TOPLEVEL :EXECUTE)
  (ORG.APACHE.THRIFT:DEF-STRUCT "get_slice_bin_args"
    (("keyspace" NIL :ID 1 :TYPE STRING)
     ("key" NIL :ID 2 :TYPE binary)
     ("column_parent" NIL :ID 3 :TYPE
      (ORG.APACHE.THRIFT:STRUCT
       "columnparent"))
     ("predicate" NIL :ID 4 :TYPE
      (ORG.APACHE.THRIFT:STRUCT
       "slicepredicate"))
     ("consistency_level" NIL :ID 5 :TYPE
      (ORG.APACHE.THRIFT:ENUM
       "ConsistencyLevel")))))

(ORG.APACHE.THRIFT.IMPLEMENTATION::DEF-REQUEST-METHOD
  CASSANDRA_2.1.0:GET-SLICE
  ((("keyspace" STRING 1) ("key" binary 2)
    ("column_parent" (ORG.APACHE.THRIFT:STRUCT "columnparent") 3)
    ("predicate" (ORG.APACHE.THRIFT:STRUCT "slicepredicate") 4)
    ("consistency_level" (ORG.APACHE.THRIFT:ENUM "ConsistencyLevel") 5))
   (ORG.APACHE.THRIFT:LIST (ORG.APACHE.THRIFT:STRUCT
                            "columnorsupercolumn")))
  (:IDENTIFIER "get_slice")
  (:DOCUMENTATION "get_slice w/ a binary key")
  (:CALL-STRUCT "get_slice_bin_args")
  (:REPLY-STRUCT "get_slice_result")
  (:EXCEPTIONS
   ("ire" NIL :ID 1 :TYPE
    (ORG.APACHE.THRIFT:STRUCT "invalidrequestexception"))
   ("ue" NIL :ID 2 :TYPE (ORG.APACHE.THRIFT:STRUCT "unavailableexception"))
   ("te" NIL :ID 3 :TYPE (ORG.APACHE.THRIFT:STRUCT "timedoutexception"))))


(cl:EVAL-WHEN (:COMPILE-TOPLEVEL :LOAD-TOPLEVEL :EXECUTE)
  (ORG.APACHE.THRIFT:DEF-STRUCT "insert_bin_args"
    (("keyspace" NIL :ID 1 :TYPE STRING)
     ("key" NIL :ID 2 :TYPE binary)
     ("column_path" NIL :ID 3 :TYPE
      (ORG.APACHE.THRIFT:STRUCT "columnpath"))
     ("value" NIL :ID 4 :TYPE binary)
     ("timestamp" NIL :ID 5 :TYPE
      ORG.APACHE.THRIFT:I64)
     ("consistency_level" NIL :ID 6 :TYPE
      (ORG.APACHE.THRIFT:ENUM
       "ConsistencyLevel")))))

(ORG.APACHE.THRIFT.IMPLEMENTATION::DEF-REQUEST-METHOD
  CASSANDRA_2.1.0:INSERT
  ((("keyspace" STRING 1) ("key" binary 2)
    ("column_path" (ORG.APACHE.THRIFT:STRUCT "columnpath") 3)
    ("value" binary 4) ("timestamp" ORG.APACHE.THRIFT:I64 5)
    ("consistency_level" (ORG.APACHE.THRIFT:ENUM "ConsistencyLevel") 6))
   ORG.APACHE.THRIFT:VOID)
  (:IDENTIFIER "insert")
  (:DOCUMENTATION
   "Insert a Column consisting of (column_path.column, value, timestamp) at the given column_path.column_family and optional
column_path.super_column. Note that column_path.column is here required, since a SuperColumn cannot directly contain binary
values -- it can only contain sub-Columns.")
  (:CALL-STRUCT "insert_bin_args")
  (:REPLY-STRUCT "insert_result")
  (:EXCEPTIONS
   ("ire" NIL :ID 1 :TYPE
    (ORG.APACHE.THRIFT:STRUCT "invalidrequestexception"))
   ("ue" NIL :ID 2 :TYPE (ORG.APACHE.THRIFT:STRUCT "unavailableexception"))
   ("te" NIL :ID 3 :TYPE (ORG.APACHE.THRIFT:STRUCT "timedoutexception"))))

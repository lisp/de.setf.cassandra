;;; -*- Mode: lisp; Syntax: ansi-common-lisp; Base: 10; Package: de.setf.cassandra; -*-

(in-package :de.setf.cassandra)

;;; graph a keyspace
                

(defun graph-keyspace (keyspace pathname
                  &key (size "11,11") (rankdir "LR")
                  row-count supercolumn-count column-count)
  (let ((*row-count* 0)
          (*column-count* 0)
          (*supercolumn-count* 0))
      (declare (special *row-count* *column-count* *supercolumn-count*))
      (labels ((to-string (vector)
                 (let ((string (trivial-utf-8:utf-8-bytes-to-string vector)))
                   (if (every #'graphic-char-p string)
                     string
                     (to-hex vector))))
               (to-hex (vector)
                 (with-output-to-string (stream)
                   (loop for elt across vector
                         do (format stream "~(~2,'0x~)" elt))))
               (put-column (column)
                 (when (or (null column-count) (<= (incf *column-count*) column-count))
                   (let* ((value (column-value column))
                          (name (column-name column))
                          (value-string (to-string value))
                          (name-string (to-string name)))
                     (dot:put-eol)
                     (dot:put-node column :label (format nil "{~a | ~a}" name-string value-string)
                                   :shape "record"))
                   column))
               (put-supercolumn (supercolumn)
                 (when (or (null supercolumn-count) (<= (incf *supercolumn-count*) supercolumn-count))
                   (let ((name-string (to-string (supercolumn-name supercolumn)))
                         (*column-count* 0))
                     (declare (special *column-count*))
                     (dot:put-node supercolumn :label name-string)
                     (dolist (column (supercolumn-columns supercolumn))
                       (cond ((put-column column)
                              (dot:put-edge supercolumn column))
                             (t
                              (return)))))
                   supercolumn))
               (put-row (keyslice)
                 (when (or (null row-count) (<= (incf *row-count*) row-count))
                   (let ((key-string (to-string (keyslice-key keyslice)))
                         (*column-count* 0)
                         (*supercolumn-count* 0))
                     (declare (special *column-count* *supercolumn-count*))
                     (dot:put-node keyslice :label key-string)
                     (dolist (cosc (key-slice-columns keyslice))
                       (let ((node nil))
                         (cond ((cond ((setf node (columnorsupercolumn-super-column cosc))
                                       (put-supercolumn node))
                                      ((setf node (columnorsupercolumn-column cosc))
                                       (put-column node))
                                      (t
                                       (warn "No cosc content: ~s: ~s." key-string cosc)))
                                (dot:put-edge keyslice node))
                               (t
                                (return)))))
                     keyslice))))
      
      (dot:context-put-graph pathname (pathname-name pathname)
                             #'(lambda ()
                                 (dolist (column-family (mapcar #'first (keyspace-description keyspace)))
                                   (let ((*row-count* 0))
                                     (declare (special *row-count*))
                                     (dot:put-subgraph (string (gensym "cluster"))
                                                       #'(lambda ()
                                                           (map-range-slices #'put-row keyspace
                                                                             :column-family column-family
                                                                             :count nil
                                                                             :start-key #() :finish-key #()))
                                                       :label (format nil "Column Family: ~a" column-family)))))
                             :rankdir rankdir
                             :size size
                             ;; :overlap "scale"
                             :ranksep 4)
      pathname)))

;;; (defparameter *ks* (keyspace *c-location* :name "Keyspace1"))
;;; (graph-keyspace *ks* "ks.dot")
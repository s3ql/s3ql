;; This tells Emacs to automatically run whitespace-cleanup when
;; editing any file in the S3QL source tree.
((nil . ((eval . (add-hook 'before-save-hook 'whitespace-cleanup nil t)))))

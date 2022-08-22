## Unreleased

* ...

## Version 1.3.0

### New Features

* Allow passing boto3 resource or client parameters to functions (issue #52, PR #57).

### Fixes

* Fix indentation in docs code examples (issues #50, #55, PRs #53, #56).

### Other changes

* Add testing under Python 3.10 (issue #58, PR #59).

## Version 1.2.1

### Fixes

* Fix typo in `transactions.put_items` unprocessed items key (issue #42, PR #45).
* Fix handling of unprocessed items in `transactions.put_items` (issue #44, PR #45).
* Fix handling of unprocessed keys in `transactions.get_items` (issue #46, PR #45).

### Other changes

* Add unit testing of unprocessed items/keys in `transactions.put_items` and `transactions.get_items` (issue #43, PR #45).
* Add missing lines to coverage report in tox config (PR #45).

## Version 1.2.0

### New Features

* Add the `attributes` parameter to the `get_df`, `transactions.get_item`, `transactions.get_items` and `transactions.get_all_items` functions. (issue #39, PR #40)


## Version 1.1.1

### Fixes

* Fix version number which was not updated in version 1.1.0.


## Version 1.1.0

### Modified Features

* Set `boto3` as an optional requirement to avoid unnecessary installation in lambda layers (issue #35, PR #36).
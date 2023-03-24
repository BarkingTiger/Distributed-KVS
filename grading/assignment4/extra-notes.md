# Extra Notes

- `expecting val in [val-i-j ]` means either val-i-j or empty (note the space after `j`), where empty is for status code 500. This happens when the node did not get the latest write for the key because of a partition and is allowed to respond to the GET request with status code 500 (and no value).
- `json: cannot unmarshal number into Go struct field [...].shard_id` means you used a number type (int/float) when you should have used a string.
- `invalid character '<' looking for beginning of value` as far as I have seen means your code threw an exception and the framework you have used (I saw this in Flask) has pretty-printed it in HTML (hence the '<').
- If a value is reported to be empty (e.g., `view={View:[]}`), it may be because of using the wrong field name in the JSON.


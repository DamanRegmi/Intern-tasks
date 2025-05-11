#!/bin/bash

input_file="web_log.log"
output_file="success.log"
grep '200' "$input_file" > "$output_file"
echo "Success logs have been extracted to $output_file."

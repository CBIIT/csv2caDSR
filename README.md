# csv2caDSR

A very simple program for harmonizing CSV files against
Common Data Elements from the [caDSR](https://datascience.cancer.gov/resources/metadata),
which we access using the [caDSR-on-FHIR service](https://github.com/HOT-Ecosystem/cadsr-on-fhir/).

## How to use

Using csv2caDSR is an overly complicated process (it will be improved in later versions if needed):

1. `sbt "run --csv example.csv --to-json example.json"`
    - Creates a JSON file describing the columns in the CSV file.

2. Fill in the caDSR values in the CSV file.

3. `sbt "run --json example-with-caDSRs.json --to-json example-with-values.json"`
    - Fills in values for caDSR values.

4. Map CSV values to caDSR values.

5. `sbt "run --json example-with-values.json --to-json example-with-enumValues.json"`
    - Retrieve descriptions and concept identifiers for the mapped caDSR values.

6. `sbt "run --csv example.csv --json example-with-enumValues.json --to-csv example-mapped.csv"`
    - Map CSV file to CDEs based on the mapping information in `example-with-enumValues.json`.

# csv2caDSR

A very simple program for harmonizing CSV files against
Common Data Elements from the [caDSR](https://datascience.cancer.gov/resources/metadata).

## How to use

Using csv2caDSR is an overly complicated process (it will be improved in later versions if needed):

1. `sbt "runMain org.renci.ccdh.csv2cadsr.csv2caDSR example.csv" > example.json`
  - Creates a JSON file describing the columns in the CSV file.

2. Fill in the caDSR values in the CSV file.

3. `sbt "runMain org.renci.ccdh.csv2cadsr.csv2caDSR fill example-with-caDSRs.json" > example-with-values.json`
  - Fills in values for caDSR values.

4. Map CSV values to caDSR values.

5. `sbt "runMain org.renci.ccdh.csv2cadsr.csv2caDSR fill example-with-values.json" > example-with-enumValues.json`
  - Retrieve descriptions and concept identifiers for the mapped caDSR values.

6. `sbt "runMain org.renci.ccdh.csv2cadsr.csv2caDSR example.csv example-with-enumValues.json > example-mapped.csv`
  - Map CSV file to CDEs based on the mapping information in `example-with-enumValues.json`.

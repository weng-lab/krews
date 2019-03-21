# State

State is tracked internally with an SQLite file database. This is periodically copied into your
working directory under state/metadata.db. This file should NOT be deleted if you want to make full use of caching.

## Caching

Tasks can often be expensive, so we don't want to repeat them if we don't need to. That's where caching comes in.

For every file created by a task run, which can be found in the outputs directory, we save the following data 
related to the task runs that created it:

- task name
- Inputs (serialized as JSON)
- 

# Run Reports

  
# pitertools
Tools to process python iterators in parallel.

## map_parallel
Spin up n threads to pull from input iterator and run an operation on it in parallel

##
Roadmap
- Add more tests 
- Add some linter, static type checking
- Add `ordered` parameter to `map_parallel` (currently results are unordered)
- Allow running on external executor

# smart-restore

CLI tool to efficiently copy data between databases

**NOTE: As of 2024-01-03, this is still in prototype phase, not ready for widespread usage yet!**

## Getting Started

1. Install via PIP:
```
$ pip install git+ssh://git@github.com/camoag/smart-restore.git@main
```

2. Run the command (use `--help` for usage details):

```
$ smart_restore source-database-url target-database-url users id=123
```

TODO: More to come!


## Roadmap / Ideas

### Usability

- [ ] Allow easy restore of formulas / "lists of rows", for example a list of users + farms for Feature Env restore
    - Consider: loading from file, or piping in newline-separated lists of (table, filter) patterns, ex `echo users id=1 id=2\nfarms id=5 | smart_restore ...`

### Clean Code
- [ ] Clean interface between reader + write, perhaps just passing single "upsert_row" function
- [ ] Move metadata out to shared "schema" class/instance
- [ ] Separate READ process
    - [ ] THEN: Move TQDM progress bars out of write and into main loop
- [ ] Test coverage
    - Consider [python-cli-test-helpers](https://github.com/painless-software/python-cli-test-helpers) library

### Performance
- [ ] Use concurrent futures to further parallelize groups of reads and writes

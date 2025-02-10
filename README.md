# ZQLite

A Zig Implementation of sqlite (work in progress), hence the name zqlite. For my own learning, I've reimplemented from scratch based upon the [sqlite documentation](https://www.sqlite.org/fileformat2.html). 

## Run

```bash
zig run src/main.zig -- ./db/test.db "SELECT * FROM users;"
```


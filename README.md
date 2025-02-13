# trove-to-ipfs

My experience moving a trove of docs into IPFS with postgres metadata

I have been given a trove of ~5M little files. It doesn't matter what they are. What does matter is that (i) together they are about 5TB, which is a lot to manage; and (ii) I want to store them in IPFS so that my partners can use them; and (iii) I want to keep the metadata in a postgres database so that we can find what we're looking for (there are other reasons to keep the metadata in postgres, but those are outside the scope of this post).

First I indexed the files. I used bash `find` to make a list of all the files. I had stored the files on two external USB drives (`STARTPATH1`, `STARTPATH2`). For not-entirely-good reasons, I stored the list of files originally in a sqlite database:
```bash
$ find $STARTPATH1 -name '*.msg' -printf "%h,%f,%s \n" | sqlite3 $OUTPUTF ".import --csv /dev/stdin fs"
$ find $STARTPATH2 -name '*.msg' -printf "%h,%f,%s \n" | sqlite3 $OUTPUTF ".import --csv /dev/stdin fs"
```
`find` outputs the path, filename, and size in bytes as a comma-delimited list (which is fine as long as none of the paths have commas in them). The data goes straight into a sqlite database. This wasn't a good place to keep the data, so I moved it into postgres:
```bash
mv fs.db fs.sqlite
sqlite3 fs.sqlite .dump > fs.sql
psql -U pball -c "CREATE DATABASE mydb;"
psql -U pball -d mydb -W < fs.sql
```
Now we can get to work.


<!-- done -->

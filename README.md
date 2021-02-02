# takeHomeProject
### 1. How to run
execute the following command to add the item in the crontab jobs

crontab -e

00 09 * * * python /src/load_data/load_data.py >> /log/load_data.log 2>&1

### 2. Something can be optimized
SQLite database is a lightweight database. If we use a data warehouse,
we can store all county's data in one partitioned fact table, add dimension
table storing county information. Everyday just load incremental data into
the fact table. So that we don't need to get county data from APi, then avoiding
SQL injection.

# Frequently Asked Questions

### ODBC connections work in `ogr_fdw_info.exe` but not in the database, why?

1. One possibility is that you registered DSN under USER instead of System.  It should be system since otherwise it will only work under the account you are logged in as. Since `ogr_fdw_info.exe` is  a client app, it will give you a false sense of success since ODBC will in server, run under the context of the PostgreSQL service account. Verify that you have a System DSN (and **not** a User DSN).
2. If you used the Windows installer for PostgreSQL, it starts up PostgreSQL using Network Service account. I always manually switch it to a real user account since I need it to access some network resources. I never tested, but I suspect ODBC keys may not be readable by Network Service account.
 
If change #1 doesn't work, try changing PostgreSQL to run under a regular user account.  Make sure that user has full control of the PostgreSQL data folder.

### Do I need ODBC to read an MS Access database?

You shouldn't need to do ODBC for MS Access, should be able to do:
 
    ogr_fdw_info.exe -s "D:\FDW Data\My Folder\MYFILE.MDB"
 
As an added bonus, using a direct access shouldn't need to read ODBC registry keys since it's a DNSless connection.
 
### Why doesn't my MS Access connection work?
 
If your MS Access database is on a network share, your PostgreSQL service account needs to be able to access it by the path you use. For MS Access databases it also has to have write permissions. The reason is MS Access databases use a locking file to manage access, so all users that have read permission also need to have write permission into the folder to create the lock file and delete the lock file (if they are the last ones in) if it doesn't exist.
 
Also note: even if the PostgreSQL service account can access the folder `\\S\Files`, if you map it to say `S:\` the connection will not work if the mapped drive is not set under the user account that PostgreSQL runs under. (That said – it's safer to use UNCs (`\\S\Files`) instead of mapped drives.

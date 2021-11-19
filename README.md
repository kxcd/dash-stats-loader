# dash-stats-loader
backed stats loader
This is a backend to create a sqlite database with some network statistics, ideally install it to ~/bin as a non-privlieged user, assume 'dash', then add the following line to crontab to call it periodically.
    */20 * * * *  nice -19 ~/bin/dash_stats_loader.sh >>~/dash_stats_loader.log  2>&1
The database file is copied to the web server, comment out the below line if that is not required.
    cp "$DATABASE_FILE" /var/www/html/dash-stats/ && mv -f /var/www/html/dash-stats/$(basename "$DATABASE_FILE") /var/www/html/dash-stats/.stats.db
The backup db will start to consume a lot of space, recommend commenting out the following lines if not important to keep DB backups.
    # This will be a good time to take a backup of the database.
    BACKUP_DB="$(dirname "$DATABASE_FILE")/"$run_date"_stats.db"
    cp "$DATABASE_FILE" "$BACKUP_DB"
    xz -eT5 "$BACKUP_DB" >/dev/null 2>&1

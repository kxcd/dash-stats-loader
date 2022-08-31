#!/bin/bash
#set -x

VERSION="$0 (v0.3.0 build date 202208311800)"
DATABASE_VERSION=2
DATADIR="$HOME/.dash_stats_loader"

dcli () {
	dash-cli -datadir=/tmp -conf=/etc/dash.conf "$@"
}

pidof -q -x -o $$ $(basename "$0")&&exit

usage(){
	text="$VERSION\n"
	text+="This program will collect key state information about the DASH network\n"
	text+="and store it to a sqlite database.\n\n"
	text+="Usage: $0 [ options ] \n\n"
	text+="Options:\n"
	text+="	-help				This help text.\n"
	text+="	-datadir [path_to_dir]		The location to save the data in, default location is $DATADIR"
	echo -e "$text"
}

# Parse commandline options and set flags.
while (( $# > 0 ));do
	arg="$1"

	case $arg in
		-h|-help|--help)
			usage
			exit 0
			;;
		-datadir)
			datadir="$2"
			shift;shift
			;;
		*)
			echo -e "[$$] $VERSION\n[$$] Unknown parameter $1\n[$$] Please check help page with $0 -help" >&2
			exit 1
			;;
	esac
done

echo "[$$] Starting $VERSION." >&2



# Now we are safe to use variables like NETWORK and DATADIR, so let's compute a database_file
DATABASE_FILE="$DATADIR/database/stats.db"


# Checks that the required software is installed on this machine.
check_dependencies(){

	bc -v >/dev/null 2>&1 || progs+=" bc"
	jq --version >/dev/null 2>&1 || progs+=" jq"
	sqlite3 -version >/dev/null 2>&1 || progs+=" sqlite3"

	if [[ -n $progs ]];then
		text="[$$] $VERSION\n[$$] Missing applications on your system, please run\n\n"
		text+="[$$] sudo apt install $progs\n\n[$$] before running this program again."
		echo -e "$text" >&2
		exit 2
	fi
}


make_datadir(){

	# If a custom datadir is passed in, use it.
	[[ -n $1 ]] && DATADIR="$1"

	if [[ ! -d "$DATADIR" ]];then
		mkdir -p "$DATADIR"/{database,logs}
		if (( $? != 0 ));then
			echo "[$$] Error creating datadir at $DATADIR exiting..." >&2
			exit 3
		fi
	fi
}


# A safe wrapper around SQL access to help with contention in concurrent environments.
execute_sql(){

	[[ -z $1 ]] && return
	for((i=1; i<100; i++));do
		sqlite3 "$DATABASE_FILE" <<< "$1" 2>>"$DATADIR"/logs/sqlite.log && return
		retval=$?
		echo "[$$] Failed query attempt number: $i." >>"$DATADIR"/logs/sqlite.log
		delay=1
		# Add extra delay time after every 10 failed shots.
		(($((i % 10)) == 0)) && delay=$((delay+RANDOM%100))
		(($((i % 20)) == 0)) && delay=$((delay+RANDOM%300))
		sleep "$delay"
	done
	echo -e "[$$] The failed query vvvvv\n$1\n[$$] ^^^^^ The above query did not succeed after $i attempts, aborting..." >>"$DATADIR"/logs/sqlite.log
	return $retval
}



initialise_database(){

	if [[ ! -f "$DATABASE_FILE" ]];then
		# Create db objects.
		# STATS						-	Stores a snapshot of the dash network.
		#			run_date					-	The date/time of execution in yyyymmddHHmiss format.
		#			height						-	The reported blockchain height
		#			chainlocked_YN				-	Mandatory, choice of 'Y' for chainlocked block, 'N' for no chainlock found.
		#			chainlock_lag				-	Number of blocks since the last chainlock was recorded.
		#			difficulty_mega				-	Mining difficulty reported in the millions.
		#			supply_mega					-	Number of coins ever minted in millions
		#			mempool_txes				-	Number of transactions in the mempool.
		#			mempool_size_kb				-	The size of the mempool in kilobytes.
		#			tx_rate						-	The average number of transactions per second seen in the last hour.
		#			collateralised_masternodes	-	The number of masternodes locking away 1000 DASH.
		#			enabled_masternodes			-	The number of masternodes that are actually running.
		#			price_usd					-	Sourced from coinpaprika.
		#			price_btc					-	Sourced from coinpaprika.
		#			price_usd_24hr_change		-	The percent gain/loss in the USD price in the last 24 hours.
		#
		# masternodes				-	Stores a snapshot of the masternode network at that point in time.
		# masternode_id_run_date	-	Stores the run_date and the masternode id only.
		#
		# loading					-	Stores the data of the current load and indicates incomplete data.
		#			run_date					-	If not null, then the data for that date is incomplete.
		#
		sql="PRAGMA foreign_keys = ON;"
		sql+="create table db_version(version integer primary key not null);"
		sql+="insert into db_version values(2);"
		sql+="create table STATS(run_date INTEGER PRIMARY KEY ASC NOT NULL, height INTEGER NOT NULL check(height>=0),chainlocked_YN text not null ,chainlock_lag integer not null,difficulty_mega real not null check(difficulty_mega>=0), supply_mega real not null check(supply_mega>=0), mempool_txes integer not null check(mempool_txes>=0), mempool_size_kb real not null check(mempool_size_kb>=0), tx_rate real not null check(tx_rate>=0), collateralised_masternodes integer not null check(collateralised_masternodes>=0), enabled_masternodes integer not null check(enabled_masternodes>=0), price_usd real not null check(price_usd>=0), price_btc real not null check(price_btc>=0), price_usd_24hr_change real not null, mn_days real not null);"
		sql+="create unique index idx_run_date on STATS(run_date);"
		sql+="CREATE TABLE masternodes(id integer primary key asc not null, protx_hash text not null, collateral_hash text not null, collateral_hash_index integer not null,ip text not null, port integer not null check(port>=0 and port<65536),status text not null check(status in('E','P','U')),owner_address text not null,voting_address text not null, payout_address text not null,collateral_address text not null);"
		# This index is handy to have, but strictly not needed to run the PHP site.
		sql+="CREATE INDEX idx_collateral_hash_index on MASTERNODES(collateral_hash,collateral_hash_index);"
		sql+="CREATE TABLE masternode_id_run_date(id int not null, run_date int not null, primary key(id,run_date),foreign key(id)references masternodes,foreign key(run_date)references stats(run_date));"
		sql+="CREATE INDEX idx_run_date_masternode_id_run_date on masternode_id_run_date(run_date);"
		sql+="create index idx_masternodes_id_run_date_id on masternode_id_run_date(id);"
		sql+="create table loading(run_date integer, foreign key(run_date)references stats(run_date),primary key(run_date));"
		execute_sql "$sql"
		if (( $? != 0 ));then
			echo "[$$] Cannot initialise sqlite database at $DATABASE_FILE exiting..." >&2
			exit 4
		fi
	fi
}





# Make sure the version is at the latest version and upgrade the schema if possible.
check_and_upgrade_database(){

	db_version=$(execute_sql "select version from db_version;")
	if (( db_version != DATABASE_VERSION ));then
		echo "[$$] The database version is $db_version was expecting $DATABASE_VERSION" >&2
		exit 5;
	fi
	count=$(execute_sql "select count(1) from loading;")
	if ((count > 0));then
		echo "[$$] Clean $count stale run(s)."
		deleted=$(execute_sql "delete from stats where run_date in (select run_date from loading);select changes();")
		echo "[$$] Deleted $deleted records in the stats table..."
		# To delete this run, we have to do a reference count to the data, if the id is used more than once, then do not delete the data from masternodes table.
		# Find all masternode records that are referenced once only, then select those that are also from this run_date and delete them.
		deleted=$(execute_sql "delete from masternodes where id in (select id from masternode_id_run_date where run_date in (select run_date from loading) and id in(select id from masternode_id_run_date group by id having count(1)=1));select changes();")
		echo "[$$] Deleted $deleted records from masternodes..."
		deleted=$(execute_sql "delete from masternode_id_run_date where run_date in (select run_date from loading);select changes();")
		echo "[$$] Deleted $deleted records from masternode_id_run_date..."
		deleted=$(execute_sql "delete from loading;select changes();")
		echo "[$$] Deleted $deleted records from loading..."
	fi
	echo "[$$] Checking integrity of the database..."
	retval=$(sqlite3 "$DATABASE_FILE" <<< "PRAGMA main.integrity_check;" 2>>"$DATADIR"/logs/sqlite.log)
	if [[ "$retval" != "ok" ]];then
		echo -e "[$$] Database integrity check failed.\n$retval\nExiting..."
		exit 6
	fi
	echo "[$$] Checking the foreign keys in the database..."
	retval=$(sqlite3 "$DATABASE_FILE" <<< "PRAGMA main.foreign_key_check;" 2>>"$DATADIR"/logs/sqlite.log)
	if (( ${#retval} != 0 ));then
		echo "[$$] There are some errors with foreign keys in this database, exiting..."
		exit 7
	fi
	missing=$(execute_sql "select count(1) from masternodes a where not exists (select 1 from masternode_id_run_date b where a.id=b.id);")
	((missing>0))&&{ echo "[$$] There are $missing records in the masternodes table.";exit 2;}
	missing=$(execute_sql "select count(1) from masternode_id_run_date a where not exists (select 1 from masternodes b where a.id=b.id);")
	((missing>0))&&{ echo "[$$] There are $missing records in the masternode_id_run_date table.";exit 2;}
	pages=$(execute_sql "select count(1) from stats;")
	mn_pages=$(execute_sql "select count(distinct run_date) from masternode_id_run_date;")
	masternodes=$(execute_sql "select count(1) from masternodes;")
	echo "[$$] Database is up to date and contains a record of $pages snapshot(s) and $mn_pages masternode snapshot(s) and $masternodes distinct masternode versions." >&2
}

# Cleanup gracefully in case of an interruption.
catch_sig(){
	echo -e "\n[$$] Caught signal, cleaning up and exiting..."
	start_time=$EPOCHSECONDS
	deleted=$(execute_sql "delete from stats where run_date=$run_date;select changes();")
	echo "[$$] Deleted $deleted records from stats..."
	# To delete this run, we have to do a reference count to the data, if the id is used more than once, then do not delete the data from masternodes table.
	# Find all masternode records that are referenced once only, then select those that are also from this run_date and delete them.
	deleted=$(execute_sql "delete from masternodes where id in (select id from masternode_id_run_date where run_date=$run_date and id in(select id from masternode_id_run_date group by id having count(1)=1));select changes();")
	echo "[$$] Deleted $deleted records from masternodes..."
	deleted=$(execute_sql "delete from masternode_id_run_date where run_date=$run_date;select changes();")
	echo "[$$] Deleted $deleted records from masternode_id_run_date..."
	execute_sql "delete from loading;"
	echo "[$$] Purge completed in $((EPOCHSECONDS - start_time)) seconds."
	exit 0
}



# Main part of the program.
echo "[$$] Checking program dependencies..."
check_dependencies

# $datadir can get set by a commandline option.
echo "[$$] Checking datadir $datadir..."
make_datadir "$datadir"

echo "[$$] Initialising database..."
initialise_database

echo "[$$] Checking database..."
check_and_upgrade_database





# Make sure we exit on any error, there is no point loading the database with garbage.
set -e
# First get all the data and load the variables, then commit all in one go since the data is timely.
# Use the same names as the database fields.

run_date=$(date +"%Y%m%d%H%M%S")
echo "[$$} Run date is $run_date."

echo "[$$] Fetching block height..."
height=$(dcli getblockcount)

echo "[$$] Fetching the network difficulty..."
difficulty_mega=$(echo "scale=2; $(dcli getblockchaininfo|jq -r '.difficulty')/1000000"|bc)
echo "[$$] Fetching the coin supply..."
supply_mega=$(echo "scale=2; $(dcli gettxoutsetinfo|jq -r '.total_amount')/1000000"|bc)

# We do these checks a little later after getting the height in order to allow for some time for the chainlock to propagate, since there is a race
# condition with getting the height and checking too soon for the lock on the block.
block=$(dcli getblock $(dcli getblockhash $height))
echo "[$$] Fetching ChainLock status..."
chainlocked_YN=$(if [ $(jq -r '.chainlock' <<<$block) = "true" ];then echo "Y";else echo "N";fi)
echo "[$$] Computing ChainLock lag..."
# It is possible to get a negative chainlock lag here, that is because of the forced delay (see above) we may be chainlocking on more recent block.
# So, a lag of <=0 means the current block $height is for sure chainlocked.
chainlock_lag=$((height-$(dcli getbestchainlock|jq -r '.height')))
[[ -z $chainlock_lag ]] && exit 1

echo "[$$] Fetching mempool tx count..."
mempool_txes=$(dcli getmempoolinfo|jq -r '.size')
echo "[$$] Fetching mempool size..."
mempool_size_kb=$(echo "scale=2;$(dcli getmempoolinfo|jq -r '.bytes')/1024"|bc)
echo "[$$] Calculating tx rate..."

current_time=$(jq -r '.time' <<<$block)
num_txes=$(jq -r '.tx | length' <<<$block)

previous_time=$current_time
_height=$height

#  Go back in time for at least 1 hour (3600 seconds) of block time and sum the number of TXes during that time.
until (( previous_time <= $((current_time-3600)) ));do
	((_height--))
	_block=$(dcli getblock $(dcli getblockhash $_height))
	previous_time=$(jq -r '.time' <<<$_block)
	((num_txes+=$(jq -r '.tx | length' <<<$_block)))
done
diff_time=$((current_time - previous_time))
tx_rate=$(printf '%0.4f' $(bc<<<"scale=6;$num_txes / $diff_time"))

echo "[$$] Fetching masternode counts..."
mn_count=$(dcli masternode count)
collateralised_masternodes=$(jq .total <<< "$mn_count")
enabled_masternodes=$(jq .enabled <<< "$mn_count")

# Collect the masternode data, but it gets used a little later.
masternodes=$(dcli masternode list)


echo "[$$] Fetching masternode days..."
protx_list=$(dcli protx list valid 1)
mn_days=$(jq '.[].state.registeredHeight'<<<"$protx_list"| awk -v height="$height" '{sum+=height-$1}END{print sum/NR*2.625/60/24}')






echo "[$$] Fetching price data..."


# This API is unreliable, if getting the prices fails, or we get junk, then skip this and just set zeros.
dash_data=$(curl -s https://api.coinpaprika.com/v1/tickers/dash-dash?quotes=USD,BTC)
if [[ -n $dash_data && ! $dash_data =~ "503 Service Unavailable" ]];then
	price_usd=$(printf '%.2f' $(jq .quotes.USD.price<<<$dash_data))
	price_btc=$(printf '%.6f'  $(jq .quotes.BTC.price<<<$dash_data))
	price_usd_24hr_change=$(printf '%.2f' $(jq .quotes.USD.percent_change_24h<<<$dash_data))
else
	price_usd=0
	price_btc=0
	price_usd_24hr_change=0
fi



set +e

# Use signal trap to roll back the database changes.
trap 'catch_sig' SIGTERM SIGINT SIGHUP

loading_count=$(execute_sql "select count(1) from loading;")
((loading_count >0))&&{ echo "[$$] The database is already busy, aborting!";exit 99;}
execute_sql "insert into loading values($run_date);"||\
{ echo "[$$] Failed to update loading table, aborting.";exit 98;}

sleep 5
loading_count=$(execute_sql "select count(1) from loading;")
if ((loading_count >1));then
	echo "[$$] Another process is trying to update the database AT THE SAME TIME! Aborting!"
	execute_sql "delete from loading where run_date=$run_date;"
	exit 99
fi

sql="insert into stats (run_date,height,chainlocked_YN,chainlock_lag,difficulty_mega,supply_mega,mempool_txes,mempool_size_kb,tx_rate,collateralised_masternodes,enabled_masternodes,price_usd,price_btc,price_usd_24hr_change,mn_days)values($run_date,$height,\"$chainlocked_YN\",$chainlock_lag,$difficulty_mega,$supply_mega,$mempool_txes,$mempool_size_kb,$tx_rate,$collateralised_masternodes,$enabled_masternodes,$price_usd,$price_btc,$price_usd_24hr_change,$mn_days);"
execute_sql "$sql"


echo "[$$] Loading masternodes database..."
start_time=$EPOCHSECONDS

key=0
collateral_hashes_array=($(jq -r 'keys_unsorted[]'<<<"$masternodes"))
while read proTxHash address status owner_address voting_address payee collateraladdress rest;do
	case $status in
		ENABLED)
			status="E"
			;;
		POSE_BANNED)
			status="P"
			;;
		*)
			echo "*** Unhandled masternode status $status ***" 1>&2
			status="U"
			;;
	esac
	if (( ${#address} <7));then
		ip=0
		port=0
	else
		ip=${address%:*}
		port=${address#*:}
	fi
	collateral_hash=${collateral_hashes_array[$key]%-*}
	collateral_hash_index=${collateral_hashes_array[$key]#*-}
	((key++))

	while :;do
		sql="select id from masternodes where protx_hash='$proTxHash' and collateral_hash='$collateral_hash' and collateral_hash_index=$collateral_hash_index and ip='$ip' and port=$port and status='$status' and owner_address='$owner_address' and voting_address='$voting_address' and payout_address='$payee' and collateral_address='$collateraladdress';"
		id=$(execute_sql "$sql")
		if [[ $id == "" ]];then
			echo -n "*"
			execute_sql "insert into masternodes(protx_hash,collateral_hash,collateral_hash_index,ip,port,status,owner_address,voting_address,payout_address,collateral_address) values('$proTxHash','$collateral_hash',$collateral_hash_index,'$ip',$port,'$status','$owner_address','$voting_address','$payee','$collateraladdress');"
		else
			break;
		fi
	done
	echo -n "#"
	execute_sql "insert into masternode_id_run_date values($id,$run_date);"
done  < <(jq -r '.[]|"\(.proTxHash) \(.address) \(.status) \(.owneraddress) \(.votingaddress) \(.payee) \(.collateraladdress)"'<<<"$masternodes")

echo -e "\n[$$] Done Loading masternodes database in $((EPOCHSECONDS - start_time)) seconds..."





echo "[$$] Checking masternodes database for differences..."
start_time=$EPOCHSECONDS

additions=$(execute_sql "select count(1) from masternode_id_run_date a join masternodes b on a.id=b.id where a.run_date=(select max(run_date) from masternode_id_run_date) and not exists (select 1 from masternodes c join masternode_id_run_date d on c.id=d.id where b.collateral_hash=c.collateral_hash and b.collateral_hash_index=c.collateral_hash_index and d.run_date=(select run_date from(SELECT distinct run_date, dense_rank() over (order by run_date desc)date_rank FROM masternode_id_run_date)where date_rank=2));")

deletions=$(execute_sql "select count(1) from masternode_id_run_date a join masternodes b on a.id=b.id where a.run_date=(select run_date from(SELECT distinct run_date, dense_rank() over (order by run_date desc)date_rank FROM masternode_id_run_date)where date_rank=2) and not exists (select 1 from masternodes c join masternode_id_run_date d on c.id=d.id where c.collateral_hash=b.collateral_hash and b.collateral_hash_index=c.collateral_hash_index and d.run_date=(select max(run_date) from masternode_id_run_date));")

((changes=additions+deletions))

echo "[$$] Done in $((EPOCHSECONDS - start_time)) seconds..."


if ((changes>0));then
	echo "[$$] New data detected, keeping $additions addition(s) and $deletions deletion(s)..."
else
	echo "[$$] No changes, purging new data..."
	start_time=$EPOCHSECONDS
	deleted=$(execute_sql "delete from stats where run_date=$run_date;select changes();")
	echo "[$$] Deleted $deleted records from stats..."
	# To delete this run, we have to do a reference count to the data, if the id is used more than once, then do not delete the data from masternodes table.
	# Find all masternode records that are referenced once only, then select those that are also from this run_date and delete them.
	deleted=$(execute_sql "delete from masternodes where id in (select id from masternode_id_run_date where run_date=$run_date and id in(select id from masternode_id_run_date group by id having count(1)=1));select changes();")
	echo "[$$] Deleted $deleted records from masternodes..."
	deleted=$(execute_sql "delete from masternode_id_run_date where run_date=$run_date;select changes();")
	echo "[$$] Deleted $deleted records from masternode_id_run_date..."

#	echo "[$$] Doing a VACUUM..."
#	execute_sql "vacuum;"
	echo "[$$] Purge completed in $((EPOCHSECONDS - start_time)) seconds."
fi
execute_sql "delete from loading where run_date=$run_date;"

trap - SIGTERM SIGINT SIGHUP

if ((changes>0));then
	# Now place a copy in /var/www/html/dash-stats/
	# Do it in two steps so the database is not being written too while copying over, the mv is instant.
	cp "$DATABASE_FILE" /var/www/html/dash-stats/ && mv -f /var/www/html/dash-stats/$(basename "$DATABASE_FILE") /var/www/html/dash-stats/.stats.db

	# This will be a good time to take a backup of the database.
	start_time=$EPOCHSECONDS
	echo "[$$] Backing up the database..."
	BACKUP_DB="$(dirname "$DATABASE_FILE")/${run_date}_stats.db"
	cp "$DATABASE_FILE" "$BACKUP_DB"
	xz -eT5 "$BACKUP_DB" >/dev/null 2>&1
	echo "[$$] Done in $((EPOCHSECONDS - start_time)) seconds..."
fi
echo "[$$] $0 exiting..."


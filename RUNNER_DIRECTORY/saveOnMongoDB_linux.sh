echo "mongoDB must be running in background and \$MONGO_HOME must be set"

for filename in results/query1/*; do
	sed '1 i\houseID, maxInstaValue' results/query1/$filename
	$MONGO_HOME/bin/mongoimport -d sabDb -c objects --type csv --file results/query1/$filename --headerline
done

for filename in results/query2mean/*; do
        sed '1 i\houseID&plugID, meanValue' results/query2mean/$filename
        $MONGO_HOME/bin/mongoimport -d sabDb -c objects --type csv --file results/query2mean/$filename --headerline
done

for filename in results/query2standardDeviation/*; do
        sed '1 i\houseID&plugID, standardDeviationValue' results/query2standardDeviation/$filename
        $MONGO_HOME/bin/mongoimport -d sabDb -c objects --type csv --file results/query2standardDeviation/$filename --headerline
done

for filename in results/query3/*; do
        sed '1 i\houseID&plugID, differenceFromMean' results/query3/$filename
        $MONGO_HOME/bin/mongoimport -d sabDb -c objects --type csv --file results/query3/$filename --headerline
done

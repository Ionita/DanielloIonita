clear
echo "mongoDB must be running in background and \$MONGO_HOME must be set"

for filename in results/query1/part*; do
sudo sed -i -e 's/(//g' $filename
sudo sed -i -e 's/)//g' $filename
cat $filename | pbcopy && echo "houseID, maxInstaValue" > $filename && pbpaste >> $filename
$MONGO_HOME/bin/mongoimport -d sabDb -c objects --type csv --file $filename --headerline
done

for filename in results/query2mean/part*; do
sudo sed -i -e 's/(//g' $filename
sudo sed -i -e 's/)//g' $filename
cat $filename | pbcopy && echo "houseID, plugID, meanValue" > $filename && pbpaste >> $filename
$MONGO_HOME/bin/mongoimport -d sabDb -c objects --type csv --file $filename --headerline
done

for filename in results/query2standardDeviation/part*; do
sudo sed -i -e 's/(//g' $filename
sudo sed -i -e 's/)//g' $filename
cat $filename | pbcopy && echo "houseID, plugID, standardDeviationValue" > $filename && pbpaste >> $filename
$MONGO_HOME/bin/mongoimport -d sabDb -c objects --type csv --file $filename --headerline
done

for filename in results/query3/part*; do
sudo sed -i -e 's/(//g' $filename
sudo sed -i -e 's/)//g' $filename
cat $filename | pbcopy && echo "houseID, plugID, differenceFromMean" > $filename && pbpaste >> $filename
$MONGO_HOME/bin/mongoimport -d sabDb -c objects --type csv --file $filename --headerline
done

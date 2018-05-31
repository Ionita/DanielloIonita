clear

echo "  _____  ___  ____________          __  "
echo " /  ___|/ _ \ | ___ \  _  \        /  | "
echo " \  --./ /_\ \| |_/ / | | |  _ __   | | "
echo "   --. \  _  || ___ \ | | | |  _ \  | | "
echo " /\__/ / | | || |_/ / |/ /  | |_) |_| | "
echo " \____/\_| |_/\____/|___/   | .__(_)___ "
echo "                            | |         "
echo "                            |_|         "
echo " ______ _  ___        _      _ _        "
echo " |  _  ( )/ _ \      (_)    | | |       "
echo " | | | |// /_\ \_ __  _  ___| | | ___   "
echo " | | | | |  _  |  _ \| |/ _ \ | |/ _ \  "
echo " | |/ /  | | | | | | | |  __/ | | (_) | "
echo " |___/   \_| |_/_| |_|_|\___|_|_|\___/  "
echo "                                        "
echo "                                        "
echo "  _____            _ _                  "
echo " |_   _|          (_) |                 "
echo "   | |  ___  _ __  _| |_ __ _           "
echo "   | | / _ \|  _ \| | __/ _  |          "
echo "  _| || (_) | | | | | || (_| |          "
echo "  \___/\___/|_| |_|_|\__\__,_|          "
echo "                                        "
echo "                                        "

echo "  Welcome!"
echo "  this script can be used to run the first project for the course Systems and Architectures for Big Data at University of Rome Tor Vergata"
echo "  Note that this script is REALLY interactive!"
echo "  Before starting, check if the next path are set in your terminal:"
echo "  \$HADOOP_HOME"
echo "  \$SPARK_HOME"
echo "  \$NIFI_HOME (optional)"
echo "  \$MONGO_HOME"
echo "  as you can see this project needs that hadoop, spark and mongoDB are installed on you computer." 
echo "  This script must be in the same directory as the following files:"
echo "  d14_filtered.csv"
echo "  SABDanielloIonita.jar"
echo "  saveOnMongoDB_{os}.sh"
echo "  saveOnHDFS.xml (optional)"
echo "  if you haven't already started frameworks required there's no need to do it now. This script will do it for you."
echo ""
echo ""

jarfinished=false
uselocal=false

echo "What path do you want to use for HDFS?"
echo "Type m to use 'hdfs://master:54310'"
echo "Typle n to use 'hdfs://local:9000'"
read choice

case "$choice" in
	m)
		uselocal=false
		;;
	n)
		uselocal=true
		;;
	*)
		echo "wrong input"
		exit
		;;


echo ""
echo ""
echo "Do you want to start Hadoop? (note that \$HADOOP_HOME must be configured) (y/n)"
read choice

case "$choice" in

    y)
        echo "starting hadoop"
	$HADOOP_HOME/bin/hdfs namenode -format
	$HADOOP_HOME/sbin/start-dfs.sh
        ;;
    n)
        echo "ok, let's procede"
        ;;
    *)
        echo "$choice is not a valid choice, not starting hadoop"
        ;;
esac

echo ""
echo ""
echo "Do you want to create directories inputDirectory and outputDirectory on HDFS? (note that \$HADOOP_HOME must be configured) (y/n)"
read choice

case "$choice" in

    y)
        echo "creating output directory"
	$HADOOP_HOME/bin/hdfs dfs -mkdir /outputDirectory
	echo "creating input directory"
	$HADOOP_HOME/bin/hdfs dfs -mkdir /inputDirectory
        ;;
    n)
        echo "ok, let's procede"
        ;;
    *)
        echo "$choice is not a valid choice, not creating hdfs directories"
        ;;
esac

echo ""
echo ""
echo "Before continuing, do you want to inject data with hdfs-put? The alternative is to use apache NIFI (xml in the current directory). The csv must be in the current directory and must be called d14_filtered.csv (y/n)"
read choice

case "$choice" in

    y)
        echo "Injecting data"
	if [uselocal=false]
	then
        $HADOOP_HOME/bin/hdfs dfs -put d14_filtered.csv hdfs://master:54310/inputDirectory/
        echo "now it's time to run the jar!"
        $SPARK_HOME/bin/spark-submit --class "runner.StructureHouses" --master "local" SABDanielloIonita.jar hdfs://master:54310/inputDirectory/d14_filtered.csv hdfs://master:54310/outputDirectory
        jarfinished=true
	fi

	if [uselocal=true]
	then
        $HADOOP_HOME/bin/hdfs dfs -put d14_filtered.csv hdfs://localhost:9000/inputDirectory/
        echo "now it's time to run the jar!"
        $SPARK_HOME/bin/spark-submit --class "runner.StructureHouses" --master "local" SABDanielloIonita.jar hdfs://localhost:9000/inputDirectory/d14_filtered.csv hdfs://localhost:9000/outputDirectory
        jarfinished=true
        fi

        ;;
    n)
	echo ""
	echo ""
        echo "Do you want to start apache NIFI ? note that it may take some time for NiFi to start (\$NIFI_HOME must be configured) (y/n)"
		read choice
		case "$choice" in

    		y)
        		echo "starting nifi"
			$NIFI_HOME/bin/nifi.sh start
			echo "apache Nifi started"
			;;
    		n)
        		echo "ok"
     		   	;;
    		*)
        		echo "$choice is not a valid choice, not starting NIFI"
        	;;
		esac
        echo "now I'm waiting. Run the template saveOnHDFS.xml on Apache NIFI"
        echo "Type a character to continue"
        read choice
        case "$choice" in
        *)
        echo "Now jar is running computing query results. Wait the computation"
       	 	if [uselocal=false]
         	then
            	$SPARK_HOME/bin/spark-submit --class "runner.StructureHouses" --master "local" SABDanielloIonita.jar hdfs://master:54310/inputDirectory/d14_filtered.csv hdfs://master:54310/outputDirectory
            	jarfinished=true
		fi

		if [uselocal=true]
                then
                $SPARK_HOME/bin/spark-submit --class "runner.StructureHouses" --master "local" SABDanielloIonita.jar hdfs://localhost:9000/inputDirectory/d14_filtered.csv hdfs://master:54310/outputDirectory
                jarfinished=true
                fi

            ;;
        esac
        ;;
    *)
        echo "$choice is not a valid choice, not injecting data"
        ;;
esac

echo ""
echo ""
if [jarfinished=true]
then
    echo "now I'm waiting. Run the template retrieveOnHDFS.xml on Apache NIFI"
    echo "type a m if you're running from OSX, n if on linux "
    read choice
    case "$choice" in
        m)
            echo "saving data on mongoDB"
            saveOnMongoDB_OSX.sh
        ;;
        n)
            echo "saving data on mongoDB"
            saveOnMongoDB_linux.sh
        ;;
    esac
fi




if [ "$1" = "eod" ]
then
        filename=${REPORT_DIR}/sync/report_pnl_eod.csv
        fileId="1VhJ27xc_pZU-yKzF_UdCUF1HJiCN2BWa"
else
        filename=${REPORT_DIR}/sync/report_pnl.csv
        fileId="10OTCW6SMOeqzxlXJoRB6x9Qp-vfeqKG-"
fi


sh $REPORT_DIR/getTrades-view.sh > $REPORT_DIR/pnl/prodtrades.csv
cd $REPORT_DIR/pnl/
java -jar odyssean-pnl-1.0-SNAPSHOT-jar-with-dependencies.jar
cp $REPORT_DIR/pnl/PNLReport.csv $filename
gdrive update $fileId $filename


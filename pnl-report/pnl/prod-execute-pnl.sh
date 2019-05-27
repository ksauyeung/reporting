sh /fs/env/prod/bin/getTrades-import.sh
cp /home/od-prod/env/report/sync/prodtrade.csv /home/od-prod/env/report/pnl/prodtrades.csv
cd /home/od-prod/env/report/pnl/
java -jar odyssean-pnl-1.0-SNAPSHOT-jar-with-dependencies.jar
rm -f /home/od-prod/env/report/sync/PNLReport.csv
cp /home/od-prod/env/report/pnl/PNLReport.csv /home/od-prod/env/report/sync/PNLReport.csv
rm -f /home/od-prod/env/report/sync/PNLReportBySymbol.csv
cp /home/od-prod/env/report/pnl/PNLReportBySymbol.csv /home/od-prod/env/report/sync/PNLReportBySymbol.csv
gdrive update 1yMpqKJtGk-OgUb1QPI1hUWd2LqUrn-za /home/od-prod/env/report/sync/PNLReport.csv
gdrive update 1yMpqKJtGk-OgUb1QPI1hUWd2LqUrn-za /home/od-prod/env/report/sync/PNLReportBySymbol.csv

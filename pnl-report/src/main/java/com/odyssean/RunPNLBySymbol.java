package main.java.com.odyssean;

import com.opencsv.CSVWriter;
import main.java.com.odyssean.pojo.PNLRawData;
import main.java.com.odyssean.util.PNLUtils;
import org.apache.log4j.Logger;

import java.io.*;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;


public class RunPNLBySymbol extends PNLBase{
    final static Logger logger = Logger.getLogger(RunPNL.class);
    private static final int ROUNDING_SCALE = 20;

    private static final String BELONGING_PAIR_DEFAULT = "initial";
    private static final String REPORT_FILE_NAME = "PNLReportBySymbol.csv";

    private static Map<String, String> openingTradeSide = new HashMap<>();
    private static Map<String, String> waveSymbol = new HashMap<>();
    private static String SIDE_SELL = "SELL";
    private static String SIDE_BUY = "BUY";
    private static Object syncObject = new Object();
    private static String RPNL = "RPnL";

    private static String YES = "Y";
    private static String NO = "N";

    private static String JOIN_SYMBOLS = "@@@";

    private static SortedSet<PNLRawData> oneSidedTrades = new TreeSet<>();

    public static void main(String args[]) throws Exception{

        buildRpnlSumMap();

        logger.info("");
        logger.info("");
        logger.info("PNL Started");


        logger.info("Loading csv into memory Started");

        SortedSet<PNLRawData> rawList = loadCSV();

        logger.info("Loading csv into memory Ended");
        logger.info("Loaded: "+ rawList.size() + " trades.");


        // loadUpPairs
        loadUpPairs(rawList);

        logger.info("Grouping by Symbol Started");

        Map<String, SortedSet<PNLRawData>> sybolList = groupBySymbolList(rawList);

        logger.info("Grouping by Symbol Ended");

        logger.info("Loaded: "+ sybolList.size() + " hedges.");

        logger.info("Generating Report Started");

        String reportName = generateReportCsv(sybolList);

        logger.info("Generating Report Ended");
        logger.info("Report: " + reportName);


        logger.info("PNL Ended");
        logger.info("");
        logger.info("");

    }


    private static SortedSet<PNLRawData> loadCSV() throws CloneNotSupportedException {

        List<PNLRawData> list = processInputFile("prodtrades.csv");

        //TreeSet<PNLRawData> set = new TreeSet<>(list);
        //TreeSet<PNLRawData> set = list.stream().collect(Collectors.toCollection(TreeSet::new));

        // only extract values with hege ids the rest does not count
        // btw the sort is a string sort
        TreeSet<PNLRawData> manualSet = new TreeSet<>();
        for (PNLRawData data : list){
            String hedge_id = data.getHedge_id();
            if (hedge_id.length() !=0) {

                // define numeric values to get sorting
                //data.setHedgeId(Long.parseLong(data.getHedge_id()));
                data.setWaveId(Long.parseLong(data.getWave_id()));
                data.setExecutionId(Long.parseLong(data.getExecution_id()));

                data.setBelongingPair(BELONGING_PAIR_DEFAULT);

                manualSet.add(data);
            }
        }

        return manualSet;

    }

    private static List<PNLRawData> processInputFile(String inputFilePath){
        List<PNLRawData> inputList = new ArrayList<>();
        try{
            File inputF = new File(inputFilePath);
            InputStream inputFS = new FileInputStream(inputF);
            BufferedReader br = new BufferedReader(new InputStreamReader(inputFS));
            // skip the header of the csv
            inputList = br.lines().skip(1).map(mapToItem).collect(Collectors.toList());
            br.close();
        } catch (FileNotFoundException e) {
            System.out.println("FileNotFoundException: " + e);
        } catch (Exception e) {
            System.out.println("Exception: " + e);
        }
        return inputList ;
    }


    private static Function<String, PNLRawData> mapToItem = (line) -> {
        String[] p = line.split(",",-1);// a CSV has comma separated lines, the -1 guarantees that all parts will show up even when they have no data, otherwise they get dropped
        PNLRawData item = new PNLRawData();
        item.setExecution_id(p[0]);//<-- this is the first column in the csv file
        item.setOrder_id(p[1]);
        if (p[2] != null && p[2].trim().length() > 0) {
            item.setDeal_id(p[2]);
        }
        item.setWave_id(p[3]);
        item.setExchange_trade_id(p[4]);
        item.setExecution_time(p[5]);
        item.setExchange_execution_id(p[6]);
        item.setExecuted_price(p[7]);
        item.setExecuted_size(p[8]);
        item.setFees(p[9]);
        item.setFees_currency(p[10]);
        item.setAdapter_id(p[11]);
        item.setStrategy_id(p[12]);
        item.setAccount_id(p[13]);
        item.setExchange_order_id(p[14]);
        item.setSymbol(p[15]);
        item.setSide(p[16]);
        item.setOrder_price(p[17]);
        item.setOrder_size(p[18]);
        item.setCcy_1(p[19]);
        item.setCcy_2(p[20]);
        item.setCcy2_src(p[21]);
        item.setUnderlying_index(p[22]);
        item.setUnderlying_index_src(p[23]);
        item.setHedge_id(p[24]);
        return item;
    };


    private static void loadUpPairs(SortedSet<PNLRawData> list) {

        for (PNLRawData data : list) {
            String runningSymbol = data.getSymbol();
            String runningWave = data.getWave_id();
            if (waveSymbol.keySet().contains(runningWave)) {
                String alreadyIn = waveSymbol.get(runningWave);
                if (!alreadyIn.contains(runningSymbol)) {
                    waveSymbol.put(runningWave, alreadyIn + JOIN_SYMBOLS + runningSymbol);
                }
            } else {
                waveSymbol.put(runningWave, runningSymbol);
            }

        }

        Set<String> waveIds = waveSymbol.keySet();

        for (String waveId : waveIds){
            String symbols = waveSymbol.get(waveId);
            String[] pairsArray = symbols.split(JOIN_SYMBOLS);
            Arrays.sort(pairsArray);
            waveSymbol.put(waveId, String.join(JOIN_SYMBOLS, pairsArray));
        }

        // set back upstream
        for (PNLRawData data : list) {
            data.setBelongingPair(waveSymbol.get(data.getWave_id()));
        }
    }

    //

    private static Map<String, SortedSet<PNLRawData>> groupBySymbolList(SortedSet<PNLRawData> list) {

        Map<String, SortedSet<PNLRawData>> map = new HashMap<>();

        for (PNLRawData row : list){

            String pairTrading = row.getBelongingPair();

            if (map.containsKey(pairTrading)){

                SortedSet<PNLRawData> treeSet = map.get(pairTrading);
                treeSet.add(row);

            } else {

                SortedSet<PNLRawData> treeSet = new TreeSet<>();
                treeSet.add(row);

                map.put(pairTrading, treeSet);

                openingTradeSide.put(pairTrading, row.getSide());

                // set post close position size initial
                row.setPostTradePositionSize(row.getExecuted_size());
                // set trade counter
                row.setTradeCount("1");
                row.setOc("O");
                row.setFlipsPosition("N");
                row.setCumOpenCoins(calculateCoinCount(row));
                row.setWtdAvg(row.getExecuted_price());

            }
            calculateExecutedSize(row);

        }

        return map;
    }


    private static String generateReportCsv(Map<String, SortedSet<PNLRawData>> map) {
        // loop through the hedge ids
        // loop through the wave ids
        // get side A and Side B collection
        // figure out which one is larger use that as the counter for the loop
        // print new line after every row


        // first create file object for file placed at location
        // specified by filepath
        String exportPath = REPORT_FILE_NAME;
        File file = new File(exportPath);
        try {
            // create FileWriter object with file as parameter
            FileWriter outputfile = new FileWriter(file);

            // create CSVWriter object filewriter object as parameter
            CSVWriter writer = new CSVWriter(outputfile);

            // adding header to csv
            String[] header = {"Pair", "Hedge_id", "strategy_id", "deal_id", "wave_id", "side", "symbol", "Size", "Coins", "Price", "Time", "Post Size", "trade count", "oc", "Flips Position", "CL trade size ctcs", "CL trade size coin", "Opening Trade Size Coins", "cum. open coins", "wtd avg", "RPnL, coin", "RPnL USD"};
            writer.writeNext(header);

            // add data to csv
            // sort
            SortedSet<String> symbolKeys = new TreeSet<>(map.keySet());

            for (String pairTrading : symbolKeys) {

                SortedSet<PNLRawData> treeSet = map.get(pairTrading);


                PNLRawData prevDataA = null;


                Iterator<PNLRawData> dataSetSideAIterator = Collections.<PNLRawData>emptyList().iterator();

                dataSetSideAIterator = treeSet.iterator();


                String[] dataRow = new String[header.length];

                PNLRawData dataA = new PNLRawData();

                while (dataSetSideAIterator.hasNext()) {

                    dataA = dataSetSideAIterator.next();

                    dataA.setFlipsPosition(NO);
                    if (prevDataA != null) {
                        if (new BigDecimal(prevDataA.getPostTradePositionSize()).compareTo(BigDecimal.ZERO) == 0) {
                            openingTradeSide.put(dataA.getBelongingPair(), dataA.getSide());
                        }
                        if (isFlipPosition(prevDataA, dataA)) {
                            openingTradeSide.put(dataA.getBelongingPair(), dataA.getSide());
                        }
                    }
                    calculateExecutedSize(dataA);


                    int index = 0;
                    dataRow[index++] = dataA.getBelongingPair();
                    dataRow[index++] = dataA.getHedge_id();
                    dataRow[index++] = dataA.getStrategy_id();
                    dataRow[index++] = dataA.getDeal_id();
                    dataRow[index++] = dataA.getWave_id();
                    dataRow[index++] = dataA.getSide();
                    dataRow[index++] = dataA.getSymbol();
                    dataRow[index++] = dataA.getExecuted_size();
                    dataRow[index++] = calculateCoinCount(dataA);
                    dataRow[index++] = dataA.getExecuted_price();
                    dataRow[index++] = replaceNull(dataA.getExecution_time()).replace("\"", "");
                    dataRow[index++] = calculatePostTradePositionSize(prevDataA, dataA);
                    dataRow[index++] = calculateTradeCount(prevDataA, dataA);
                    dataRow[index++] = calculateOC(prevDataA, dataA);
                    dataRow[index++] = dataA.getFlipsPosition();
                    dataRow[index++] = calculateCLTradeSizeCTCS(prevDataA, dataA);
                    dataRow[index++] = calculateCLTradeSizeCoin(dataA);
                    dataRow[index++] = calculateOpeningTradeSizeCoins(dataA);
                    String pnlA = calculatePNL(prevDataA, dataA); // pnl calc needed for cum open coins
                    dataRow[index++] = calculateCumOpenCoins(prevDataA, dataA);
                    dataRow[index++] = calculateWeightedAvg(prevDataA, dataA);
                    ;
                    dataRow[index++] = pnlA;
                    dataRow[index++] = calculatePNLUSD(pnlA, dataA);


                    writer.writeNext(dataRow);

                    // do not set empty rows as previous
                    if (dataA.getWave_id() != null) {
                        prevDataA = dataA;
                    }
                }

            }


            // closing writer connection
            writer.close();
        }
        catch (IOException e) {
            logger.error("error generating export", e);
        }

        return file.getName();

    }

    private static String calculateExecutedSize(PNLRawData data) {
        if (data.getExecuted_size() == null) {
            return null;
        }
        BigDecimal current = new BigDecimal(data.getExecuted_size());
        BigDecimal sign = data.getSide().equalsIgnoreCase(openingTradeSide.get(data.getBelongingPair())) ? BigDecimal.ONE : new BigDecimal("-1");
        BigDecimal finalTotal = sign.multiply(current.abs()); // doing abs since the sign above dictates now position
        data.setExecuted_size(finalTotal.toString());
        return finalTotal.toString();
    }



    private static String calculatePNLUSD(String pnl, PNLRawData data) {
        if (pnl == null || pnl.equals("") || pnl.equals("0")){
            return "0";
        } else {
            return new BigDecimal(pnl).multiply(new BigDecimal(data.getUnderlying_index())).toString();
        }
    }

    private static boolean isFlipPosition(PNLRawData prevData, PNLRawData data) {

        // prev data should not be null here

        if (!openingTradeSide.get(data.getBelongingPair()).equalsIgnoreCase(data.getSide())){
            long prevPositionSize = new BigDecimal(prevData.getPostTradePositionSize()).abs().longValue();
            long positionSize = new BigDecimal(data.getExecuted_size()).abs().longValue();

            String val = prevPositionSize<positionSize?YES:NO;


            data.setFlipsPosition(val);
            return val.equalsIgnoreCase(YES);
        }
        return false;
    }

    private static String calculatePNL(PNLRawData prevData, PNLRawData data) {


        if (data.getExecuted_size() == null) {
            return null;
        }

        if (prevData != null) {

            String result ="0";

            if(data.getSide().equalsIgnoreCase(SIDE_BUY)){
                if (data.getOc().equalsIgnoreCase(RPNL)){
                    BigDecimal ctcs = new BigDecimal(data.getClTradeSizeCTCS());
                    BigDecimal executedPrice = new BigDecimal(data.getExecuted_price());
                    BigDecimal prevWeightAvg = new BigDecimal(prevData.getWtdAvg());
                    BigDecimal left = ctcs.multiply(getRpnlSum(data)).divide(executedPrice, ROUNDING_SCALE, BigDecimal.ROUND_HALF_UP);
                    BigDecimal right = new BigDecimal("-1").multiply(ctcs).multiply(getRpnlSum(data)).divide(prevWeightAvg, ROUNDING_SCALE, BigDecimal.ROUND_HALF_UP);
                    result = left.add(right).toString();
                }
            } else {
                if (data.getOc().equalsIgnoreCase(RPNL)){
                    BigDecimal ctcs = new BigDecimal(data.getClTradeSizeCTCS());
                    BigDecimal executedPrice = new BigDecimal(data.getExecuted_price());
                    BigDecimal prevWeightAvg = new BigDecimal(prevData.getWtdAvg());
                    BigDecimal left = new BigDecimal("-1").multiply(ctcs).multiply(getRpnlSum(data)).divide(executedPrice, ROUNDING_SCALE, BigDecimal.ROUND_HALF_UP);
                    BigDecimal right = ctcs.multiply(getRpnlSum(data)).divide(prevWeightAvg, ROUNDING_SCALE, BigDecimal.ROUND_HALF_UP);
                    result = left.add(right).toString();
                }
            }

            data.setRPnLCoin(result);
            return result;
        } else {
            return "0";
        }
    }

    private static String calculateWeightedAvg(PNLRawData prevData, PNLRawData data) {
        if (data.getExecuted_size() == null) {
            return null;
        }

        if (prevData != null) {
            BigDecimal postTradePositionSize = new BigDecimal(data.getPostTradePositionSize());

            String result;
            if (postTradePositionSize.equals(BigDecimal.ZERO)){
                result = BigDecimal.ZERO.toString();
            } else {
                BigDecimal prevPostTradePositionSize = new BigDecimal(prevData.getPostTradePositionSize());

                BigDecimal executedPrice = new BigDecimal(data.getExecuted_price());
                if (prevPostTradePositionSize.equals(BigDecimal.ZERO)) {
                    result = executedPrice.toString();
                } else {
                    if (data.getFlipsPosition().equalsIgnoreCase(YES)){
                        result = executedPrice.toString();
                    } else {
                        if (data.getOc().equalsIgnoreCase(RPNL)){
                            result = prevData.getWtdAvg();
                        } else {

                            BigDecimal prevCumOpenCoins = new BigDecimal(prevData.getCumOpenCoins());
                            BigDecimal prevWtdAvg = new BigDecimal(prevData.getWtdAvg());

                            BigDecimal prev = prevCumOpenCoins.multiply(prevWtdAvg);

                            BigDecimal openingTradeSizeCoins = new BigDecimal(data.getOpeningTradeSizeCoins());

                            BigDecimal current = executedPrice.multiply(openingTradeSizeCoins);

                            BigDecimal cumOpenCoins = new BigDecimal(data.getCumOpenCoins());

                            // in case coin sizes add up perfectly
                            if ((openingTradeSizeCoins.add(prevCumOpenCoins)).compareTo(BigDecimal.ZERO)==0){
                                result = "0";
                            } else {
                                result = prev.add(current).divide(cumOpenCoins, ROUNDING_SCALE, BigDecimal.ROUND_HALF_UP).toString();
                            }
                        }
                    }
                }
            }
            data.setWtdAvg(result);
            return result;
        } else {
            // first is already set
            return data.getWtdAvg();
        }

    }

    private static String calculateCumOpenCoins(PNLRawData prevData, PNLRawData data) {

        if (data.getExecuted_size() == null) {
            return null;
        }

        if (prevData != null) {
            BigDecimal openingTradeSizeCoins = new BigDecimal(data.getOpeningTradeSizeCoins());
            BigDecimal prevCumOpenCoins = new BigDecimal(prevData.getCumOpenCoins());
            BigDecimal clTradeSizeCoin = new BigDecimal(data.getClTradeSizeCoin());
            BigDecimal RPnLCoin = new BigDecimal(data.getRPnLCoin()==null? "0" : data.getRPnLCoin());

            String rslt = openingTradeSizeCoins.add(prevCumOpenCoins).subtract(clTradeSizeCoin).subtract(RPnLCoin).toString();
            data.setCumOpenCoins(rslt);
            return rslt;
        } else {
            // first is already set
            return data.getCumOpenCoins();
        }

    }

    private static String calculateOpeningTradeSizeCoins(PNLRawData data) {
        if (data.getExecuted_size() == null) {
            return null;
        }
        String openingCoinStr = data.getClTradeSizeCoin();
        String sumCoinStr = data.getSumCoin();

        BigDecimal openingCoin = BigDecimal.ZERO;
        if (!PNLUtils.isNullOrBlank(openingCoinStr)) {
            openingCoin = new BigDecimal(openingCoinStr);
        }


        BigDecimal sumCoin = BigDecimal.ZERO;
        if (!PNLUtils.isNullOrBlank(sumCoinStr)) {
            sumCoin = new BigDecimal(sumCoinStr);
        }


        BigDecimal val = openingCoin.add(sumCoin);
        data.setOpeningTradeSizeCoins(val.toString());
        return val.toString();

    }

    private static String calculateCLTradeSizeCoin(PNLRawData data) {
        if (data.getExecuted_size() == null) {
            return null;
        }
        String ctcs = data.getClTradeSizeCTCS();
        if (ctcs != null && !ctcs.equalsIgnoreCase("")) {
            BigDecimal ctcsD = new BigDecimal((ctcs));
            if (ctcsD.compareTo(BigDecimal.ZERO)>0) {
                BigDecimal executedPrice = new BigDecimal(data.getExecuted_price());
                BigDecimal val = getRpnlSum(data).multiply(ctcsD).divide(executedPrice, ROUNDING_SCALE, BigDecimal.ROUND_HALF_UP);
                data.setClTradeSizeCoin(val.toString());
                return val.toString();
            }
        }
        data.setClTradeSizeCoin("0"); // set to zero for other calcs
        return "0";
    }

    private static String calculateCLTradeSizeCTCS(PNLRawData prevData, PNLRawData data) {
        if (data.getExecuted_size() == null) {
            return null;
        }
        if (data.getOc() != null && data.getOc().equalsIgnoreCase(RPNL)) {
            long val = Math.min(
                    Math.abs(new BigDecimal((data.getExecuted_size())).longValue()),
                    Math.abs(new BigDecimal(prevData.getPostTradePositionSize()).longValue()));
            data.setClTradeSizeCTCS(""+val);
            return ""+val;
        }
        return "0";
    }

    private static String calculateTradeCount(PNLRawData prevData, PNLRawData data) {
        if (data.getExecuted_size() == null) {
            return null;
        }
        // note prev its set to null at every new hedge id. Also the first does not need it.

        if (prevData != null) {
            int prevPositionSize = new BigDecimal(prevData.getPostTradePositionSize()).abs().intValue();
            int positionSize = new BigDecimal(data.getPostTradePositionSize()).abs().intValue();
            int prevTradeCount = Integer.parseInt(prevData.getTradeCount());
            int tradeCount = prevTradeCount;

            if (prevPositionSize>=positionSize){
                tradeCount = 0;
            } else {
                tradeCount++;
            }
            data.setTradeCount(""+tradeCount);
            return "" +tradeCount;
        } else {
            // first is already set
            return data.getTradeCount();
        }
    }

    private static String calculateOC(PNLRawData prevData, PNLRawData data) {
        // this makes sure its an active row that has data
        if (data.getExecuted_size() == null) {
            return null;
        }
        // note prev its set to null at every new hedge id. Also the first does not need it.

        if (prevData != null) {
            String ocText = "O";
            boolean decreaseInPosition = new BigDecimal(prevData.getPostTradePositionSize()).subtract(new BigDecimal(data.getPostTradePositionSize())).intValue() > 0;
            if (decreaseInPosition || data.getFlipsPosition().equalsIgnoreCase(YES)){
                ocText = RPNL;
            }
            data.setOc(ocText);
            return ocText;
        } else {
            // first is already set
            return data.getOc();
        }
    }

    private static String calculatePostTradePositionSize(PNLRawData prevData, PNLRawData data) {
        if (data.getExecuted_size() == null) {
            return null;
        }
        // note prev its set to null at every new hedge id. Also the first does not need it.
        if (prevData != null) {
            BigDecimal runningTotal = new BigDecimal(prevData.getPostTradePositionSize());
            BigDecimal current = new BigDecimal(data.getExecuted_size());

            BigDecimal finalTotal;
            if (!data.getFlipsPosition().equalsIgnoreCase(YES)) {
                finalTotal = runningTotal.add(current);
            } else {
                finalTotal = runningTotal.subtract(current).abs();
            }
            String finalTotalStr = finalTotal.toString();
            data.setPostTradePositionSize(finalTotalStr);
            return finalTotalStr;
        } else {
            return data.getPostTradePositionSize();
        }

    }

    private static String calculateCoinCount(PNLRawData data) {

        String executed_size = data.getExecuted_size();
        String executed_price = data.getExecuted_price();

        if (PNLUtils.isNullOrBlank(executed_size) || PNLUtils.isNullOrBlank(executed_price)){
            return "";
        }

        if (isSpot(data)){
            data.setSumCoin(executed_size);
            return executed_size;
        }

        String val = (new BigDecimal(executed_size))
                .divide(new BigDecimal(executed_price) , ROUNDING_SCALE, RoundingMode.HALF_UP).multiply(getRpnlSum(data))
                .toString();
        data.setSumCoin(val);
        return val;
    }




}

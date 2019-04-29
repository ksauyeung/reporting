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


public class RunPNL {

    final static Logger logger = Logger.getLogger(RunPNL.class);
    private static final String SIDE_A = "A";
    private static final String SIDE_B = "B";
    private static final int ROUNDING_SCALE = 20;
    private static final String OKEXFUTURE_ADAPTER = "OKEXFUTURE_ADAPTER";
    private static final String DERABIT_ADAPTER = "DERIBIT_ADAPTER";
    private static final String BITMEX_ADAPTER = "BITMEX_ADAPTER";
    private static final String BELONGING_PAIR_DEFAULT = "initial";

    private static Map<String, String> openingTradeSide = new HashMap<>();
    private static Map<String, String> openingHedgeSymbol = new HashMap<>();
    private static Map<String, String> openingHedgeWave = new HashMap<>();
    private static Map<String, String> waveSymbol = new HashMap<>();
    private static String SIDE_SELL = "SELL";
    private static String SIDE_BUY = "BUY";
    private static Object syncObject = new Object();
    private static String RPNL = "RPnL";

    private static String YES = "Y";
    private static String NO = "N";

    private static HashMap<String, String> sizesMap = new HashMap<>();
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

        weedOutOneSidedTrades(rawList);

        logger.info("After removing one sided: "+ rawList.size() + " trades.");

        // loadUpPairs
        loadUpPairs(rawList);

        logger.info("Grouping by Hedge Id, Wave Id, Report Side Started");

        //(1, (5, ( A, List(obj)))

        Map<String ,Map<String, Map<String, SortedSet<PNLRawData>>>> splitBySideList = groupByHedgeIdWaveIdABSidegroupedList(rawList);


        logger.info("Grouping by Hedge Id, Wave Id, Report Side Ended");
        logger.info("Loaded: "+ splitBySideList.size() + " hedges.");

        logger.info("Generating Report Started");

        String reportName = generateReportCsv(splitBySideList);

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

    private static void weedOutOneSidedTrades(SortedSet<PNLRawData> list) {
        synchronized(syncObject) {

            Set<String> sells = new HashSet<>();
            Set<String> buys = new HashSet<>();


            for (PNLRawData data : list) {
                if (data.getSide().equals(SIDE_SELL)) {
                    sells.add(data.getHedge_id() + "-" + data.getWave_id());
                }
                if (data.getSide().equals(SIDE_BUY)) {
                    buys.add(data.getHedge_id() + "-" + data.getWave_id());
                }
            }
            Iterator<PNLRawData> iter = list.iterator();

            while (iter.hasNext()) {
                PNLRawData data = iter.next();
                if (!(sells.contains(data.getHedge_id() + "-" + data.getWave_id()) && buys.contains(data.getHedge_id() + "-" + data.getWave_id()))) {
                    logger.warn("ONE SIDED TRADE: hedge id: " + data.getHedge_id() + " wave id: " + data.getWave_id() + " execution id: " +  data.getExecution_id() +" REMOVED");
                    oneSidedTrades.add(data);
                    iter.remove();
                }
            }
        }

    }


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
        Collection<String> valsL = waveSymbol.values();
        Set vals = new HashSet<String>(valsL);

        Set<String> waveIds = waveSymbol.keySet();

        for (String waveId : waveIds){
            String symbols = waveSymbol.get(waveId);
            String[] pairsArray = symbols.split(JOIN_SYMBOLS);
            Arrays.sort(pairsArray);
            waveSymbol.put(waveId, String.join(JOIN_SYMBOLS, pairsArray));
        }

        Collection<String> valsL2 = waveSymbol.values();
        Set vals2 = new HashSet<String>(valsL);

        // set back upstream
        for (PNLRawData data : list) {
            data.setBelongingPair(waveSymbol.get(data.getWave_id()));
        }
    }


    private static Map<String,Map<String,Map<String,SortedSet<PNLRawData>>>> groupByHedgeIdWaveIdABSidegroupedList(SortedSet<PNLRawData> list) {

        Map<String,Map<String,Map<String,SortedSet<PNLRawData>>>> map = new HashMap<>();

        for (PNLRawData row : list){

            String pairTrading = row.getBelongingPair();
            String waveId = row.getWave_id();

            if (map.containsKey(pairTrading)){


                Map<String, Map<String, SortedSet<PNLRawData>>> setSideWaveMap = map.get(pairTrading);

                if (setSideWaveMap.containsKey(waveId)){
                    Map<String, SortedSet<PNLRawData>> setSideMap = setSideWaveMap.get(waveId);

                    String rowSumbol = row.getSymbol();

                    if(rowSumbol.equalsIgnoreCase(openingHedgeSymbol.get(pairTrading))){
                        // we are on side A
                        if (setSideMap.containsKey(SIDE_A)){
                            SortedSet<PNLRawData> treeSet = setSideMap.get(SIDE_A);
                            calculateExecutedSize(row, SIDE_A);
                            treeSet.add(row);
                        } else {
                            SortedSet<PNLRawData> treeSet = new TreeSet<>();
                            treeSet.add(row);
                            calculateExecutedSize(row, SIDE_A);
                            setSideMap.put(SIDE_A, treeSet);
                        }

                    } else {
                        if (setSideMap.containsKey(SIDE_B)){
                            SortedSet<PNLRawData> treeSet = setSideMap.get(SIDE_B);
                            calculateExecutedSize(row, SIDE_B);
                            treeSet.add(row);
                        } else {
                            SortedSet<PNLRawData> treeSet = new TreeSet<>();
                            //calculateExecutedSize(row, SIDE_B);
                            treeSet.add(row);
                            setSideMap.put(SIDE_B, treeSet);

                            if (openingHedgeWave.get(pairTrading).equalsIgnoreCase(waveId)){
                                openingTradeSide.put(SIDE_B+pairTrading, row.getSide());

                                //row.setPostTradePositionSize(row.getExecuted_size());
                                row.setPostTradePositionSize(new BigDecimal(row.getExecuted_size()).toString());
                                row.setTradeCount("1");
                                row.setOc("O");
                                row.setFlipsPosition("N");
                                row.setCumOpenCoins(calculateCoinCount(row));
                                row.setWtdAvg(row.getExecuted_price());
                            }
                            calculateExecutedSize(row, SIDE_B);
                        }
                    }


                } else {

                    String openingHedgeSymbolData = openingHedgeSymbol.get(pairTrading);

                    String reocrdSide = SIDE_A;
                    calculateExecutedSize(row, SIDE_A);
                    if(!openingHedgeSymbolData.equalsIgnoreCase(row.getSymbol())){
                        reocrdSide = SIDE_B;

                        if (openingHedgeWave.get(pairTrading).equalsIgnoreCase(waveId)){

                            openingTradeSide.put(SIDE_B+pairTrading, row.getSide());

                            row.setPostTradePositionSize(new BigDecimal(row.getExecuted_size()).toString());
                            row.setTradeCount("1");
                            row.setOc("O");
                            row.setFlipsPosition("N");
                            row.setCumOpenCoins(calculateCoinCount(row));
                            row.setWtdAvg(row.getExecuted_price());
                        }
                        calculateExecutedSize(row, SIDE_B);
                    }

                    SortedSet<PNLRawData> treeSet = new TreeSet<>();
                    treeSet.add(row);

                    Map<String, SortedSet<PNLRawData>> setSideMap = new HashMap<>();
                    setSideMap.put(reocrdSide, treeSet); // note this is the very first transaction that dictates side A and opening trade.


                    setSideWaveMap.put(waveId, setSideMap);


                }


            } else {

                SortedSet<PNLRawData> treeSet = new TreeSet<>();
                treeSet.add(row);

                Map<String, SortedSet<PNLRawData>> setSideMap = new HashMap<>();
                setSideMap.put(SIDE_A, treeSet); // note this is the very first transaction that dictates side A and opening trade.

                Map<String, Map<String, SortedSet<PNLRawData>>> setSideWaveMap = new HashMap<>();
                setSideWaveMap.put(waveId, setSideMap);

                map.put(pairTrading, setSideWaveMap);

                openingTradeSide.put(SIDE_A+pairTrading, row.getSide());
                openingHedgeWave.put(pairTrading, row.getWave_id());
                openingHedgeSymbol.put(pairTrading, row.getSymbol());

                // set post close position size initial
                row.setPostTradePositionSize(row.getExecuted_size());
                // set trade counter
                row.setTradeCount("1");
                row.setOc("O");
                row.setFlipsPosition("N");
                row.setCumOpenCoins(calculateCoinCount(row));
                row.setWtdAvg(row.getExecuted_price());

            }
        }

        return map;
    }



    private static String generateReportCsv(Map<String, Map<String, Map<String, SortedSet<PNLRawData>>>> hedgeWaveSideMap) {
        // loop through the hedge ids
        // loop through the wave ids
        // get side A and Side B collection
        // figure out which one is larger use that as the counter for the loop
        // print new line after every row


        // first create file object for file placed at location
        // specified by filepath
        String filename = "PNLReport.csv";
        String exportPath = filename;
        File file = new File(exportPath);
        try {
            // create FileWriter object with file as parameter
            FileWriter outputfile = new FileWriter(file);

            // create CSVWriter object filewriter object as parameter
            CSVWriter writer = new CSVWriter(outputfile);

            // adding header to csv
            String[] header = {"Pair", "Hedge_id", "strategy_id", "deal_id", "wave_id", "side", "symbol", "Size","Coins","Price","Time","Post Size","trade count","oc","Flips Position","CL trade size ctcs","CL trade size coin","Opening Trade Size Coins","cum. open coins","wtd avg","RPnL, coin","RPnL USD","|||", "Pair", "Hedge_id", "strategy_id", "deal_id", "wave_id", "side", "symbol", "Size", "Coins", "Price", "Time", "Post Size","trade count","oc","Flips Position", "CL trade size ctcs","CL trade size coin", "Opening Trade Size Coins","cum. open coins","wtd avg","RPnL, coin","RPnL USD"};
            writer.writeNext(header);

            // add data to csv
            // sort
            SortedSet<String> hedgeKeys =  new TreeSet<>(hedgeWaveSideMap.keySet());

            for (String pairTrading : hedgeKeys){
                Map<String, Map<String, SortedSet<PNLRawData>>> waveAndSideMap = hedgeWaveSideMap.get(pairTrading);

                SortedSet<Long> waveKeys = new TreeSet<>(waveAndSideMap.keySet().stream()
                        .map(s -> Long.parseLong(s))
                        .collect(Collectors.toSet()));


                PNLRawData prevDataA = null;
                PNLRawData prevDataB = null;

                for (Long waveId : waveKeys) {
                    Map<String, SortedSet<PNLRawData>> sideMap = waveAndSideMap.get(waveId.toString());


                    SortedSet<PNLRawData> dataSetSideA = sideMap.get(SIDE_A);
                    SortedSet<PNLRawData> dataSetSideB = sideMap.get(SIDE_B);

                    int aSize = 0;
                    Iterator<PNLRawData> dataSetSideAIterator = Collections.<PNLRawData>emptyList().iterator();
                    if (dataSetSideA != null){
                        aSize = dataSetSideA.size();
                        dataSetSideAIterator = dataSetSideA.iterator();
                    }

                    int bSize = 0;
                    Iterator<PNLRawData> dataSetSideBIterator = Collections.<PNLRawData>emptyList().iterator();
                    if (dataSetSideB != null){
                        bSize = dataSetSideB.size();
                        dataSetSideBIterator = dataSetSideB.iterator();
                    }

                    int maxRowsPerHedge = Math.max(aSize, bSize);

                    String[] dataRow = new String[header.length];

                    for (int i = 0; i < maxRowsPerHedge; i++) {
                        PNLRawData dataA = new PNLRawData();
                        PNLRawData dataB = new PNLRawData();

                        if (dataSetSideAIterator.hasNext()) {
                            dataA = dataSetSideAIterator.next();

                            dataA.setFlipsPosition(NO);
                            if(prevDataA != null){
                                if (new BigDecimal(prevDataA.getPostTradePositionSize()).compareTo(BigDecimal.ZERO) == 0){
                                    openingTradeSide.put(SIDE_A+dataA.getBelongingPair(), dataA.getSide());
                                }
                                if(isFlipPosition(prevDataA, dataA, SIDE_A)){
                                    openingTradeSide.put(SIDE_A+dataA.getBelongingPair(), dataA.getSide());
                                }
                            }
                            calculateExecutedSize(dataA, SIDE_A);
                        }

                        if (dataSetSideBIterator.hasNext()) {
                            dataB = dataSetSideBIterator.next();

                            dataB.setFlipsPosition(NO);
                            if(prevDataB != null){
                                if (new BigDecimal(prevDataB.getPostTradePositionSize()).compareTo(BigDecimal.ZERO) == 0){
                                    openingTradeSide.put(SIDE_B+dataB.getBelongingPair(), dataB.getSide());
                                }
                                if (isFlipPosition(prevDataB, dataB, SIDE_B)){
                                    openingTradeSide.put(SIDE_B+dataB.getBelongingPair(), dataB.getSide());
                                }
                            }
                            calculateExecutedSize(dataB, SIDE_B);
                        }


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
                        dataRow[index++] = calculateTradeCount(prevDataA, dataA, SIDE_A);
                        dataRow[index++] = calculateOC(prevDataA, dataA, SIDE_A);
                        dataRow[index++] = dataA.getFlipsPosition();
                        dataRow[index++] = calculateCLTradeSizeCTCS(prevDataA, dataA);
                        dataRow[index++] = calculateCLTradeSizeCoin(dataA);
                        dataRow[index++] = calculateOpeningTradeSizeCoins(dataA);
                        String pnlA = calculatePNL(prevDataA,dataA); // pnl calc needed for cum open coins
                        dataRow[index++] = calculateCumOpenCoins(prevDataA,dataA);
                        dataRow[index++] = calculateWeightedAvg(prevDataA,dataA);;
                        dataRow[index++] = pnlA;
                        dataRow[index++] = calculatePNLUSD(pnlA, dataA);
                        dataRow[index++] = "|||";
                        dataRow[index++] = dataB.getBelongingPair();
                        dataRow[index++] = dataB.getHedge_id();
                        dataRow[index++] = dataB.getStrategy_id();
                        dataRow[index++] = dataB.getDeal_id();
                        dataRow[index++] = dataB.getWave_id();
                        dataRow[index++] = dataB.getSide();
                        dataRow[index++] = dataB.getSymbol();
                        dataRow[index++] = dataB.getExecuted_size();
                        dataRow[index++] = calculateCoinCount(dataB);
                        dataRow[index++] = dataB.getExecuted_price();
                        dataRow[index++] = replaceNull(dataB.getExecution_time()).replace("\"", "");
                        dataRow[index++] = calculatePostTradePositionSize(prevDataB, dataB);
                        dataRow[index++] = calculateTradeCount(prevDataB, dataB, SIDE_B);
                        dataRow[index++] = calculateOC(prevDataB, dataB, SIDE_B);
                        dataRow[index++] = dataB.getFlipsPosition();
                        dataRow[index++] = calculateCLTradeSizeCTCS(prevDataB, dataB);
                        dataRow[index++] = calculateCLTradeSizeCoin(dataB);
                        dataRow[index++] = calculateOpeningTradeSizeCoins(dataB);
                        String pnlB = calculatePNL(prevDataB,dataB);
                        dataRow[index++] = calculateCumOpenCoins(prevDataB, dataB);
                        dataRow[index++] = calculateWeightedAvg(prevDataB,dataB);
                        dataRow[index++] = pnlB;
                        dataRow[index++] = calculatePNLUSD(pnlB, dataB);


                        writer.writeNext(dataRow);

                        // do not set empty rows as previous
                        if(dataA.getWave_id() !=null) {
                            prevDataA = dataA;
                        }
                        if (dataB.getWave_id() != null) {
                            prevDataB = dataB;
                        }
                    }

                }
            }

            processOneSided(writer, header.length);

            // closing writer connection
            writer.close();
        }
        catch (IOException e) {
            logger.error("error generating export", e);
        }

        return file.getName();

    }

    private static void processOneSided(CSVWriter writer, int rowLength) {

        String[] dataRow = new String[rowLength];
        writer.writeNext(dataRow);
        writer.writeNext(dataRow);
        writer.writeNext(dataRow);

        for (PNLRawData data  : oneSidedTrades){
            int index = 0;

            dataRow = new String[rowLength];

            dataRow[index++] = data.getBelongingPair();
            dataRow[index++] = data.getHedge_id();
            dataRow[index++] = data.getStrategy_id();
            dataRow[index++] = data.getDeal_id();
            dataRow[index++] = data.getWave_id();
            dataRow[index++] = data.getSide();
            dataRow[index++] = data.getSymbol();
            dataRow[index++] = data.getExecuted_size();
            dataRow[index++] = "";
            dataRow[index++] = data.getExecuted_price();
            dataRow[index++] = replaceNull(data.getExecution_time()).replace("\"", "");

            writer.writeNext(dataRow);
        }
    }

    private static String calculatePNLUSD(String pnl, PNLRawData data) {
        if (pnl == null || pnl.equals("") || pnl.equals("0")){
            return "0";
        } else {
            return new BigDecimal(pnl).multiply(new BigDecimal(data.getUnderlying_index())).toString();
        }
    }

    private static boolean isFlipPosition(PNLRawData prevData, PNLRawData data, String side) {

        // prev data should not be null here

        if (!openingTradeSide.get(side+data.getBelongingPair()).equalsIgnoreCase(data.getSide())){
            long prevPositionSize = new BigDecimal(prevData.getPostTradePositionSize()).abs().longValue();
            long positionSize = new BigDecimal(data.getExecuted_size()).abs().longValue();

            String val = prevPositionSize<positionSize?YES:NO;


            data.setFlipsPosition(val);
            return val.equalsIgnoreCase(YES);
        }
        return false;
    }

    private static String calculatePNL(PNLRawData prevData, PNLRawData data) {

        /*

    IF(
        F7 = "BUY",
        IF(
            M7 = "RPnL",
            ( O7 * $N$3 / J7 ) + (- O7 * $N$3 / S6 ),
            0
        ),
        IF(
            M7 = "RPnL",
            (- O7 * $N$3 / J7 ) + ( O7 * $N$3 / S6 ),
            0
        )
    )

         */


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

                            result = prev.add(current).divide(cumOpenCoins, ROUNDING_SCALE, BigDecimal.ROUND_HALF_UP).toString();
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

    private static String calculateTradeCount(PNLRawData prevData, PNLRawData data, String reportSide) {
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

    private static String calculateOC(PNLRawData prevData, PNLRawData data, String reportSide) {
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

    private static String calculateExecutedSize(PNLRawData data, String side) {
        if (data.getExecuted_size() == null) {
            return null;
        }
        BigDecimal current = new BigDecimal(data.getExecuted_size());
        BigDecimal sign = data.getSide().equalsIgnoreCase(openingTradeSide.get(side+data.getBelongingPair())) ? BigDecimal.ONE : new BigDecimal("-1");
        BigDecimal finalTotal = sign.multiply(current.abs()); // doing abs since the sign above dictates now position
        data.setExecuted_size(finalTotal.toString());
        return finalTotal.toString();
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

        String val = (new BigDecimal(executed_size))
                .divide(new BigDecimal(executed_price) , ROUNDING_SCALE, RoundingMode.HALF_UP).multiply(getRpnlSum(data))
                .toString();
        data.setSumCoin(val);
        return val;
    }


    private static BigDecimal getRpnlSum(PNLRawData data) {

        String adapter = data.getAdapter_id();
        String symobl = data.getSymbol();

        for (Map.Entry<String, String> e : sizesMap.entrySet()) {
            if ((adapter+symobl).startsWith(e.getKey().replace("*",""))) {
                return new BigDecimal(e.getValue());
            }
        }
        return new BigDecimal("10");

    }




    private static void buildRpnlSumMap() {


        // okex
        sizesMap.put(OKEXFUTURE_ADAPTER+"BTC*","100");
        sizesMap.put(OKEXFUTURE_ADAPTER+"ETH*","10");
        sizesMap.put("ETC*","10");
        sizesMap.put("XRP*","10");
        sizesMap.put("EOS*","10");
        sizesMap.put("BCH*","10");
        sizesMap.put("BSV*","10");

        // bitmex
        sizesMap.put("XBT*","1");
        sizesMap.put("ETHUSD","1");
        sizesMap.put(BITMEX_ADAPTER+"ETH*","1");
        sizesMap.put("ADA*","1");
        sizesMap.put("BCH*","1");
        sizesMap.put("EOS*","1");
        sizesMap.put("LTC*","1");
        sizesMap.put("XRP*","1");

        // derabit
        sizesMap.put(DERABIT_ADAPTER+"BTC*","10");
        sizesMap.put(DERABIT_ADAPTER+"ETH*","1");

        // CRYPTOFACILITIES
        sizesMap.put("PI_*","1");
        sizesMap.put("FI_*","1");
        sizesMap.put("PV_XRPXBT*","1");
        sizesMap.put("FV_XRPXBT*","1");

    }

    public static String replaceNull(String input) {
        return input == null ? "" : input;
    }



////// ignore below


    /*


execution_id,order_id,deal_id,wave_id,exchange_trade_id,execution_time,exchange_execution_id,executed_price,executed_size,fees,fees_currency,adapter_id,strategy_id,account_id,exchange_order_id,symbol,side,order_price,order_size,ccy_1,ccy_2,ccy2_src,underlying_index,underlying_index_src,hedge_id
1,1,621,34040,,"2018-11-22 13:13:33",2,5.514,4.0,0.00217628,ETC,OKEXFUTURE_ADAPTER,MANUAL-621,0,1845258712730624,ETC1123,SELL,0.0,5.0,0.0,0.0,,5.52862189,OKEXFUTURE_ADAPTER.etc_usd,1130

[
  {
    "execution_id": "1",
    "order_id": "1",
    "deal_id": "621",
    "wave_id": "34040",
    "exchange_trade_id": "",
    "execution_time": "2018-11-22 13:13:33",
    "exchange_execution_id": "2",
    "executed_price": "5.514",
    "executed_size": "4.0",
    "fees": "0.00217628",
    "fees_currency": "ETC",
    "adapter_id": "OKEXFUTURE_ADAPTER",
    "strategy_id": "MANUAL-621",
    "account_id": "0",
    "exchange_order_id": "1845258712730624",
    "symbol": "ETC1123",
    "side": "SELL",
    "order_price": "0.0",
    "order_size": "5.0",
    "ccy_1": "0.0",
    "ccy_2": "0.0",
    "ccy2_src": "",
    "underlying_index": "5.52862189",
    "underlying_index_src": "OKEXFUTURE_ADAPTER.etc_usd",
    "hedge_id": "1130"
  }
]

     */

}
